"""
Real-time order engine.

Runs the order book graph on a background thread so that synchronous callers
(the REST API, scripts, an AI agent's tools) can submit a message and get the
response back.  Everything the engine processes is written to the event log
before the caller is answered, and on start the engine rebuilds its orders
from that log.

Usage::

    engine = OrderEngine("order_events.db")
    engine.start()
    engine.set_credit(CreditPosition(counterparty="ACME", limit_amount=1_000_000.0))
    response = engine.submit(NewOrder(order_id="ORD-1", user_id="trader1", details=details))
    engine.stop()
"""

import logging
import threading
import uuid
from concurrent.futures import Future
from dataclasses import replace
from typing import Callable

from hgraph import EvaluationMode, run_graph

from hgraph_oms.engine_graph import Envelope, PermissionUpdate, Stop, build_engine_graph
from hgraph_oms.event_log import append_events, init_event_log, read_events, replay
from hgraph_oms.trades import TradeTicket, tickets_from_events
from hgraph_oms.model import CreditPosition, CreditRemoved, OmsInput, OrderMessage, OrderResponse, OrderSnapshot

__all__ = ("OrderEngine",)

logger = logging.getLogger(__name__)


class OrderEngine:
    """Runs the order book graph in real time and answers submitted messages.

    :param db_path: SQLite file holding the order event log.
    :param enforce_entitlements: When ``True``, users must hold ``execute_trade``
        (see :meth:`set_permissions`) for their messages to be applied.
    :param trade_sink: Called on the graph thread with the trade tickets of each batch's fills,
        after the batch is logged and before callers are answered.  On start it is also given
        the ticket of every fill in the event log, so it must skip trades it has already handled.
    """

    def __init__(
        self,
        db_path: str,
        *,
        enforce_entitlements: bool = False,
        trade_sink: Callable[[tuple[TradeTicket, ...]], object] | None = None,
    ):
        self.db_path = db_path
        self.enforce_entitlements = enforce_entitlements
        self._trade_sink = trade_sink
        self._orders: dict[str, OrderSnapshot] = {}
        self._pending: dict[str, Future] = {}
        self._listeners: list[Callable[[tuple[OrderSnapshot, ...]], object]] = []
        self._lock = threading.Lock()
        self._sender: Callable[[Envelope], bool] | None = None
        self._ready = threading.Event()
        self._thread: threading.Thread | None = None
        self._error: BaseException | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self, *, timeout: float = 10.0) -> None:
        """Rebuild orders from the event log and start the graph thread.

        :raises RuntimeError: If the engine is already running or fails to start.
        """
        if self._thread is not None:
            raise RuntimeError("Order engine already started")

        init_event_log(self.db_path)
        events = read_events(self.db_path)
        self._orders = replay(events)
        logger.info("Order engine recovered %d order(s) from %s", len(self._orders), self.db_path)
        # Re-offer every logged fill so a trade logged but not handed on before a crash is not lost
        self._send_trades(tickets_from_events(events))

        self._thread = threading.Thread(target=self._run, name="order-engine", daemon=True)
        self._thread.start()
        if not self._ready.wait(timeout) or self._error is not None:
            raise RuntimeError("Order engine failed to start") from self._error

    def stop(self, *, timeout: float = 10.0) -> None:
        """Stop the graph and wait for its thread to finish. Safe to call more than once."""
        thread, self._thread = self._thread, None
        if thread is None:
            return
        if thread.is_alive() and self._sender is not None:
            self._sender(Envelope(Stop()))
        thread.join(timeout)
        self._ready.clear()
        self._sender = None

    @property
    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    # ------------------------------------------------------------------
    # Inputs
    # ------------------------------------------------------------------

    def submit(self, message: OrderMessage, *, timeout: float = 10.0) -> OrderResponse:
        """Send a message to the order book and wait for its response.

        The response is returned only after the message and its outcome are in the event log.

        :raises RuntimeError: If the engine is not running.
        :raises TimeoutError: If no response arrives within ``timeout`` seconds.
        """
        if not message.request_id:
            message = replace(message, request_id=uuid.uuid4().hex)

        future: Future = Future()
        with self._lock:
            if message.request_id in self._pending:
                raise ValueError(f"Request {message.request_id} is already in flight")
            self._pending[message.request_id] = future
        try:
            self._push(message)
            return future.result(timeout)
        finally:
            with self._lock:
                self._pending.pop(message.request_id, None)

    def set_credit(self, position: CreditPosition) -> None:
        """Publish a counterparty's credit position to the graph."""
        self._push(position)

    def clear_credit(self, counterparty: str) -> None:
        """Tell the graph the counterparty no longer has a credit limit."""
        self._push(CreditRemoved(counterparty=counterparty))

    def set_permissions(self, user_id: str, actions: frozenset[str]) -> None:
        """Publish the entitlement actions a user holds."""
        self._push(PermissionUpdate(user_id=user_id, actions=frozenset(actions)))

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def get_order(self, order_id: str) -> OrderSnapshot | None:
        with self._lock:
            return self._orders.get(order_id)

    def orders(self) -> list[OrderSnapshot]:
        """Return the latest snapshot of every order."""
        with self._lock:
            return list(self._orders.values())

    def add_listener(self, listener: Callable[[tuple[OrderSnapshot, ...]], object]) -> Callable[[], None]:
        """Call ``listener`` with the orders that changed, after each batch is logged and before callers are answered.

        The listener runs on the graph thread, so it must return quickly and must not call
        :meth:`submit`.  An exception it raises is logged and does not affect the engine.

        :returns: A function that removes the listener.
        """
        with self._lock:
            self._listeners.append(listener)

        def remove() -> None:
            with self._lock:
                if listener in self._listeners:
                    self._listeners.remove(listener)

        return remove

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _push(self, payload: OmsInput) -> None:
        if not self.is_running or self._sender is None:
            raise RuntimeError("Order engine is not running") from self._error
        if not self._sender(Envelope(payload)):
            raise RuntimeError("Order engine has stopped")

    def _run(self) -> None:
        try:
            run_graph(self._build_graph(), run_mode=EvaluationMode.REAL_TIME)
        except BaseException as exc:  # the thread must never die silently
            logger.exception("Order engine graph failed")
            self._error = exc
        finally:
            self._ready.set()
            with self._lock:
                pending, self._pending = self._pending, {}
            for future in pending.values():
                if not future.done():
                    future.set_exception(RuntimeError("Order engine stopped before responding"))

    def _send_trades(self, tickets: tuple[TradeTicket, ...]) -> None:
        if not tickets or self._trade_sink is None:
            return
        try:
            self._trade_sink(tickets)
        except Exception:
            # The fills are in the event log and will be re-offered on the next start
            logger.exception("Trade sink failed for %d ticket(s)", len(tickets))

    def _on_output(
        self,
        changed: dict[str, OrderSnapshot],
        responses: tuple[OrderResponse, ...],
        tickets: tuple[TradeTicket, ...] = (),
    ) -> None:
        """Runs on the graph thread for every batch: log, hand on trades, publish, notify, answer callers."""
        append_events(self.db_path, responses)
        self._send_trades(tickets)
        with self._lock:
            self._orders.update(changed)
            futures = [(self._pending.get(r.message.request_id), r) for r in responses]
            listeners = list(self._listeners) if changed else []
        for listener in listeners:
            try:
                listener(tuple(changed.values()))
            except Exception:
                logger.exception("Order listener failed")
        for future, response in futures:
            if future is not None and not future.done():
                future.set_result(response)

    def _on_ready(self, sender: Callable[[Envelope], bool]) -> None:
        self._sender = sender
        self._ready.set()

    def _build_graph(self):
        return build_engine_graph(
            on_ready=self._on_ready,
            on_output=self._on_output,
            enforce_entitlements=self.enforce_entitlements,
            initial_orders=tuple(self._orders.values()),
        )
