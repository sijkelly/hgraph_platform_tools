"""
hgraph wiring for the real-time order engine.

One push queue carries every external input (order messages, credit positions,
permission updates, the stop request) so that inputs sent together arrive in
the same engine cycle and in order.  Small nodes split the stream back out by
type and feed the order book.
"""

from dataclasses import dataclass
from typing import Callable

from hgraph import REMOVE_IF_EXISTS, TS, TSB, TSD, compute_node, graph, push_queue, sink_node, stop_engine

from hgraph_oms.model import CreditPosition, CreditRemoved, OmsInput, OrderMessage, OrderResponse, OrderSnapshot
from hgraph_oms.order_book import OrderBookOutput, order_book
from hgraph_oms.trades import TradeTicket, tickets_from_responses

__all__ = ("PermissionUpdate", "Stop", "Envelope", "build_engine_graph")


@dataclass(frozen=True, kw_only=True)
class PermissionUpdate(OmsInput):
    user_id: str
    actions: frozenset[str]


@dataclass(frozen=True)
class Stop(OmsInput):
    pass


@dataclass(frozen=True)
class Envelope:
    """Single concrete type carried by the push queue; the payload is any :class:`OmsInput`."""

    payload: object


@compute_node
def _messages(inputs: TS[tuple[Envelope, ...]]) -> TS[tuple[OrderMessage, ...]]:
    found = tuple(e.payload for e in inputs.value if isinstance(e.payload, OrderMessage))
    return found if found else None


@compute_node
def _credit(inputs: TS[tuple[Envelope, ...]]) -> TSD[str, TS[CreditPosition]]:
    found: dict = {}
    for payload in (e.payload for e in inputs.value):
        if isinstance(payload, CreditPosition):
            found[payload.counterparty] = payload
        elif isinstance(payload, CreditRemoved):
            found[payload.counterparty] = REMOVE_IF_EXISTS
    return found if found else None


@compute_node
def _permissions(inputs: TS[tuple[Envelope, ...]]) -> TSD[str, TS[frozenset[str]]]:
    found = {e.payload.user_id: e.payload.actions for e in inputs.value if isinstance(e.payload, PermissionUpdate)}
    return found if found else None


@compute_node
def _stop_requested(inputs: TS[tuple[Envelope, ...]]) -> TS[bool]:
    return True if any(isinstance(e.payload, Stop) for e in inputs.value) else None


@compute_node
def _fill_tickets(responses: TS[tuple[OrderResponse, ...]]) -> TS[tuple[TradeTicket, ...]]:
    """One trade ticket per accepted fill; ticks only in cycles that had a fill."""
    tickets = tickets_from_responses(responses.value)
    return tickets if tickets else None


def build_engine_graph(
    *,
    on_ready: Callable[[Callable[[Envelope], bool]], None],
    on_output: Callable[[dict[str, OrderSnapshot], tuple[OrderResponse, ...], tuple[TradeTicket, ...]], None],
    enforce_entitlements: bool = False,
    initial_orders: tuple[OrderSnapshot, ...] = (),
):
    """Build the engine graph.

    :param on_ready: Called once at graph start with the thread-safe sender for the input queue.
    :param on_output: Called on the graph thread for every processed batch with the orders that
        changed, the responses, and the trade tickets of any fills in the batch.
    :param enforce_entitlements: Passed to :func:`~hgraph_oms.order_book.order_book`.
    :param initial_orders: Orders the book starts with.
    :returns: A graph to pass to ``run_graph`` in real-time mode.
    """

    @push_queue(TS[tuple[Envelope, ...]], burst=True)
    def inputs(sender):
        on_ready(sender)

    # ``tickets`` has no value until the first fill, so it must not gate the node
    @sink_node(active=("out",), valid=("out",))
    def publish(out: TSB[OrderBookOutput], tickets: TS[tuple[TradeTicket, ...]]):
        if out.responses.modified:
            changed = {k: v.value for k, v in out.orders.modified_items()} if out.orders.modified else {}
            on_output(changed, out.responses.value, tickets.value if tickets.modified else ())

    @graph
    def order_engine_graph():
        received = inputs()
        book = order_book(
            _messages(received),
            _credit(received),
            _permissions(received),
            enforce_entitlements=enforce_entitlements,
            initial_orders=initial_orders,
        )
        publish(book, _fill_tickets(book.responses))
        stop_engine(_stop_requested(received))

    return order_engine_graph
