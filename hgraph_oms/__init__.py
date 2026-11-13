"""
hgraph_oms — the order management core.

* :mod:`hgraph_oms.model` — immutable messages, snapshots and responses
* :mod:`hgraph_oms.state_machine` — order lifecycle transitions (pure)
* :mod:`hgraph_oms.pre_trade` — credit and entitlement checks (pure)
* :mod:`hgraph_oms.order_book` — the hgraph node combining the two
* :mod:`hgraph_oms.event_log` — append-only SQLite log and replay
* :mod:`hgraph_oms.trades` — fills as trade tickets and booking-pipeline input (pure)
* :mod:`hgraph_oms.booking` — books tickets through the trade booking pipeline
* :mod:`hgraph_oms.delivery` — delivers booked trades downstream, at least once
* :mod:`hgraph_oms.proposals` — orders drafted by an agent for a person to confirm
* :mod:`hgraph_oms.engine_graph` — hgraph wiring for the real-time engine
* :mod:`hgraph_oms.engine` — runs the graph on a thread for synchronous callers
"""

__all__: tuple[str, ...] = ()
