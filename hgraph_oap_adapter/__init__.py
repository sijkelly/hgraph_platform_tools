"""
Bridge between hg_oap domain models and hgraph_platform_tools trade booking.

The adapter package converts rich hg_oap domain objects (swaps, parties,
assets) into the flat dictionary format consumed by the
``hgraph_trade`` booking pipeline.

Usage::

    from hgraph_oap_adapter.energy_swap_adapter import (
        fixed_float_swap_to_trade_data,
        basis_swap_to_trade_data,
    )
    from hgraph_oap_adapter.party_adapter import (
        legal_entity_to_counterparty_row,
        relationship_to_trade_parties,
    )
"""

__all__: tuple[str, ...] = ()
