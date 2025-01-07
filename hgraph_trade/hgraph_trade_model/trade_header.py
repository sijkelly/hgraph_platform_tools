"""
trade_header.py

This module provides functionality to create the trade header section of a trade message.
It uses global and header-specific mappings to translate hgraph trade data into FpML-like
keys. The resulting tradeHeader structure includes essential identifiers, date information,
and references to involved parties, portfolios, and traders.
"""

from typing import Dict, Any
from hgraph_trade.hgraph_trade_mapping.fpml_mappings import get_global_mapping, get_instrument_mapping

def map_hgraph_to_fpml(trade_data: Dict[str, Any], mapping: Dict[str, str]) -> Dict[str, Any]:
    """
    Map hgraph trade data keys to their corresponding FpML keys.

    :param trade_data: Dictionary containing hgraph trade data.
    :param mapping: A mapping dictionary for hgraph to FpML keys.
    :return: Dictionary with keys converted to FpML format.
    """
    mapped_data = {}
    for key, value in trade_data.items():
        fpml_key = mapping.get(key, key)
        mapped_data[fpml_key] = value
    return mapped_data

def create_trade_header(trade_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create the trade header section of the trade data.

    This function applies global and header-specific mappings to translate hgraph fields into
    FpML-compatible ones. It then constructs a tradeHeader dictionary that includes identifiers,
    trade date information, parties, portfolios, and traders associated with the trade.

    :param trade_data: Dictionary containing hgraph trade data.
    :return: A dictionary representing the trade header section, including metadata.
    """
    global_mapping = get_global_mapping()
    header_mapping = get_instrument_mapping("trade_header")
    combined_mapping = {**global_mapping, **header_mapping}

    # Map hgraph keys to FpML keys
    fpml_data = map_hgraph_to_fpml(trade_data, combined_mapping)

    # Extract nested fields for counterparty, portfolio, and traders
    counterparty = trade_data.get("counterparty", {})
    internal_party = counterparty.get("internal", "")
    external_party = counterparty.get("external", "")

    portfolio = trade_data.get("portfolio", {})
    internal_portfolio = portfolio.get("internal", "")
    external_portfolio = portfolio.get("external", "")

    traders = trade_data.get("traders", {})
    internal_trader = traders.get("internal", "")
    external_trader = traders.get("external", "")

    return {
        "tradeHeader": {
            "partyTradeIdentifier": {
                "tradeId": fpml_data.get("tradeId", "")
            },
            "tradeDate": fpml_data.get("tradeDate", ""),
            "parties": [
                {"internalParty": internal_party},
                {"externalParty": external_party}
            ],
            "portfolio": {
                "internalPortfolio": internal_portfolio,
                "externalPortfolio": external_portfolio
            },
            "trader": {
                "internalTrader": internal_trader,
                "externalTrader": external_trader
            },
        },
        "metadata": {
            "type": "tradeHeader",
            "version": "1.0"
        }
    }

# Example usage (commented out for production; can be tested separately)
# if __name__ == "__main__":
#     sample_trade_data = {
#         "trade_date": "2024-11-10",
#         "party_reference": "Party1",
#         "trade_id": "SWAP-003",
#         "counterparty": {
#             "internal": "Internal Counterparty",
#             "external": "External Counterparty"
#         },
#         "portfolio": {
#             "internal": "Portfolio A",
#             "external": "Portfolio B"
#         },
#         "traders": {
#             "internal": "Trader X",
#             "external": "Trader Y"
#         },
#     }

#     trade_header = create_trade_header(sample_trade_data)
#     print(json.dumps(trade_header, indent=4))