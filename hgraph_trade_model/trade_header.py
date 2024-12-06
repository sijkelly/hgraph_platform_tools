import json
from typing import Dict, Any
from hgraph_trade_model.fpml_mappings import get_global_mapping, get_instrument_mapping


def map_hgraph_to_fpml(trade_data: Dict[str, Any], mapping: Dict[str, str]) -> Dict[str, Any]:
    """
    Map hgraph trade data keys to their corresponding fpml keys.

    :param trade_data: Dictionary containing hgraph trade data.
    :param mapping: Mapping dictionary for hgraph to fpml keys.
    :return: Dictionary with keys converted to fpml format.
    """
    mapped_data = {}
    for key, value in trade_data.items():
        # Translate keys using the mapping
        fpml_key = mapping.get(key, key)
        mapped_data[fpml_key] = value
    return mapped_data


def create_trade_header(trade_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create the trade header section.

    :param trade_data: Dictionary containing hgraph trade data.
    :return: A dictionary representing the trade header.
    """
    # Retrieve global and header-specific mappings
    global_mapping = get_global_mapping()
    header_mapping = get_instrument_mapping("trade_header")
    combined_mapping = {**global_mapping, **header_mapping}

    # Map hgraph keys to fpml keys
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
                {"internalParty": internal_party},  # Use parsed data
                {"externalParty": external_party}   # Use parsed data
            ],
            "portfolio": {
                "internalPortfolio": internal_portfolio,  # Use parsed data
                "externalPortfolio": external_portfolio   # Use parsed data
            },
            "trader": {
                "internalTrader": internal_trader,  # Use parsed data
                "externalTrader": external_trader   # Use parsed data
            },
        },
        "metadata": {
            "type": "tradeHeader",
            "version": "1.0"
        }
    }


# Example usage for testing
if __name__ == "__main__":
    # Sample trade data for testing
    sample_trade_data = {
        "trade_date": "2024-11-10",
        "party_reference": "Party1",
        "trade_id": "SWAP-003",
        "counterparty": {
            "internal": "Internal Counterparty",
            "external": "External Counterparty"
        },
        "portfolio": {
            "internal": "Portfolio A",
            "external": "Portfolio B"
        },
        "traders": {
            "internal": "Trader X",
            "external": "Trader Y"
        },
    }

    # Generate the trade header
    trade_header = create_trade_header(sample_trade_data)

    # Print the generated trade header
    print(json.dumps(trade_header, indent=4))