import json
from typing import Dict, Any

# Define the mapping between hgraph tags and fpml tags
HGRAPH_TO_FPML_MAPPING = {
    "trade_date": "tradeDate",
    "party_reference": "partyReference",
    "trade_id": "tradeId",
    "internal_party": "internalParty",
    "external_party": "externalParty",
    "internal_portfolio": "internalPortfolio",
    "external_portfolio": "externalPortfolio",
    "internal_trader": "internalTrader",
    "external_trader": "externalTrader",
}


def map_hgraph_to_fpml(trade_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map hgraph trade data keys to their corresponding fpml keys.

    :param trade_data: Dictionary containing hgraph trade data.
    :return: Dictionary with keys converted to fpml format.
    """
    mapped_data = {}
    for key, value in trade_data.items():
        fpml_key = HGRAPH_TO_FPML_MAPPING.get(key, key)  # Default to the original key if no mapping exists
        mapped_data[fpml_key] = value
    return mapped_data


def create_trade_header(trade_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create the trade header section.

    :param trade_data: Dictionary containing hgraph trade data.
    :return: A dictionary representing the trade header.
    """
    # Map hgraph keys to fpml keys
    fpml_data = map_hgraph_to_fpml(trade_data)

    return {
        "tradeHeader": {
            "partyTradeIdentifier": {
                "partyReference": fpml_data.get("partyReference", ""),
                "tradeId": fpml_data.get("tradeId", "")
            },
            "tradeDate": fpml_data.get("tradeDate", ""),
            "parties": [
                {"internalParty": fpml_data.get("internalParty", "")},
                {"externalParty": fpml_data.get("externalParty", "")}
            ],
            "portfolio": {
                "internalPortfolio": fpml_data.get("internalPortfolio", ""),
                "externalPortfolio": fpml_data.get("externalPortfolio", "")
            },
            "trader": {
                "internalTrader": fpml_data.get("internalTrader", ""),
                "externalTrader": fpml_data.get("externalTrader", "")
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
        "internal_party": "Internal Counterparty",
        "external_party": "External Counterparty",
        "internal_portfolio": "Portfolio A",
        "external_portfolio": "Portfolio B",
        "internal_trader": "Trader X",
        "external_trader": "Trader Y",
    }

    # Generate the trade header
    trade_header = create_trade_header(sample_trade_data)

    # Print the generated trade header
    print(json.dumps(trade_header, indent=4))