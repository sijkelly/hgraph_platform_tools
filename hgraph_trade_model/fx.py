import json
from typing import Dict, Any
from hgraph_trade_model.fpml_mappings import get_global_mapping, get_instrument_mapping, map_hgraph_to_fpml


def create_fx_trade(trade_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create the FX trade section.

    :param trade_data: Dictionary containing hgraph trade data.
    :return: A dictionary representing the FX trade.
    """
    # Retrieve the global and FX-specific mappings
    global_mapping = get_global_mapping()
    fx_mapping = get_instrument_mapping("fx")
    combined_mapping = {**global_mapping, **fx_mapping}

    # Map hgraph keys to fpml keys
    fpml_data = map_hgraph_to_fpml(trade_data, combined_mapping)

    # Construct the FX trade section
    fx_trade = {
        "buySell": fpml_data.get("buySell", ""),
        "tradeDate": fpml_data.get("tradeDate", ""),
        "settlementDate": fpml_data.get("settlementDate", ""),
        "currencyPair": fpml_data.get("currencyPair", ""),
        "notionalAmount": {
            "amount": fpml_data.get("notionalAmount", 0),
            "currency": fpml_data.get("currency", "")
        },
        "rate": fpml_data.get("rate", 0)
    }

    return {
        "fxTrade": fx_trade,
        "metadata": {
            "type": "fxTrade",
            "version": "1.0"
        }
    }


# Example usage for testing
if __name__ == "__main__":
    # Sample FX trade data for testing
    sample_trade_data = {
        "buySell": "Buy",
        "tradeDate": "2024-11-20",
        "settlementDate": "2024-11-22",
        "currencyPair": "EUR/USD",
        "notionalAmount": 1000000,
        "currency": "EUR",
        "rate": 1.10
    }

    # Generate FX trade data structure
    fx_trade = create_fx_trade(sample_trade_data)

    # Print the generated FX trade
    print(json.dumps(fx_trade, indent=4))