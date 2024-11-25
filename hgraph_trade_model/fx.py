import json
from typing import Dict, Any


def create_fx_trade(trade_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create the FX trade section.

    :param trade_data: Dictionary containing trade data.
        Expected keys:
        - buySell (str): Buy or sell indicator.
        - tradeDate (str): Trade execution date.
        - settlementDate (str): FX trade settlement date.
        - currencyPair (str): Currency pair being traded (e.g., "EUR/USD").
        - notionalAmount (float): Notional amount being traded.
        - currency (str): Currency of the notional amount.
        - rate (float): FX rate for the trade.
    :return: A dictionary representing the FX trade.
    """
    return {
        "fxTrade": {
            "buySell": trade_data.get("buySell", ""),
            "tradeDate": trade_data.get("tradeDate", ""),
            "settlementDate": trade_data.get("settlementDate", ""),
            "currencyPair": trade_data.get("currencyPair", ""),
            "notionalAmount": {
                "amount": trade_data.get("notionalAmount", 0),
                "currency": trade_data.get("currency", "")
            },
            "rate": trade_data.get("rate", 0)
        },
        "metadata": {
            "type": "fxTrade",
            "version": "1.0"
        }
    }


# Example usage for testing
if __name__ == "__main__":
    # Sample FX trade data
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