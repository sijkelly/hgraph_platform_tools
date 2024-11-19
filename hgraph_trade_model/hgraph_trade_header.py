import json
from typing import Dict, Any


def create_trade_header(trade_data: dict[str, Any]) -> Dict[str, Any]:
    """
    Create the trade header section.
    """
    return {
        "tradeHeader": {
            "partyTradeIdentifier": {
                "partyReference": trade_data["partyReference"],
                "tradeId": trade_data["tradeId"]
            },
            "tradeDate": trade_data["tradeDate"],
            "parties": {
                {"internalParty": trade_data["internalParty"]},
                {"externalParty": trade_data["externalParty"]}
            },
            "portfolio": {
                "internalPortfolio": trade_data["internalPortfolio"],
                "externalPortfolio": trade_data["externalPortfolio"]
            ]
            "trader: {
                "internalTrader": trade_data["internalTrader"],
                "externalTrader": trade_data["externalTrader"]
            },
        }
    }
