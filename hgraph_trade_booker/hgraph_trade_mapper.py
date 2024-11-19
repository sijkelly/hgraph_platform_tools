from typing import Dict, Any
from hgraph_trade_model import create_commodity_swap


def map_trade_to_model(trade_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map raw trade data to the correct trade model based on trade type.
    """
    trade_type = trade_data.get("tradeType")
    swap_type = trade_data.get("swapType")

    if trade_type == "CommoditySwap":
        return create_commodity_swap(trade_data)
    else:
        raise ValueError(f"Unsupported trade type: {trade_type}")