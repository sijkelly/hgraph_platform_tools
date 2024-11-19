from hgraph_trade_loader import load_trade_from_file
from hgraph_trade_mapper import map_trade_to_model
from hgraph_trade_booker import book_trade
from config import KAFKA_TOPIC

if __name__ == "__main__":
    # Load trade from local file
    trade_data = load_trade_from_file("sample_trade.json")

    # Map to the correct model
    trade_model = map_trade_to_model(trade_data)

    # Book the trade
    book_trade(trade_model, KAFKA_TOPIC)