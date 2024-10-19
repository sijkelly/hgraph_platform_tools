# Example Commodity Swap structure
def create_commodity_swap(trade_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create a commodity swap based on trade data.
    """
    # Assuming trade_data follows the structure of FPML_TAGS
    swap = {
        "tradeHeader": {
            "partyTradeIdentifier": {
                "partyReference": trade_data["party1"]["id"],
                "tradeId": trade_data["trade_id"]
            },
            "tradeDate": trade_data["trade_date"]
        },
        "parties": [
            {"id": trade_data["party1"]["id"], "name": trade_data["party1"]["name"]},
            {"id": trade_data["party2"]["id"], "name": trade_data["party2"]["name"]}
        ],
        "commoditySwap": {
            "effectiveDate": trade_data["effective_date"],
            "notionalQuantity": {
                "quantity": trade_data["notional_quantity"],
                "unit": trade_data["notional_unit"]
            },
            "paymentCurrency": trade_data["currency"],
            "fixedLeg": {
                "fixedPrice": trade_data["fixed_leg_price"],
                "priceUnit": trade_data["price_unit"]
            },
            "floatLeg": {
                "referencePrice": trade_data["float_leg_price"],
                "resetDates": trade_data["reset_dates"]
            },
            "terminationDate": trade_data["termination_date"]
        }
    }
    return swap


def generate_fpml(swap_data: Dict[str, Any]) -> str:
    """
    Generate FPML from swap data using the FPML_TAGS dictionary.
    """
    root = ET.Element("trade", {"xmlns:fpml": "http://www.fpml.org/FpML-5/confirmation", "fpmlVersion": "5-3"})

    # Building the tradeHeader
    trade_header = ET.SubElement(root, "tradeHeader")
    trade_id = ET.SubElement(trade_header, "tradeId")
    trade_id.text = swap_data["tradeHeader"]["partyTradeIdentifier"]["tradeId"]

    # Adding party information
    for party in swap_data["parties"]:
        party_element = ET.SubElement(root, "party", {"id": party["id"]})
        party_name = ET.SubElement(party_element, "partyName")
        party_name.text = party["name"]

    # Adding commodity swap details
    commodity_swap = ET.SubElement(root, "commoditySwap")
    effective_date = ET.SubElement(commodity_swap, "effectiveDate")
    effective_date.text = swap_data["commoditySwap"]["effectiveDate"]

    # Convert the tree to an FPML string
    return ET.tostring(root, encoding='utf-8').decode('utf-8')


# Sample data for creating a commodity swap
sample_trade_data = {
    "trade_id": "SWAP-001",
    "trade_date": "2024-10-20",
    "party1": {"id": "Party1", "name": "Commodity Trader A"},
    "party2": {"id": "Party2", "name": "Commodity Trader B"},
    "effective_date": "2024-11-01",
    "notional_quantity": 10000,
    "notional_unit": "barrels",
    "currency": "USD",
    "fixed_leg_price": 85.50,
    "float_leg_price": 88.00,
    "price_unit": "USD per barrel",
    "reset_dates": ["2024-12-01", "2025-01-01"],
    "termination_date": "2025-11-01"
}

# Generate a commodity swap
commodity_swap = create_commodity_swap(sample_trade_data)
fpml_output = generate_fpml(commodity_swap)

# Print generated FPML
print(fpml_output)
