from typing import Dict, Any
import xml.etree.ElementTree as ET

# FPML tag dictionary for Commodity Swaps with explanations for each tag
FPML_TAGS = {
    # The tradeHeader contains information about the trade itself, such as the trade date and identifiers
    "tradeHeader": {
        # partyTradeIdentifier holds the party references and the unique trade ID
        "partyTradeIdentifier": {
            # partyReference is an internal reference ID for each party in the trade
            "partyReference": "href",
            # tradeId is a unique identifier for the trade, often provided by a trade repository
            "tradeId": "tradeIdScheme"
        },
        # tradeDate specifies the date the trade was executed
        "tradeDate": "date"
    },
    # parties contains the details of all parties involved in the trade
    "parties": {
        "party": {
            # id is the unique identifier for each party (e.g., Party1, Party2)
            "id": "partyId",
            # name is the legal or commercial name of the party
            "name": "partyName"
        }
    },
    # commoditySwap defines the structure of the commodity swap
    "commoditySwap": {
        # effectiveDate is the start date of the swap when it becomes legally binding
        "effectiveDate": "date",
        # notionalQuantity specifies the quantity of the commodity being traded (e.g., barrels of oil)
        "notionalQuantity": {
            # quantity refers to the amount of the commodity
            "quantity": "int",
            # unit refers to the unit of measurement for the commodity (e.g., barrels, tons)
            "unit": "str"
        },
        # paymentCurrency specifies the currency in which the swap is denominated (e.g., USD, EUR)
        "paymentCurrency": "currency",
        # fixedLeg contains details about the fixed leg of the swap
        "fixedLeg": {
            # fixedPrice refers to the fixed price at which the commodity is traded in the swap
            "fixedPrice": "float",
            # priceUnit specifies the unit for the fixed price (e.g., USD per barrel)
            "priceUnit": "str"
        },
        # floatLeg contains details about the floating leg of the swap
        "floatLeg": {
            # referencePrice specifies the floating reference price (e.g., market index or spot price)
            "referencePrice": "float",
            # resetDates refers to the dates on which the floating rate will be reset (e.g., monthly or quarterly)
            "resetDates": "date"
        },
        # terminationDate is the end date of the swap when it matures
        "terminationDate": "date"
    }
}
