from typing import Dict, Any
import xml.etree.ElementTree as ET

# FPML tag dictionary for Commodity Swaps with explanations for each tag taken from https://www.fpml.org/spec/fpml-5-3-6-rec-1/html/confirmation/fpml-5-3-intro-16.html
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
        # buySell specifies the direction of the swap
        "buySell": "str",
        # fixedLeg contains details about the fixed leg of the swap.
        "CommodityUnderlyer": {
            # commodityBase element identifies the base type of the commodity being traded, for example 'Oil'.": "float",
            "commodityBase": "str",
            # commodityDetails also identifies the type of commodity but it is more specific than the base, for example 'Brent'
            "commodityDetails": "str",
            # unit element identifies the unit in which the underlyer is denominated
            "unit": "str",
            # currency identifies the currency in which the Commodity Reference Price is published
            "currency": "currency",
            # 'Either the 'exchange' or the 'publication' is specified. For those commodities being traded with reference to the price of a listed future, the exchange where that future is listed should be specified in the 'exchange' element. On the other hand, for those commodities being traded with reference to a price distributed by a publication, that publication should be specified in the 'publication' element.  Not FPML standard
            "referencePrice": "str",

        },
        "fixedLeg": {
            # fixedPrice refers to the fixed price at which the commodity is traded in the swap
            "fixedPrice": "float",
            # priceUnit specifies the unit for the fixed price (e.g., USD per barrel)
            "priceUnit": "str",
            # fixedPriceSchedule allows the specification of a Fixed Price that varies over the life of the trade.
            "fixedPriceSchedule": "str",
        },
        # floatLeg contains details about the floating leg of the swap
        "floatLeg": {
            # referencePrice specifies the floating reference price (e.g., market index or spot price)
            "referencePrice": "float",
            # resetDates refers to the dates on which the floating rate will be reset (e.g., monthly or quarterly)
            "resetDates": "date"
        },
        # terminationDate is the end date of the swap when it matures
        "terminationDate": "date",
        "CommodityContent": {
            # Common pricing may be relevant for a Transaction that references more than one Commodity Reference Price. If Common Pricing is not specified as applicable, it will be deemed not to apply.
            "commonPricing": "str",
            # Market disruption events as defined in the ISDA 1993 Commodity Definitions or in ISDA 2005 Commodity Definitions, as applicable.
            "marketDisruption": "str",
            # Rounding direction and precision for amounts.
            "marketDisruption": "float",
            # The consequences of Bullion Settlement Disruption Events.
            "settlementDisruption": "float",

    }
}
