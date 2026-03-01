"""
Tests for hgraph_trade.hgraph_trade_model — all instrument model creators.
"""

import pytest
from hgraph_trade.hgraph_trade_model.swap import create_commodity_swap
from hgraph_trade.hgraph_trade_model.option import create_commodity_option
from hgraph_trade.hgraph_trade_model.forward import create_commodity_forward
from hgraph_trade.hgraph_trade_model.future import create_commodity_future
from hgraph_trade.hgraph_trade_model.physical import create_commodity_physical
from hgraph_trade.hgraph_trade_model.swaption import create_commodity_swaption
from hgraph_trade.hgraph_trade_model.fx import create_fx_trade
from hgraph_trade.hgraph_trade_model.cash import create_cash_trade
from hgraph_trade.hgraph_trade_model.trade_header import create_trade_header
from hgraph_trade.hgraph_trade_model.trade_footer import create_trade_footer


# ==================== Swap Tests ====================

class TestCreateCommoditySwap:
    """Tests for create_commodity_swap."""

    def test_fixed_float_returns_list(self, swap_fixed_float_data):
        result = create_commodity_swap(swap_fixed_float_data, "fixedFloat")
        assert isinstance(result, list)
        assert len(result) == 1

    def test_fixed_float_has_commodity_swap_key(self, swap_fixed_float_data):
        result = create_commodity_swap(swap_fixed_float_data, "fixedFloat")
        assert "commoditySwap" in result[0]

    def test_fixed_float_has_fixed_leg(self, swap_fixed_float_data):
        swap = result = create_commodity_swap(swap_fixed_float_data, "fixedFloat")
        commodity_swap = result[0]["commoditySwap"]
        assert "fixedLeg" in commodity_swap

    def test_fixed_float_has_floating_leg(self, swap_fixed_float_data):
        result = create_commodity_swap(swap_fixed_float_data, "fixedFloat")
        commodity_swap = result[0]["commoditySwap"]
        assert "floatingLeg" in commodity_swap

    def test_fixed_float_settlement_currency(self, swap_fixed_float_data):
        result = create_commodity_swap(swap_fixed_float_data, "fixedFloat")
        commodity_swap = result[0]["commoditySwap"]
        assert commodity_swap["settlementCurrency"] == "USD"

    def test_float_float_returns_list(self, swap_float_float_data):
        result = create_commodity_swap(swap_float_float_data, "floatFloat")
        assert isinstance(result, list)
        assert len(result) == 1

    def test_float_float_has_two_float_legs(self, swap_float_float_data):
        result = create_commodity_swap(swap_float_float_data, "floatFloat")
        commodity_swap = result[0]["commoditySwap"]
        assert "floatLeg1" in commodity_swap
        assert "floatLeg2" in commodity_swap

    def test_unsupported_sub_instrument_raises(self, swap_fixed_float_data):
        with pytest.raises(ValueError, match="Unsupported sub-instrument"):
            create_commodity_swap(swap_fixed_float_data, "invalidType")


# ==================== Option Tests ====================

class TestCreateCommodityOption:
    """Tests for create_commodity_option."""

    def test_returns_list(self, option_data):
        result = create_commodity_option(option_data)
        assert isinstance(result, list)
        assert len(result) == 1

    def test_has_commodity_option_key(self, option_data):
        result = create_commodity_option(option_data)
        assert "commodityOption" in result[0]

    def test_has_strike_price(self, option_data):
        result = create_commodity_option(option_data)
        option = result[0]["commodityOption"]
        assert option["strikePrice"] == 4.00

    def test_premium_calculation(self, option_data):
        result = create_commodity_option(option_data)
        option = result[0]["commodityOption"]
        # premiumPerUnit (0.25) * quantity (10000) = 2500
        assert option["premium"]["totalPremium"] == 2500

    def test_european_exercise_style(self, option_data):
        result = create_commodity_option(option_data)
        option = result[0]["commodityOption"]
        assert option["exerciseStyle"] == "European"

    def test_bermudan_has_exercise_dates(self, option_data):
        option_data["option_type"] = "Bermudan"
        option_data["exercise_dates"] = ["2025-06-01", "2025-09-01"]
        result = create_commodity_option(option_data)
        option = result[0]["commodityOption"]
        assert option["exerciseStyle"] == "Bermudan"
        assert option["exerciseDates"] == ["2025-06-01", "2025-09-01"]

    def test_invalid_exercise_style_raises(self, option_data):
        option_data["option_type"] = "InvalidStyle"
        with pytest.raises(ValueError, match="Invalid exercise style"):
            create_commodity_option(option_data)


# ==================== Forward Tests ====================

class TestCreateCommodityForward:
    """Tests for create_commodity_forward."""

    def test_returns_list(self, forward_data):
        result = create_commodity_forward(forward_data)
        assert isinstance(result, list)
        assert len(result) == 1

    def test_has_commodity_forward_key(self, forward_data):
        result = create_commodity_forward(forward_data)
        assert "commodityForward" in result[0]

    def test_has_fixed_price(self, forward_data):
        result = create_commodity_forward(forward_data)
        forward = result[0]["commodityForward"]
        assert forward["fixedPrice"] == 75.50

    def test_has_delivery_location(self, forward_data):
        result = create_commodity_forward(forward_data)
        forward = result[0]["commodityForward"]
        assert forward["deliveryLocation"] == "Houston"


# ==================== Future Tests ====================

class TestCreateCommodityFuture:
    """Tests for create_commodity_future."""

    def test_returns_list(self, future_data):
        result = create_commodity_future(future_data)
        assert isinstance(result, list)
        assert len(result) == 1

    def test_has_commodity_future_key(self, future_data):
        result = create_commodity_future(future_data)
        assert "commodityFuture" in result[0]

    def test_has_contract_price(self, future_data):
        result = create_commodity_future(future_data)
        future = result[0]["commodityFuture"]
        assert future["contractPrice"] == 76.25

    def test_has_exchange(self, future_data):
        result = create_commodity_future(future_data)
        future = result[0]["commodityFuture"]
        assert future["exchange"] == "NYMEX"


# ==================== Physical Tests ====================

class TestCreateCommodityPhysical:
    """Tests for create_commodity_physical."""

    def test_returns_list(self, physical_gas_data):
        result = create_commodity_physical(physical_gas_data)
        assert isinstance(result, list)
        assert len(result) == 1

    def test_gas_physical_has_correct_leg_key(self, physical_gas_data):
        result = create_commodity_physical(physical_gas_data)
        structure = result[0]
        assert "commoditySwap" in structure
        swap = structure["commoditySwap"]
        assert "gasPhysicalLeg" in swap

    def test_gas_physical_has_delivery_point(self, physical_gas_data):
        result = create_commodity_physical(physical_gas_data)
        swap = result[0]["commoditySwap"]
        leg = swap["gasPhysicalLeg"]
        assert leg["deliveryPoint"] == "Henry Hub"

    def test_gas_physical_has_gas_type(self, physical_gas_data):
        result = create_commodity_physical(physical_gas_data)
        swap = result[0]["commoditySwap"]
        leg = swap["gasPhysicalLeg"]
        assert leg["gasType"] == "NaturalGas"

    def test_gas_physical_has_fixed_leg(self, physical_gas_data):
        result = create_commodity_physical(physical_gas_data)
        swap = result[0]["commoditySwap"]
        assert "fixedLeg" in swap

    def test_bullion_physical_uses_forward_structure(self, base_trade_data):
        data = {
            **base_trade_data,
            "sub_instrument_type": "bullionPhysical",
            "payerPartyReference": "InternalCo",
            "receiverPartyReference": "ExternalCo",
            "bullionType": "Gold",
            "deliveryLocation": "London",
            "quantityUnit": "TOZ",
            "quantity": 100,
        }
        result = create_commodity_physical(data)
        assert "commodityForward" in result[0]

    def test_invalid_sub_instrument_raises(self, base_trade_data):
        data = {**base_trade_data, "sub_instrument_type": "invalidPhysical"}
        with pytest.raises(ValueError, match="Invalid physical sub-instrument"):
            create_commodity_physical(data)


# ==================== Swaption Tests ====================

class TestCreateCommoditySwaption:
    """Tests for create_commodity_swaption."""

    def test_returns_list(self, swaption_data):
        result = create_commodity_swaption(swaption_data)
        assert isinstance(result, list)
        assert len(result) == 1

    def test_has_commodity_swaption_key(self, swaption_data):
        result = create_commodity_swaption(swaption_data)
        assert "commoditySwaption" in result[0]

    def test_has_premium(self, swaption_data):
        result = create_commodity_swaption(swaption_data)
        swaption = result[0]["commoditySwaption"]
        assert "premium" in swaption
        assert swaption["premium"]["premiumPerUnit"] == 0.15

    def test_premium_total_calculated(self, swaption_data):
        result = create_commodity_swaption(swaption_data)
        swaption = result[0]["commoditySwaption"]
        # premiumPerUnit (0.15) * qty (10000) = 1500
        assert swaption["premium"]["totalPremium"] == 1500

    def test_has_exercise_style(self, swaption_data):
        result = create_commodity_swaption(swaption_data)
        swaption = result[0]["commoditySwaption"]
        assert swaption["exercise"]["exerciseStyle"] == "European"

    def test_has_underlying_swap(self, swaption_data):
        result = create_commodity_swaption(swaption_data)
        swaption = result[0]["commoditySwaption"]
        assert "underlyingSwap" in swaption
        assert "fixedLeg" in swaption["underlyingSwap"]
        assert "floatingLeg" in swaption["underlyingSwap"]

    def test_bermudan_has_exercise_dates(self, swaption_data):
        swaption_data["option_type"] = "Bermudan"
        swaption_data["exercise_dates"] = ["2025-03-01", "2025-06-01"]
        result = create_commodity_swaption(swaption_data)
        swaption = result[0]["commoditySwaption"]
        assert swaption["exercise"]["exerciseDates"] == ["2025-03-01", "2025-06-01"]

    def test_invalid_exercise_style_raises(self, swaption_data):
        swaption_data["option_type"] = "InvalidStyle"
        with pytest.raises(ValueError, match="Invalid exercise style"):
            create_commodity_swaption(swaption_data)

    def test_has_metadata(self, swaption_data):
        result = create_commodity_swaption(swaption_data)
        assert result[0]["metadata"]["type"] == "commoditySwaption"


# ==================== FX Tests ====================

class TestCreateFxTrade:
    """Tests for create_fx_trade."""

    def test_returns_list(self, fx_data):
        result = create_fx_trade(fx_data)
        assert isinstance(result, list)
        assert len(result) == 1

    def test_has_fx_trade_key(self, fx_data):
        result = create_fx_trade(fx_data)
        assert "fxTrade" in result[0]

    def test_has_currency_pair(self, fx_data):
        result = create_fx_trade(fx_data)
        fx = result[0]["fxTrade"]
        assert fx["currencyPair"] == "EUR/USD"

    def test_has_rate(self, fx_data):
        result = create_fx_trade(fx_data)
        fx = result[0]["fxTrade"]
        assert fx["rate"] == 1.0850


# ==================== Cash Tests ====================

class TestCreateCashTrade:
    """Tests for create_cash_trade."""

    def test_returns_list(self, cash_data):
        result = create_cash_trade(cash_data)
        assert isinstance(result, list)
        assert len(result) == 1

    def test_has_cash_trade_key(self, cash_data):
        result = create_cash_trade(cash_data)
        assert "cashTrade" in result[0]

    def test_has_payment_amount(self, cash_data):
        result = create_cash_trade(cash_data)
        cash = result[0]["cashTrade"]
        assert cash["paymentAmount"]["amount"] == 50000
        assert cash["paymentAmount"]["currency"] == "USD"

    def test_has_cash_flow_type(self, cash_data):
        result = create_cash_trade(cash_data)
        cash = result[0]["cashTrade"]
        assert cash["cashFlowType"] == "FeePayment"

    def test_has_description(self, cash_data):
        result = create_cash_trade(cash_data)
        cash = result[0]["cashTrade"]
        assert cash["description"] == "Monthly clearing fee"

    def test_has_metadata(self, cash_data):
        result = create_cash_trade(cash_data)
        assert result[0]["metadata"]["type"] == "cashTrade"

    def test_without_optional_fields(self, base_trade_data):
        data = {
            **base_trade_data,
            "instrument": "cash",
            "payment_date": "2024-12-15",
            "payment_amount": 10000,
            "payment_currency": "USD",
            "cash_flow_type": "MarginCall",
        }
        result = create_cash_trade(data)
        cash = result[0]["cashTrade"]
        assert "description" not in cash
        assert "relatedTradeId" not in cash


# ==================== Trade Header Tests ====================

class TestCreateTradeHeader:
    """Tests for create_trade_header."""

    def test_returns_dict(self, base_trade_data):
        result = create_trade_header(base_trade_data)
        assert isinstance(result, dict)

    def test_has_metadata_type(self, base_trade_data):
        result = create_trade_header(base_trade_data)
        assert result.get("metadata", {}).get("type") == "tradeHeader"


# ==================== Trade Footer Tests ====================

class TestCreateTradeFooter:
    """Tests for create_trade_footer."""

    def test_returns_dict(self, base_trade_data):
        result = create_trade_footer(base_trade_data)
        assert isinstance(result, dict)

    def test_has_metadata_type(self, base_trade_data):
        result = create_trade_footer(base_trade_data)
        assert result.get("metadata", {}).get("type") == "tradeFooter"
