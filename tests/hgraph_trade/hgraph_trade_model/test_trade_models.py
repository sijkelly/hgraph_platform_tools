"""Tests for all instrument model creators."""

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

# ==================== Swap ====================


def test_fixed_float_swap_returns_list(swap_fixed_float_data):
    result = create_commodity_swap(swap_fixed_float_data, "fixedFloat")
    assert isinstance(result, list)
    assert len(result) == 1


@pytest.mark.parametrize("key", ["commoditySwap"])
def test_fixed_float_swap_top_key(swap_fixed_float_data, key):
    result = create_commodity_swap(swap_fixed_float_data, "fixedFloat")
    assert key in result[0]


@pytest.mark.parametrize("leg_key", ["fixedLeg", "floatingLeg"])
def test_fixed_float_swap_has_legs(swap_fixed_float_data, leg_key):
    result = create_commodity_swap(swap_fixed_float_data, "fixedFloat")
    assert leg_key in result[0]["commoditySwap"]


def test_fixed_float_swap_settlement_currency(swap_fixed_float_data):
    result = create_commodity_swap(swap_fixed_float_data, "fixedFloat")
    assert result[0]["commoditySwap"]["settlementCurrency"] == "USD"


def test_float_float_swap_returns_list(swap_float_float_data):
    result = create_commodity_swap(swap_float_float_data, "floatFloat")
    assert isinstance(result, list)
    assert len(result) == 1


@pytest.mark.parametrize("leg_key", ["floatLeg1", "floatLeg2"])
def test_float_float_swap_has_legs(swap_float_float_data, leg_key):
    result = create_commodity_swap(swap_float_float_data, "floatFloat")
    assert leg_key in result[0]["commoditySwap"]


def test_swap_unsupported_sub_instrument_raises(swap_fixed_float_data):
    with pytest.raises(ValueError, match="Unsupported sub-instrument"):
        create_commodity_swap(swap_fixed_float_data, "invalidType")


# ==================== Option ====================


def test_option_returns_single_item(option_data):
    result = create_commodity_option(option_data)
    assert isinstance(result, list)
    assert len(result) == 1
    assert "commodityOption" in result[0]


def test_option_strike_price(option_data):
    option = create_commodity_option(option_data)[0]["commodityOption"]
    assert option["strikePrice"] == 4.00


def test_option_premium_calculation(option_data):
    option = create_commodity_option(option_data)[0]["commodityOption"]
    assert option["premium"]["totalPremium"] == 2500  # 0.25 * 10000


@pytest.mark.parametrize(
    "style,has_dates",
    [
        ("European", False),
        ("Bermudan", True),
    ],
)
def test_option_exercise_styles(option_data, style, has_dates):
    option_data["option_type"] = style
    if has_dates:
        option_data["exercise_dates"] = ["2025-06-01", "2025-09-01"]
    result = create_commodity_option(option_data)[0]["commodityOption"]
    assert result["exerciseStyle"] == style
    if has_dates:
        assert result["exerciseDates"] == ["2025-06-01", "2025-09-01"]


def test_option_invalid_exercise_style_raises(option_data):
    option_data["option_type"] = "InvalidStyle"
    with pytest.raises(ValueError, match="Invalid exercise style"):
        create_commodity_option(option_data)


# ==================== Forward ====================


def test_forward_returns_single_item(forward_data):
    result = create_commodity_forward(forward_data)
    assert isinstance(result, list)
    assert len(result) == 1
    assert "commodityForward" in result[0]


def test_forward_fixed_price(forward_data):
    assert create_commodity_forward(forward_data)[0]["commodityForward"]["fixedPrice"] == 75.50


def test_forward_delivery_location(forward_data):
    assert create_commodity_forward(forward_data)[0]["commodityForward"]["deliveryLocation"] == "Houston"


# ==================== Future ====================


def test_future_returns_single_item(future_data):
    result = create_commodity_future(future_data)
    assert isinstance(result, list)
    assert len(result) == 1
    assert "commodityFuture" in result[0]


def test_future_contract_price(future_data):
    assert create_commodity_future(future_data)[0]["commodityFuture"]["contractPrice"] == 76.25


def test_future_exchange(future_data):
    assert create_commodity_future(future_data)[0]["commodityFuture"]["exchange"] == "NYMEX"


# ==================== Physical ====================


def test_physical_gas_returns_single_item(physical_gas_data):
    result = create_commodity_physical(physical_gas_data)
    assert isinstance(result, list)
    assert len(result) == 1


def test_physical_gas_has_correct_structure(physical_gas_data):
    structure = create_commodity_physical(physical_gas_data)[0]
    assert "commoditySwap" in structure
    assert "gasPhysicalLeg" in structure["commoditySwap"]


def test_physical_gas_delivery_point(physical_gas_data):
    leg = create_commodity_physical(physical_gas_data)[0]["commoditySwap"]["gasPhysicalLeg"]
    assert leg["deliveryPoint"] == "Henry Hub"
    assert leg["gasType"] == "NaturalGas"


def test_physical_gas_has_fixed_leg(physical_gas_data):
    swap = create_commodity_physical(physical_gas_data)[0]["commoditySwap"]
    assert "fixedLeg" in swap


def test_bullion_physical_uses_forward_structure(base_trade_data):
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
    assert "commodityForward" in create_commodity_physical(data)[0]


def test_physical_invalid_sub_instrument_raises(base_trade_data):
    data = {**base_trade_data, "sub_instrument_type": "invalidPhysical"}
    with pytest.raises(ValueError, match="Invalid physical sub-instrument"):
        create_commodity_physical(data)


# ==================== Swaption ====================


def test_swaption_returns_single_item(swaption_data):
    result = create_commodity_swaption(swaption_data)
    assert isinstance(result, list)
    assert len(result) == 1
    assert "commoditySwaption" in result[0]


def test_swaption_premium(swaption_data):
    swaption = create_commodity_swaption(swaption_data)[0]["commoditySwaption"]
    assert swaption["premium"]["premiumPerUnit"] == 0.15
    assert swaption["premium"]["totalPremium"] == 1500  # 0.15 * 10000


def test_swaption_exercise_style(swaption_data):
    swaption = create_commodity_swaption(swaption_data)[0]["commoditySwaption"]
    assert swaption["exercise"]["exerciseStyle"] == "European"


@pytest.mark.parametrize("underlying_key", ["fixedLeg", "floatingLeg"])
def test_swaption_underlying_swap(swaption_data, underlying_key):
    swaption = create_commodity_swaption(swaption_data)[0]["commoditySwaption"]
    assert "underlyingSwap" in swaption
    assert underlying_key in swaption["underlyingSwap"]


def test_swaption_bermudan_exercise_dates(swaption_data):
    swaption_data["option_type"] = "Bermudan"
    swaption_data["exercise_dates"] = ["2025-03-01", "2025-06-01"]
    swaption = create_commodity_swaption(swaption_data)[0]["commoditySwaption"]
    assert swaption["exercise"]["exerciseDates"] == ["2025-03-01", "2025-06-01"]


def test_swaption_invalid_exercise_style_raises(swaption_data):
    swaption_data["option_type"] = "InvalidStyle"
    with pytest.raises(ValueError, match="Invalid exercise style"):
        create_commodity_swaption(swaption_data)


def test_swaption_metadata(swaption_data):
    assert create_commodity_swaption(swaption_data)[0]["metadata"]["type"] == "commoditySwaption"


# ==================== FX ====================


def test_fx_returns_single_item(fx_data):
    result = create_fx_trade(fx_data)
    assert isinstance(result, list)
    assert len(result) == 1
    assert "fxTrade" in result[0]


def test_fx_currency_pair(fx_data):
    assert create_fx_trade(fx_data)[0]["fxTrade"]["currencyPair"] == "EUR/USD"


def test_fx_rate(fx_data):
    assert create_fx_trade(fx_data)[0]["fxTrade"]["rate"] == 1.0850


# ==================== Cash ====================


def test_cash_returns_single_item(cash_data):
    result = create_cash_trade(cash_data)
    assert isinstance(result, list)
    assert len(result) == 1
    assert "cashTrade" in result[0]


def test_cash_payment_amount(cash_data):
    cash = create_cash_trade(cash_data)[0]["cashTrade"]
    assert cash["paymentAmount"]["amount"] == 50000
    assert cash["paymentAmount"]["currency"] == "USD"


def test_cash_flow_type(cash_data):
    assert create_cash_trade(cash_data)[0]["cashTrade"]["cashFlowType"] == "FeePayment"


def test_cash_description(cash_data):
    assert create_cash_trade(cash_data)[0]["cashTrade"]["description"] == "Monthly clearing fee"


def test_cash_metadata(cash_data):
    assert create_cash_trade(cash_data)[0]["metadata"]["type"] == "cashTrade"


def test_cash_without_optional_fields(base_trade_data):
    data = {
        **base_trade_data,
        "instrument": "cash",
        "payment_date": "2024-12-15",
        "payment_amount": 10000,
        "payment_currency": "USD",
        "cash_flow_type": "MarginCall",
    }
    cash = create_cash_trade(data)[0]["cashTrade"]
    assert "description" not in cash
    assert "relatedTradeId" not in cash


# ==================== Trade Header / Footer ====================


def test_trade_header_returns_dict(base_trade_data):
    result = create_trade_header(base_trade_data)
    assert isinstance(result, dict)
    assert result.get("metadata", {}).get("type") == "tradeHeader"


def test_trade_footer_returns_dict(base_trade_data):
    result = create_trade_footer(base_trade_data)
    assert isinstance(result, dict)
    assert result.get("metadata", {}).get("type") == "tradeFooter"
