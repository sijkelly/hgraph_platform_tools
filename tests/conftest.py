"""
Shared test fixtures for hgraph_platform_tools test suite.
"""

import pytest


@pytest.fixture
def base_trade_data():
    """Base trade data common to all instrument types."""
    return {
        "trade_id": "TEST-001",
        "trade_date": "2024-11-20",
        "buy_sell": "Buy",
        "qty": 10000,
        "unit": "MMBTU",
        "currency": "USD",
        "effective_date": "2024-12-01",
        "termination_date": "2025-12-01",
        "internal_party": "InternalCo",
        "external_party": "ExternalCo",
        "internal_portfolio": "PortfolioA",
        "external_portfolio": "PortfolioB",
        "internal_trader": "TraderX",
        "external_trader": "TraderY",
        "tradeType": "newTrade",
        "instrument": "outright",
        "counterparty": {
            "internal": "InternalCo",
            "external": "ExternalCo"
        },
        "portfolio": {
            "internal": "PortfolioA",
            "external": "PortfolioB"
        },
        "traders": {
            "internal": "TraderX",
            "external": "TraderY"
        },
    }


@pytest.fixture
def swap_fixed_float_data(base_trade_data):
    """Trade data for a fixed/float commodity swap."""
    return {
        **base_trade_data,
        "instrument": "outright",
        "fixedLeg.payerPartyReference": "InternalCo",
        "fixedLeg.receiverPartyReference": "ExternalCo",
        "fixedLeg.price": 3.50,
        "fixedLeg.priceCurrency": "USD",
        "fixedLeg.priceUnit": "MMBTU",
        "fixedLeg.quantityUnit": "MMBTU",
        "fixedLeg.quantityFrequency": "PerCalendarDay",
        "fixedLeg.quantity": 10000,
        "floatingLeg.payerPartyReference": "ExternalCo",
        "floatingLeg.receiverPartyReference": "InternalCo",
        "floatingLeg.instrumentId": "NATURAL_GAS-HENRY_HUB",
        "floatingLeg.specifiedPrice": "Settlement",
        "floatingLeg.quantityUnit": "MMBTU",
        "floatingLeg.quantityFrequency": "PerCalendarDay",
        "floatingLeg.quantity": 10000,
        "settlementCurrency": "USD",
    }


@pytest.fixture
def swap_float_float_data(base_trade_data):
    """Trade data for a float/float commodity swap."""
    return {
        **base_trade_data,
        "instrument": "crack_spread",
        "floatLeg1.payerPartyReference": "InternalCo",
        "floatLeg1.receiverPartyReference": "ExternalCo",
        "floatLeg1.instrumentId": "CRUDE_OIL-WTI",
        "floatLeg1.specifiedPrice": "Settlement",
        "floatLeg1.quantityUnit": "BBL",
        "floatLeg1.quantityFrequency": "PerCalendarDay",
        "floatLeg1.quantity": 5000,
        "floatLeg2.payerPartyReference": "ExternalCo",
        "floatLeg2.receiverPartyReference": "InternalCo",
        "floatLeg2.instrumentId": "HEATING_OIL-NYH",
        "floatLeg2.specifiedPrice": "Settlement",
        "floatLeg2.quantityUnit": "BBL",
        "floatLeg2.quantityFrequency": "PerCalendarDay",
        "floatLeg2.quantity": 5000,
        "floatLeg2.spreadCurrency": "USD",
        "floatLeg2.spreadAmount": 0.50,
        "settlementCurrency": "USD",
    }


@pytest.fixture
def option_data(base_trade_data):
    """Trade data for a commodity option."""
    return {
        **base_trade_data,
        "instrument": "option",
        "option_type": "European",
        "strike_price": 4.00,
        "premium_payment_date": "2024-12-05",
        "premium_per_unit": 0.25,
        "commodity": "NaturalGas",
        "expirationDate": "2025-11-30",
    }


@pytest.fixture
def forward_data(base_trade_data):
    """Trade data for a commodity forward."""
    return {
        **base_trade_data,
        "instrument": "forward",
        "fixed_price": 75.50,
        "delivery_location": "Houston",
        "commodity": "CrudeOil",
    }


@pytest.fixture
def future_data(base_trade_data):
    """Trade data for a commodity future."""
    return {
        **base_trade_data,
        "instrument": "future",
        "contract_price": 76.25,
        "expiry_date": "2025-12-19",
        "exchange": "NYMEX",
        "delivery_location": "Cushing",
        "commodity": "CrudeOil",
    }


@pytest.fixture
def fx_data(base_trade_data):
    """Trade data for an FX trade."""
    return {
        **base_trade_data,
        "instrument": "fx",
        "settlement_date": "2024-12-03",
        "currency_pair": "EUR/USD",
        "notional_amount": 1000000,
        "rate": 1.0850,
    }


@pytest.fixture
def swaption_data(base_trade_data):
    """Trade data for a commodity swaption."""
    return {
        **base_trade_data,
        "instrument": "swaption",
        "option_type": "European",
        "premium_payment_date": "2024-12-05",
        "premium_per_unit": 0.15,
        "expiration_date": "2025-06-01",
        "swap_effective_date": "2025-06-15",
        "swap_termination_date": "2026-06-15",
        "settlement_currency": "USD",
        "fixedLeg.payerPartyReference": "InternalCo",
        "fixedLeg.receiverPartyReference": "ExternalCo",
        "fixedLeg.price": 3.50,
        "fixedLeg.priceCurrency": "USD",
        "fixedLeg.priceUnit": "MMBTU",
        "fixedLeg.quantityUnit": "MMBTU",
        "fixedLeg.quantityFrequency": "PerCalendarDay",
        "fixedLeg.quantity": 10000,
        "floatingLeg.payerPartyReference": "ExternalCo",
        "floatingLeg.receiverPartyReference": "InternalCo",
        "floatingLeg.instrumentId": "NATURAL_GAS-HENRY_HUB",
        "floatingLeg.specifiedPrice": "Settlement",
        "floatingLeg.quantityUnit": "MMBTU",
        "floatingLeg.quantityFrequency": "PerCalendarDay",
        "floatingLeg.quantity": 10000,
    }


@pytest.fixture
def cash_data(base_trade_data):
    """Trade data for a cash trade."""
    return {
        **base_trade_data,
        "instrument": "cash",
        "payment_date": "2024-12-15",
        "payment_amount": 50000,
        "payment_currency": "USD",
        "cash_flow_type": "FeePayment",
        "payer_party": "InternalCo",
        "receiver_party": "ExternalCo",
        "description": "Monthly clearing fee",
    }


@pytest.fixture
def physical_gas_data(base_trade_data):
    """Trade data for a gas physical trade."""
    return {
        **base_trade_data,
        "instrument": "physical",
        "sub_instrument_type": "gasPhysical",
        "payerPartyReference": "InternalCo",
        "receiverPartyReference": "ExternalCo",
        "settlementCurrency": "USD",
        "quantityUnit": "MMBTU",
        "quantityFrequency": "PerCalendarDay",
        "quantity": 10000,
        "gasType": "NaturalGas",
        "deliveryPoint": "Henry Hub",
        "hasFixedLeg": True,
        "fixedLeg.payerPartyReference": "ExternalCo",
        "fixedLeg.receiverPartyReference": "InternalCo",
        "fixedLeg.price": 3.50,
        "fixedLeg.priceCurrency": "USD",
        "fixedLeg.priceUnit": "MMBTU",
        "fixedLeg.quantityReference": "deliveryQuantity",
    }
