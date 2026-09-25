#!/usr/bin/env python3
"""Seed the OMS databases with demo data for local development.

Usage:
    uv run python scripts/seed_demo_data.py [--db-dir ./data]
"""

import argparse
import os
from datetime import date

from hg_oap.credit.credit_limit import (
    CreditLimit,
    CreditLimitType,
    CreditStatus,
    CreditUtilization,
)
from hg_oap.parties.party import LegalEntity, PartyClassification
from hg_oap.parties.agreement import MasterAgreement, MasterAgreementType
from hg_oap.parties.relationship import ClearingStatus, TradingRelationship
from hg_oap.portfolio.portfolio_info import (
    BookInfo,
    BookType,
    PortfolioInfo,
    PortfolioStatus,
    PortfolioType,
)

from hgraph_static_admin.credit_store import (
    init_credit_db,
    upsert_credit_limit,
    upsert_credit_utilization,
)
from hgraph_static_admin.party_store import (
    init_party_db,
    upsert_legal_entity,
    upsert_trading_relationship,
)
from hgraph_static_admin.portfolio_store import (
    init_portfolio_db,
    upsert_book,
    upsert_portfolio,
)
from hgraph_entitlements import get_db_connection, initialize_db, update_user_role
from hgraph_api.services.order_store import init_order_db, create_order


def seed(db_dir: str) -> None:
    os.makedirs(db_dir, exist_ok=True)

    credit_db = os.path.join(db_dir, "credit_data.db")
    party_db = os.path.join(db_dir, "party_data.db")
    portfolio_db = os.path.join(db_dir, "portfolio_data.db")
    order_db = os.path.join(db_dir, "order_data.db")

    # -----------------------------------------------------------------------
    # Parties
    # -----------------------------------------------------------------------
    print("Seeding parties...")
    init_party_db(party_db)

    entities = [
        LegalEntity(
            symbol="HGDEALER",
            name="HGraph Dealer Corp",
            classification=PartyClassification.SWAP_DEALER,
            lei="529900HGDEALER00LEI",
            jurisdiction="US",
        ),
        LegalEntity(
            symbol="ACME",
            name="ACME Energy Corp",
            classification=PartyClassification.NON_FINANCIAL_END_USER,
            lei="529900ACMEENERGY0LEI",
            jurisdiction="US",
        ),
        LegalEntity(
            symbol="GLOBEX",
            name="Globex Power Trading",
            classification=PartyClassification.SWAP_DEALER,
            lei="529900GLOBEXPOWER0LE",
            jurisdiction="UK",
        ),
        LegalEntity(
            symbol="INITECH",
            name="Initech Gas & Oil",
            classification=PartyClassification.NON_FINANCIAL_END_USER,
            lei="529900INITECHGAS0LEI",
            jurisdiction="US",
        ),
        LegalEntity(
            symbol="MASSIVEDN",
            name="Massive Dynamic Energy",
            classification=PartyClassification.SWAP_DEALER,
            lei="529900MASSIVEDYN0LEI",
            jurisdiction="DE",
        ),
    ]
    for e in entities:
        upsert_legal_entity(party_db, e)

    relationships = [
        TradingRelationship(
            internal_party=entities[0],
            external_party=entities[1],
            clearing_status=ClearingStatus.BILATERAL,
            isda=MasterAgreement(
                agreement_type=MasterAgreementType.ISDA,
                version="2002",
                credit_support_annex=True,
                governing_law="New York",
            ),
            dropcopy_enabled=False,
            internal_portfolio="GAS-BOOK-1",
        ),
        TradingRelationship(
            internal_party=entities[0],
            external_party=entities[2],
            clearing_status=ClearingStatus.CLEARED,
            isda=MasterAgreement(
                agreement_type=MasterAgreementType.ISDA,
                version="2002",
                credit_support_annex=True,
                governing_law="English",
            ),
            dropcopy_enabled=True,
            internal_portfolio="POWER-BOOK-1",
        ),
        TradingRelationship(
            internal_party=entities[0],
            external_party=entities[3],
            clearing_status=ClearingStatus.BILATERAL,
            isda=MasterAgreement(
                agreement_type=MasterAgreementType.ISDA,
                version="2002",
                credit_support_annex=False,
                governing_law="New York",
            ),
            dropcopy_enabled=False,
            internal_portfolio="GAS-BOOK-1",
        ),
        TradingRelationship(
            internal_party=entities[0],
            external_party=entities[4],
            clearing_status=ClearingStatus.CLEARED,
            isda=MasterAgreement(
                agreement_type=MasterAgreementType.ISDA,
                version="2021",
                credit_support_annex=True,
                governing_law="English",
            ),
            dropcopy_enabled=True,
            internal_portfolio="POWER-BOOK-2",
        ),
    ]
    for r in relationships:
        upsert_trading_relationship(party_db, r)

    # -----------------------------------------------------------------------
    # Credit
    # -----------------------------------------------------------------------
    print("Seeding credit limits and utilizations...")
    init_credit_db(credit_db)

    limits = [
        CreditLimit(
            counterparty_symbol="ACME",
            limit_type=CreditLimitType.BILATERAL,
            limit_amount=50_000_000.0,
            limit_currency="USD",
            effective_date=date(2025, 1, 1),
            expiry_date=date(2026, 12, 31),
            status=CreditStatus.ACTIVE,
            approved_by="Credit Committee",
            last_review_date=date(2025, 6, 15),
        ),
        CreditLimit(
            counterparty_symbol="ACME",
            limit_type=CreditLimitType.CLEARED,
            limit_amount=100_000_000.0,
            limit_currency="USD",
            status=CreditStatus.ACTIVE,
        ),
        CreditLimit(
            counterparty_symbol="GLOBEX",
            limit_type=CreditLimitType.TOTAL,
            limit_amount=200_000_000.0,
            limit_currency="USD",
            effective_date=date(2025, 3, 1),
            expiry_date=date(2026, 3, 1),
            status=CreditStatus.ACTIVE,
            approved_by="Risk Board",
        ),
        CreditLimit(
            counterparty_symbol="GLOBEX",
            limit_type=CreditLimitType.BILATERAL,
            limit_amount=75_000_000.0,
            limit_currency="USD",
            status=CreditStatus.ACTIVE,
        ),
        CreditLimit(
            counterparty_symbol="INITECH",
            limit_type=CreditLimitType.BILATERAL,
            limit_amount=30_000_000.0,
            limit_currency="USD",
            effective_date=date(2025, 1, 1),
            expiry_date=date(2025, 12, 31),
            status=CreditStatus.ACTIVE,
            approved_by="Credit Committee",
            last_review_date=date(2025, 9, 1),
        ),
        CreditLimit(
            counterparty_symbol="MASSIVEDN",
            limit_type=CreditLimitType.TOTAL,
            limit_amount=150_000_000.0,
            limit_currency="EUR",
            status=CreditStatus.ACTIVE,
            approved_by="Risk Board",
        ),
    ]
    for cl in limits:
        upsert_credit_limit(credit_db, cl)

    utilizations = [
        CreditUtilization(
            counterparty_symbol="ACME",
            utilized_amount=15_000_000.0,
            limit_amount=50_000_000.0,
            available_amount=35_000_000.0,
            utilization_percentage=30.0,
            as_of_timestamp="2026-03-22T10:00:00+00:00",
        ),
        CreditUtilization(
            counterparty_symbol="GLOBEX",
            utilized_amount=120_000_000.0,
            limit_amount=200_000_000.0,
            available_amount=80_000_000.0,
            utilization_percentage=60.0,
            as_of_timestamp="2026-03-22T10:00:00+00:00",
        ),
        CreditUtilization(
            counterparty_symbol="INITECH",
            utilized_amount=27_000_000.0,
            limit_amount=30_000_000.0,
            available_amount=3_000_000.0,
            utilization_percentage=90.0,
            as_of_timestamp="2026-03-22T10:00:00+00:00",
        ),
        CreditUtilization(
            counterparty_symbol="MASSIVEDN",
            utilized_amount=45_000_000.0,
            limit_amount=150_000_000.0,
            available_amount=105_000_000.0,
            utilization_percentage=30.0,
            as_of_timestamp="2026-03-22T10:00:00+00:00",
        ),
    ]
    for cu in utilizations:
        upsert_credit_utilization(credit_db, cu)

    # -----------------------------------------------------------------------
    # Portfolios & Books
    # -----------------------------------------------------------------------
    print("Seeding portfolios and books...")
    init_portfolio_db(portfolio_db)

    portfolios = [
        PortfolioInfo(
            symbol="GAS-TRADING",
            name="Gas Trading Portfolio",
            portfolio_type=PortfolioType.TRADING,
            owner="Gas Desk",
            base_currency="USD",
            legal_entity_symbol="HGDEALER",
            status=PortfolioStatus.ACTIVE,
        ),
        PortfolioInfo(
            symbol="POWER-TRADING",
            name="Power Trading Portfolio",
            portfolio_type=PortfolioType.TRADING,
            owner="Power Desk",
            base_currency="USD",
            legal_entity_symbol="HGDEALER",
            status=PortfolioStatus.ACTIVE,
        ),
        PortfolioInfo(
            symbol="HEDGING",
            name="Corporate Hedging Portfolio",
            portfolio_type=PortfolioType.HEDGING,
            owner="Treasury",
            base_currency="USD",
            legal_entity_symbol="HGDEALER",
            status=PortfolioStatus.ACTIVE,
        ),
    ]
    for p in portfolios:
        upsert_portfolio(portfolio_db, p)

    books = [
        BookInfo(
            symbol="GAS-BOOK-1",
            name="Gas Trading Book 1",
            book_type=BookType.TRADING,
            owner="Gas Desk",
            base_currency="USD",
            portfolio_symbol="GAS-TRADING",
            status=PortfolioStatus.ACTIVE,
        ),
        BookInfo(
            symbol="GAS-BOOK-2",
            name="Gas Trading Book 2",
            book_type=BookType.TRADING,
            owner="Gas Desk",
            base_currency="USD",
            portfolio_symbol="GAS-TRADING",
            status=PortfolioStatus.ACTIVE,
        ),
        BookInfo(
            symbol="POWER-BOOK-1",
            name="Power Trading Book 1",
            book_type=BookType.TRADING,
            owner="Power Desk",
            base_currency="USD",
            portfolio_symbol="POWER-TRADING",
            status=PortfolioStatus.ACTIVE,
        ),
        BookInfo(
            symbol="POWER-BOOK-2",
            name="Power Trading Book 2",
            book_type=BookType.TRADING,
            owner="Power Desk",
            base_currency="EUR",
            portfolio_symbol="POWER-TRADING",
            status=PortfolioStatus.ACTIVE,
        ),
        BookInfo(
            symbol="HEDGE-BOOK-1",
            name="Hedging Book 1",
            book_type=BookType.HEDGING,
            owner="Treasury",
            base_currency="USD",
            portfolio_symbol="HEDGING",
            status=PortfolioStatus.ACTIVE,
        ),
    ]
    for b in books:
        upsert_book(portfolio_db, b)

    # -----------------------------------------------------------------------
    # Entitlements
    # -----------------------------------------------------------------------
    print("Seeding entitlements...")
    conn = get_db_connection()
    initialize_db(conn)
    update_user_role(conn, "demo", "Trader")
    update_user_role(conn, "admin", "Super User")
    update_user_role(conn, "sales1", "Sales")
    conn.close()

    # -----------------------------------------------------------------------
    # Sample orders
    # -----------------------------------------------------------------------
    print("Seeding sample orders...")
    init_order_db(order_db)

    sample_orders = [
        {
            "instrument": "Henry Hub Natural Gas",
            "side": "Buy",
            "quantity": "50000",
            "unit": "MMBtu",
            "price": "3.45",
            "counterparty": "ACME",
            "delivery_period": "January 2027",
            "order_type": "Limit",
            "portfolio": "GAS-TRADING",
            "book": "GAS-BOOK-1",
        },
        {
            "instrument": "WTI Crude Oil",
            "side": "Sell",
            "quantity": "10000",
            "unit": "bbl",
            "price": "72.50",
            "counterparty": "GLOBEX",
            "delivery_period": "April 2026",
            "order_type": "Limit",
            "portfolio": "GAS-TRADING",
            "book": "GAS-BOOK-2",
        },
        {
            "instrument": "PJM Western Hub Power",
            "side": "Buy",
            "quantity": "200",
            "unit": "MW",
            "price": "45.00",
            "counterparty": "MASSIVEDN",
            "delivery_period": "Q3 2026",
            "order_type": "Limit",
            "portfolio": "POWER-TRADING",
            "book": "POWER-BOOK-1",
        },
        {
            "instrument": "TTF Natural Gas",
            "side": "Buy",
            "quantity": "25000",
            "unit": "MWh",
            "price": "28.75",
            "counterparty": "MASSIVEDN",
            "delivery_period": "Cal 2027",
            "order_type": "Market",
            "portfolio": "POWER-TRADING",
            "book": "POWER-BOOK-2",
        },
        {
            "instrument": "Henry Hub Natural Gas",
            "side": "Sell",
            "quantity": "30000",
            "unit": "MMBtu",
            "price": "3.60",
            "counterparty": "INITECH",
            "delivery_period": "February 2027",
            "order_type": "Limit",
            "portfolio": "GAS-TRADING",
            "book": "GAS-BOOK-1",
        },
    ]
    for o in sample_orders:
        create_order(order_db, o, created_by="seed")

    print(f"\nDemo data seeded in {db_dir}/")
    print("  - 5 legal entities, 4 trading relationships")
    print("  - 6 credit limits, 4 credit utilizations")
    print("  - 3 portfolios, 5 books")
    print("  - 3 users (demo/Trader, admin/Super User, sales1/Sales)")
    print("  - 5 sample orders")
    print(f"\nStart the API:  uv run hgraph-tools serve --db-dir {db_dir}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Seed OMS demo data")
    parser.add_argument("--db-dir", default="./data", help="Directory for SQLite databases (default: ./data)")
    args = parser.parse_args()
    seed(args.db_dir)
