"""Tests for the order core's database layer, on SQLite and (when configured) PostgreSQL."""

import threading

import pytest

from hgraph_oms.db import POSTGRES, SQLITE, Database, close_databases, get_database
from hgraph_oms.engine import OrderEngine
from hgraph_oms.model import NewOrder, OrderDetails
from hgraph_oms.proposals import CONFIRMED, DISMISSED, PROPOSED, ProposalStore

_ITEMS = "CREATE TABLE IF NOT EXISTS items ({seq_column} name TEXT PRIMARY KEY, amount DOUBLE PRECISION NOT NULL)"


@pytest.fixture
def db(oms_db):
    database = get_database(oms_db)
    database.create(_ITEMS.format(seq_column=database.seq_column))
    return database


# ---------------------------------------------------------------------------
# Targets
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "target,dialect",
    [
        ("orders.db", SQLITE),
        ("/var/data/orders.db", SQLITE),
        ("postgresql://oms@db.example.test/oms", POSTGRES),
        ("postgres://oms@db.example.test/oms", POSTGRES),
    ],
)
def test_dialect_comes_from_the_target(target, dialect):
    assert Database(target).dialect == dialect


def test_description_leaves_out_credentials():
    database = Database("postgresql://oms:s3cret@db.example.test:5433/trading?sslmode=require")
    assert database.description == "postgresql://db.example.test:5433/trading"
    assert "s3cret" not in database.description


def test_empty_target_is_refused():
    with pytest.raises(ValueError):
        Database("")


def test_placeholders_are_translated_for_postgres():
    postgres = Database("postgresql://db.example.test/oms")
    assert postgres.sql("SELECT a FROM t WHERE b = ? AND c = ?") == "SELECT a FROM t WHERE b = %s AND c = %s"
    assert Database("orders.db").sql("SELECT a FROM t WHERE b = ?") == "SELECT a FROM t WHERE b = ?"
    with pytest.raises(ValueError):
        postgres.sql("SELECT a FROM t WHERE b LIKE 'x%'")


def test_shared_database_is_reused_per_target(tmp_path):
    target = str(tmp_path / "shared.db")
    assert get_database(target) is get_database(target)
    assert get_database(get_database(target)) is get_database(target)
    close_databases()


# ---------------------------------------------------------------------------
# Statements
# ---------------------------------------------------------------------------


def test_rows_come_back_as_dicts_in_insertion_order(db):
    db.executemany("INSERT INTO items (name, amount) VALUES (?, ?)", [("c", 3.5), ("a", 1.25), ("b", 2.0)])

    rows = db.query(f"SELECT name, amount FROM items ORDER BY {db.row_order()}")

    assert rows == [{"name": "c", "amount": 3.5}, {"name": "a", "amount": 1.25}, {"name": "b", "amount": 2.0}]


def test_execute_reports_rows_changed(db):
    db.executemany("INSERT INTO items (name, amount) VALUES (?, ?)", [("a", 1.0), ("b", 2.0)])
    assert db.execute("UPDATE items SET amount = ? WHERE name = ?", (9.0, "a")) == 1
    assert db.execute("UPDATE items SET amount = ? WHERE name = ?", (9.0, "missing")) == 0


def test_upsert_keeps_the_row_in_place(db):
    upsert = "INSERT INTO items (name, amount) VALUES (?, ?) ON CONFLICT(name) DO UPDATE SET amount = excluded.amount"
    db.execute(upsert, ("a", 1.0))
    db.execute(upsert, ("b", 2.0))
    db.execute(upsert, ("a", 5.0))

    assert db.query(f"SELECT name, amount FROM items ORDER BY {db.row_order()}") == [
        {"name": "a", "amount": 5.0},
        {"name": "b", "amount": 2.0},
    ]


def test_failed_transaction_changes_nothing_and_the_database_stays_usable(db):
    with pytest.raises(Exception):
        with db.transaction() as tx:
            tx.execute("INSERT INTO items (name, amount) VALUES (?, ?)", ("a", 1.0))
            tx.execute("INSERT INTO items (name, amount) VALUES (?, ?)", ("a", 2.0))  # duplicate key

    assert db.query("SELECT name FROM items") == []
    db.execute("INSERT INTO items (name, amount) VALUES (?, ?)", ("a", 1.0))
    assert db.query("SELECT name FROM items") == [{"name": "a"}]


def test_create_is_repeatable(db):
    db.create(_ITEMS.format(seq_column=db.seq_column))
    db.create(_ITEMS.format(seq_column=db.seq_column))


def test_concurrent_writers_all_succeed(db):
    errors: list[BaseException] = []

    def write(worker: int) -> None:
        try:
            for i in range(20):
                db.execute("INSERT INTO items (name, amount) VALUES (?, ?)", (f"{worker}-{i}", float(i)))
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=write, args=(worker,)) for worker in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert not errors
    assert db.query("SELECT COUNT(*) AS n FROM items") == [{"n": 160}]


# ---------------------------------------------------------------------------
# Single-writer lock
# ---------------------------------------------------------------------------


def test_lock_is_exclusive_between_processes(oms_db):
    ours, theirs = Database(oms_db), Database(oms_db)  # two instances stand in for two processes
    try:
        if ours.dialect == SQLITE:
            assert ours.try_lock("engine") and theirs.try_lock("engine")  # not enforced on SQLite
            return

        assert ours.try_lock("engine")
        assert not theirs.try_lock("engine")
        assert not ours.try_lock("engine")
        assert theirs.try_lock("something-else")

        ours.unlock("engine")
        assert theirs.try_lock("engine")
    finally:
        ours.close()
        theirs.close()


def test_second_engine_on_the_same_postgres_log_refuses_to_start(oms_db):
    first = OrderEngine(oms_db)
    first.start()
    try:
        if get_database(oms_db).dialect == SQLITE:
            return
        other_process = Database(oms_db)
        try:
            second = OrderEngine(oms_db)
            second._db = other_process
            with pytest.raises(RuntimeError, match="Another order engine is already running"):
                second.start()
        finally:
            other_process.close()
    finally:
        first.stop()

    # Once the first has stopped, the log can be taken over
    successor = OrderEngine(oms_db)
    successor.start()
    successor.stop()


# ---------------------------------------------------------------------------
# Proposal store
# ---------------------------------------------------------------------------

_FIELDS = {"instrument": "Henry Hub Natural Gas", "side": "Buy", "quantity": "10000"}


def test_proposals_are_listed_newest_first(oms_db):
    store = ProposalStore(oms_db)
    first = store.create(_FIELDS, proposed_by="ai-agent", source_text="buy 10k HH", rationale="as asked")
    second = store.create({**_FIELDS, "side": "Sell"}, proposed_by="ai-agent")

    assert [p.proposal_id for p in store.proposals()] == [second.proposal_id, first.proposal_id]
    assert store.get(first.proposal_id) == first
    assert store.get("PRP-MISSING") is None


def test_proposal_can_only_be_decided_once(oms_db):
    store = ProposalStore(oms_db)
    proposal = store.create(_FIELDS, proposed_by="ai-agent")

    confirmed = store.decide(proposal.proposal_id, CONFIRMED, decided_by="trader1", order_id="ORD-1")
    assert (confirmed.status, confirmed.decided_by, confirmed.order_id) == (CONFIRMED, "trader1", "ORD-1")
    assert store.decide(proposal.proposal_id, DISMISSED, decided_by="trader2") is None
    assert store.proposals(status=PROPOSED) == []

    assert store.reopen(proposal.proposal_id).status == PROPOSED


def test_engine_recovers_orders_from_the_log(oms_db):
    details = OrderDetails(instrument="Henry Hub Natural Gas", side="Buy", quantity=10_000.0, price=3.5)
    first = OrderEngine(oms_db)
    first.start()
    try:
        assert first.submit(NewOrder(order_id="ORD-1", user_id="trader1", details=details)).accepted
    finally:
        first.stop()

    second = OrderEngine(oms_db)
    second.start()
    try:
        (order,) = second.orders()
        assert (order.order_id, order.details, order.created_by) == ("ORD-1", details, "trader1")
    finally:
        second.stop()
