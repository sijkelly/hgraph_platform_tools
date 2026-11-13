"""
The order core's database access: SQLite for development, PostgreSQL for deployment.

A store is opened on a *target*, which is either a SQLite file path or a
``postgresql://`` URL.  :class:`Database` hides the differences that matter to
the stores:

* SQL is written once, with ``?`` placeholders, in the subset both databases
  understand (including ``INSERT ... ON CONFLICT``).
* Rows come back as dicts.
* Tables that are read in insertion order declare :attr:`Database.seq_column`
  and sort by :meth:`Database.row_order`, since PostgreSQL has no ``rowid``.
* PostgreSQL connections are pooled; SQLite opens a connection per operation,
  as before.

``psycopg`` is only needed, and only imported, for PostgreSQL targets.
"""

import logging
import queue
import sqlite3
import threading
from contextlib import contextmanager
from typing import Any, Iterable, Iterator, Sequence
from urllib.parse import urlsplit

__all__ = ("SQLITE", "POSTGRES", "Transaction", "Database", "get_database", "close_databases")

logger = logging.getLogger(__name__)

SQLITE = "sqlite"
POSTGRES = "postgres"

_POSTGRES_SCHEMES = ("postgresql", "postgres")
_DEFAULT_POOL_SIZE = 5
_POOL_TIMEOUT = 30.0

# Advisory locks are per database; including the schema keeps separate schemas independent
_LOCK_KEY_SQL = "hashtextextended('hgraph_oms:' || current_schema() || ':' || ?, 0)"


class Transaction:
    """Statements run inside one transaction. Obtained from :meth:`Database.transaction`."""

    def __init__(self, database: "Database", connection: Any):
        self._database = database
        self._connection = connection

    def execute(self, sql: str, params: Sequence[Any] = ()) -> int:
        """Run a statement and return the number of rows it changed."""
        return self._connection.execute(self._database.sql(sql), tuple(params)).rowcount

    def executemany(self, sql: str, rows: Iterable[Sequence[Any]]) -> None:
        rows = [tuple(row) for row in rows]
        if not rows:
            return
        if self._database.dialect == POSTGRES:
            with self._connection.cursor() as cursor:
                cursor.executemany(self._database.sql(sql), rows)
        else:
            self._connection.executemany(sql, rows)

    def query(self, sql: str, params: Sequence[Any] = ()) -> list[dict[str, Any]]:
        """Run a ``SELECT`` and return its rows as dicts keyed by column name."""
        cursor = self._connection.execute(self._database.sql(sql), tuple(params))
        return [dict(row) for row in cursor.fetchall()]


class Database:
    """One database, opened on a SQLite path or a ``postgresql://`` URL.

    :param target: SQLite file path, or a PostgreSQL connection URL.
    :param pool_size: The most PostgreSQL connections kept open at once.
    """

    def __init__(self, target: str, *, pool_size: int = _DEFAULT_POOL_SIZE):
        if not target:
            raise ValueError("A database path or URL is required")
        self.target = target
        self.dialect = POSTGRES if urlsplit(target).scheme in _POSTGRES_SCHEMES else SQLITE
        self._pool: queue.LifoQueue = queue.LifoQueue()
        self._slots = threading.BoundedSemaphore(pool_size)
        self._locks: dict[str, Any] = {}
        self._closed = False

    # ------------------------------------------------------------------
    # What differs between the two databases
    # ------------------------------------------------------------------

    @property
    def description(self) -> str:
        """Where the data is, without credentials; safe to log."""
        if self.dialect == SQLITE:
            return self.target
        parts = urlsplit(self.target)
        return f"postgresql://{parts.hostname or ''}{':' + str(parts.port) if parts.port else ''}{parts.path}"

    @property
    def seq_column(self) -> str:
        """Column definition, for ``CREATE TABLE``, that records insertion order; see :meth:`row_order`.

        Empty for SQLite, which numbers rows itself.
        """
        return "seq BIGINT GENERATED ALWAYS AS IDENTITY," if self.dialect == POSTGRES else ""

    @property
    def serial_primary_key(self) -> str:
        """Type and constraint of an auto-numbered primary key column."""
        if self.dialect == POSTGRES:
            return "BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY"
        return "INTEGER PRIMARY KEY AUTOINCREMENT"

    def row_order(self, alias: str = "") -> str:
        """The expression to ``ORDER BY`` for insertion order, on a table declared with :attr:`seq_column`."""
        column = "seq" if self.dialect == POSTGRES else "rowid"
        return f"{alias}.{column}" if alias else column

    def sql(self, statement: str) -> str:
        """Translate a statement written with ``?`` placeholders for this database."""
        if self.dialect == SQLITE:
            return statement
        if "%" in statement:
            raise ValueError("SQL for the order stores must not contain '%'")
        return statement.replace("?", "%s")

    # ------------------------------------------------------------------
    # Running statements
    # ------------------------------------------------------------------

    @contextmanager
    def transaction(self) -> Iterator[Transaction]:
        """Run statements in one transaction: committed on exit, rolled back if the block raises."""
        if self.dialect == SQLITE:
            connection = sqlite3.connect(self.target)
            connection.row_factory = sqlite3.Row
            try:
                yield Transaction(self, connection)
                connection.commit()
            finally:
                connection.close()  # an uncommitted transaction is rolled back
            return

        connection = self._acquire()
        healthy = False
        try:
            yield Transaction(self, connection)
            connection.commit()
            healthy = True
        except BaseException:
            try:
                connection.rollback()
                healthy = True
            except Exception:
                logger.warning("Discarding a broken connection to %s", self.description)
            raise
        finally:
            self._release(connection, healthy)

    def execute(self, sql: str, params: Sequence[Any] = ()) -> int:
        """Run one statement in its own transaction and return the number of rows it changed."""
        with self.transaction() as tx:
            return tx.execute(sql, params)

    def executemany(self, sql: str, rows: Iterable[Sequence[Any]]) -> None:
        """Run one statement for every row, all in a single transaction."""
        with self.transaction() as tx:
            tx.executemany(sql, rows)

    def query(self, sql: str, params: Sequence[Any] = ()) -> list[dict[str, Any]]:
        """Run a ``SELECT`` and return its rows as dicts keyed by column name."""
        with self.transaction() as tx:
            return tx.query(sql, params)

    def create(self, *statements: str) -> None:
        """Run schema statements (``CREATE TABLE IF NOT EXISTS`` and the like) in one transaction."""
        with self.transaction() as tx:
            if self.dialect == SQLITE:
                tx.execute("PRAGMA journal_mode = WAL")
            else:
                # Two processes creating the same table at once would otherwise collide
                tx.execute(f"SELECT pg_advisory_xact_lock({_LOCK_KEY_SQL})", ("create-tables",))
            for statement in statements:
                tx.execute(statement)

    # ------------------------------------------------------------------
    # Single-writer lock
    # ------------------------------------------------------------------

    def try_lock(self, name: str) -> bool:
        """Claim a named lock for this process, so that only one writer uses the data at a time.

        On PostgreSQL the lock is held by a dedicated connection until :meth:`unlock` or the process
        ends, and is seen by every process using the same database and schema.  SQLite has no equivalent and the
        lock is always granted.

        :returns: ``False`` if another process (or this one) already holds the lock.
        """
        if self.dialect == SQLITE:
            return True
        if name in self._locks:
            return False
        connection = self._connect(autocommit=True)
        try:
            row = connection.execute(
                self.sql(f"SELECT pg_try_advisory_lock({_LOCK_KEY_SQL}) AS locked"), (name,)
            ).fetchone()
        except BaseException:
            connection.close()
            raise
        if not row["locked"]:
            connection.close()
            return False
        self._locks[name] = connection
        return True

    def unlock(self, name: str) -> None:
        """Release a lock taken with :meth:`try_lock`. Does nothing if it is not held."""
        connection = self._locks.pop(name, None)
        if connection is not None:
            connection.close()  # closing the session releases its advisory locks

    # ------------------------------------------------------------------
    # PostgreSQL connections
    # ------------------------------------------------------------------

    def _connect(self, *, autocommit: bool = False) -> Any:
        try:
            import psycopg
            from psycopg.rows import dict_row
        except ImportError:
            raise RuntimeError(
                "The 'psycopg' package is required for PostgreSQL. Install it with: uv pip install -e '.[postgres]'"
            ) from None
        return psycopg.connect(self.target, autocommit=autocommit, row_factory=dict_row, connect_timeout=10)

    def _acquire(self) -> Any:
        if self._closed:
            raise RuntimeError(f"The database {self.description} has been closed")
        if not self._slots.acquire(timeout=_POOL_TIMEOUT):
            raise TimeoutError(f"No connection to {self.description} became free within {_POOL_TIMEOUT:.0f}s")
        try:
            while True:
                try:
                    connection = self._pool.get_nowait()
                except queue.Empty:
                    return self._connect()
                if not connection.closed and not connection.broken:
                    return connection
                connection.close()
        except BaseException:
            self._slots.release()
            raise

    def _release(self, connection: Any, healthy: bool) -> None:
        try:
            if healthy and not self._closed and not connection.closed and not connection.broken:
                self._pool.put(connection)
            else:
                connection.close()
        finally:
            self._slots.release()

    def close(self) -> None:
        """Close pooled connections and release any locks. The database must not be used afterwards."""
        self._closed = True
        for name in list(self._locks):
            self.unlock(name)
        while True:
            try:
                self._pool.get_nowait().close()
            except queue.Empty:
                return


# ---------------------------------------------------------------------------
# Shared instances
# ---------------------------------------------------------------------------

_databases: dict[str, Database] = {}
_databases_lock = threading.Lock()


def get_database(target: "str | Database") -> Database:
    """Return the shared :class:`Database` for a target, so stores on one database share its pool."""
    if isinstance(target, Database):
        return target
    with _databases_lock:
        database = _databases.get(target)
        if database is None:
            database = _databases[target] = Database(target)
        return database


def close_databases() -> None:
    """Close every shared database, e.g. at shutdown or between tests."""
    with _databases_lock:
        databases = list(_databases.values())
        _databases.clear()
    for database in databases:
        database.close()
