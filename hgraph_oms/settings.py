"""
Deployment settings shared by everything that opens a database.

Kept free of the API's dependencies so that the command-line tools and the
Kafka subscribers resolve the tenant and the database exactly as the API does.

* ``TENANT_ID`` names the customer a deployment serves (empty: single-tenant).
* ``DATABASE_URL`` moves every store into one PostgreSQL database, in the
  tenant's own schema; without it each store is a SQLite file.
"""

import os
import re

from hgraph_oms.db import with_schema
from secure_config import config

__all__ = ("tenant_id", "database_schema", "database_target")

_TENANT_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,62}$")


def _setting(name: str) -> str:
    return (os.getenv(name) or config.get(name) or "").strip()


def tenant_id() -> str:
    """Return the tenant this deployment serves, or ``""`` if it is single-tenant.

    :raises RuntimeError: If ``TENANT_ID`` is set to something that is not a plain identifier.
    """
    tenant = _setting("TENANT_ID")
    if tenant and not _TENANT_ID.match(tenant):
        raise RuntimeError("TENANT_ID must be 1-63 letters, digits, '.', '_' or '-', starting with a letter or digit")
    return tenant


def database_schema() -> str:
    """Return the PostgreSQL schema holding this deployment's data, or ``""`` for the default.

    ``DATABASE_SCHEMA`` if set; otherwise ``tenant_<id>`` (lower-cased, with ``.`` and ``-`` as
    ``_``) when a tenant is configured.
    """
    schema = _setting("DATABASE_SCHEMA")
    if schema:
        return schema
    tenant = tenant_id()
    return "tenant_" + re.sub(r"[.-]", "_", tenant.lower()) if tenant else ""


def database_target(sqlite_path: str, *, database_url: str | None = None, schema: str | None = None) -> str:
    """Return where a store keeps its data: the configured PostgreSQL database, or its own SQLite file.

    :param sqlite_path: The store's SQLite file, used when no PostgreSQL database is configured.
    :param database_url: Overrides ``DATABASE_URL``.
    :param schema: Overrides :func:`database_schema`.
    """
    url = database_url or _setting("DATABASE_URL")
    if not url:
        return sqlite_path
    return with_schema(url, database_schema() if schema is None else schema)
