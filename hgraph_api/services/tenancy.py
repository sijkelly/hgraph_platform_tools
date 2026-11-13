"""
Tenancy: one deployment serves one customer.

Each customer (tenant) gets its own API process, and with it its own order
engine, order log, reference data and entitlements.  ``TENANT_ID`` names the
tenant a deployment serves.  From it follow the two things that keep tenants
apart when they share infrastructure:

* its PostgreSQL schema (:func:`database_schema`), so tenants can share one
  database server, and
* the organisation its users' tokens must belong to (``OIDC_TENANT_CLAIM``,
  checked in :mod:`hgraph_api.services.tokens`), so tenants can share one
  identity provider.

With ``TENANT_ID`` unset the deployment is single-tenant and neither applies.
"""

import os
import re

from hgraph_api.config import api_config

__all__ = ("tenant_id", "database_schema")

_TENANT_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,62}$")


def _setting(name: str) -> str:
    return (os.getenv(name) or api_config.get(name) or "").strip()


def tenant_id() -> str:
    """Return the tenant this deployment serves, or ``""`` if it is single-tenant.

    :raises RuntimeError: If ``TENANT_ID`` is set to something that is not a plain identifier.
    """
    tenant = _setting("TENANT_ID")
    if tenant and not _TENANT_ID.match(tenant):
        raise RuntimeError("TENANT_ID must be 1-63 letters, digits, '.', '_' or '-', starting with a letter or digit")
    return tenant


def database_schema() -> str:
    """Return the PostgreSQL schema holding this deployment's order data, or ``""`` for the default.

    ``DATABASE_SCHEMA`` if set; otherwise ``tenant_<id>`` (lower-cased, with ``.`` and ``-`` as
    ``_``) when a tenant is configured.
    """
    schema = _setting("DATABASE_SCHEMA")
    if schema:
        return schema
    tenant = tenant_id()
    return "tenant_" + re.sub(r"[.-]", "_", tenant.lower()) if tenant else ""
