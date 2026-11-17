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

from hgraph_api.config import api_config
from hgraph_oms.settings import database_schema, tenant_id

__all__ = ("api_config", "tenant_id", "database_schema")
