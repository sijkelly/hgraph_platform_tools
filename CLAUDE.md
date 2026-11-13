# AI Helper

**Last Updated:** 2026-11-13
**Purpose:** Quick-start reference for initialising debugging/development sessions on the hgraph_platform_tools project.

---

## Project Overview

### What is hgraph_platform_tools?

Platform tooling built on top of the [hgraph](https://github.com/hhenson/hgraph) forward propagation graph framework.
Provides an order management API (``hgraph_api``), trade booking, entitlements management, static data
administration, and notification pipelines for commodity and financial trade processing.

The project consumes hgraph as a dependency and follows its coding conventions.

### Key Principles

1. Follow the coding standards of the upstream **hgraph** core repository.
2. All configuration is centralised in ``secure_config.py`` and driven by environment variables (python-dotenv).
3. Tests use **pytest function-based style** (not class-based) with ``@pytest.mark.parametrize``.
4. All modules declare an ``__all__`` tuple to control the public API.
5. Code is formatted with **Black** (line-length 120, target Python 3.12).
6. Modules import each other by full package path (``from hgraph_trade.hgraph_trade_booker.x import y``),
   never as bare sibling imports.
7. Keep business rules in **pure functions** and hgraph nodes thin. Modules whose signatures use time-series
   types (``TS[...]``, ``TSD[...]``) need a mypy override in ``pyproject.toml``; keep plain Python out of them.
8. **An AI agent never changes an order.** Agents read and draft proposals (``hgraph_api/mcp_server.py``);
   a person confirms. Do not add agent tools that submit, amend, fill or cancel.
9. **Black, mypy and pytest must all pass** — CI (``.github/workflows/ci.yml``) runs them on every pull request.

---

## First Time Setup

```bash
# 1. Install uv (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# 2. Clone the hg_oap fork next to this repo (pyproject.toml installs it from ../hg_oap)
git clone -b simon/extensions https://github.com/sijkelly/hg_oap.git ../hg_oap

# 3. Create virtual environment with Python 3.12
uv venv --python 3.12 .venv

# 4. Install the project with all dependencies (including dev/test)
uv pip install -e ".[dev]"

# 5. Verify installation
uv run pytest tests/ -q
```

---

## Testing

The order core's stores (``hgraph_oms``) are written against ``hgraph_oms.db.Database`` with ``?`` placeholders.
The reference-data and entitlement stores open connections with ``hgraph_oms.db.connect``, never
``sqlite3.connect``, and keep their SQL to what both SQLite and PostgreSQL accept. Tests that take the ``oms_db`` fixture run on SQLite and, when ``TEST_POSTGRES_URL``
is set (CI sets it), on PostgreSQL as well.

Tests live in ``tests/`` and mirror the source directory structure:

| Source Location | Test Location |
|-----------------|---------------|
| ``hgraph_trade/hgraph_trade_booker/`` | ``tests/hgraph_trade/hgraph_trade_booker/`` |
| ``hgraph_trade/hgraph_trade_model/`` | ``tests/hgraph_trade/hgraph_trade_model/`` |
| ``hgraph_trade/hgraph_trade_mapping/`` | ``tests/hgraph_trade/hgraph_trade_mapping/`` |
| ``hgraph_entitlements/`` | ``tests/hgraph_entitlements/`` |
| ``hgraph_api/`` | ``tests/hgraph_api/`` |
| ``hgraph_oms/`` | ``tests/hgraph_oms/`` |
| ``hgraph_oap_adapter/`` | ``tests/hgraph_oap_adapter/`` |
| ``hgraph_static_admin/`` | ``tests/hgraph_static_admin/`` |

```bash
# Run all tests
uv run pytest tests/ -q

# Run with coverage
uv run pytest tests/ --cov --cov-report=term-missing

# Run a single test file
uv run pytest tests/hgraph_trade/hgraph_trade_booker/test_pipeline_result.py -v
```

### Test Style Guidelines

Tests follow the pytest function-based style (not class-based):

```python
import pytest

def test_simple_case():
    assert some_function() == expected

@pytest.mark.parametrize("input_val,expected", [
    (1, 2),
    (2, 4),
])
def test_parameterized(input_val, expected):
    assert double(input_val) == expected
```

---

## Project Structure

```
hgraph_platform_tools/
├── cli.py                              # Unified CLI entry point (hgraph-tools)
├── secure_config.py                    # Centralised env-var config
├── pyproject.toml                      # PEP 621 project metadata (uv/hatch)
├── Dockerfile, compose.yaml            # Container image for the API (needs ../hg_oap as a build context)
├── CLAUDE.md                           # This file
│
├── hgraph_trade/                       # Core trade processing
│   ├── logging_config/                 # Centralised logging setup
│   ├── hgraph_trade_booker/            # Booking pipeline
│   │   ├── main.py                     # Pipeline entry point
│   │   ├── trade_loader.py             # Load & validate trades
│   │   ├── trade_mapper.py             # Map trades to FpML model
│   │   ├── trade_booker.py             # Write trades to output
│   │   ├── kafka_sender.py             # Send to Kafka with retry
│   │   ├── message_wrapper.py          # Add headers/footers/checksums
│   │   ├── decomposition.py            # Split complex trades
│   │   ├── pipeline_result.py          # Structured result objects
│   │   └── utils.py                    # File/directory helpers
│   ├── hgraph_trade_model/             # FpML-like trade builders
│   │   ├── swap.py, option.py, forward.py, future.py
│   │   ├── physical.py, swaption.py, fx.py, cash.py
│   │   ├── trade_header.py, trade_footer.py
│   │   └── __init__.py                 # Re-exports all creators
│   ├── hgraph_trade_mapping/           # Field & instrument mappings
│   └── fpml_xsd_reference_files/       # XSD parser and reference data
│
├── hgraph_api/                         # FastAPI OMS service
│   ├── app.py                          # Application factory
│   ├── routers/                        # orders, proposals, credit, parties, portfolios, entitlements, health
│   ├── services/                       # Order engine access, Claude order parser
│   ├── websockets/                     # Live order and credit feeds
│   ├── mcp_server.py                   # MCP tools for AI agents (read + propose only)
│   ├── client_auth.py                  # Bearer-token sign-in for API clients (MCP server)
│   ├── services/tokens.py              # OIDC access-token verification
│   ├── services/tenancy.py             # TENANT_ID: one deployment per customer, schema per tenant
│   └── middleware/auth.py              # Caller identity (X-User-Id or OIDC token) + entitlements check
├── hgraph_oms/                         # Order core (hgraph_api's order endpoints run on it)
│   ├── model.py                        # Immutable messages, snapshots, responses
│   ├── state_machine.py                # Lifecycle transitions (pure)
│   ├── pre_trade.py                    # Credit and entitlement checks (pure)
│   ├── order_book.py                   # hgraph compute_node: checks + state machine
│   ├── db.py                           # Database access: SQLite path or postgresql:// URL
│   ├── settings.py                     # TENANT_ID, DATABASE_URL -> where a store keeps its data
│   ├── event_log.py                    # Append-only event log and replay
│   ├── proposals.py                    # Orders drafted by an agent, awaiting a person's decision
│   ├── trades.py                       # Fills -> trade tickets -> booking pipeline input (pure)
│   ├── booking.py                      # TradeBooker: books tickets, records the outcome
│   ├── delivery.py                     # TradePublisher: sends booked trades to Kafka, with retry
│   ├── engine_graph.py                 # hgraph wiring for the real-time engine
│   └── engine.py                       # OrderEngine: runs the graph on a thread
├── hgraph_oap_adapter/                 # hg_oap domain objects <-> pipeline dicts and stores
├── hgraph_entitlements/                # Role-based access control
├── hgraph_notification/                # Email notifications
├── hgraph_static_admin/                # Party, portfolio and credit stores + Kafka subscribers
├── scripts/seed_demo_data.py           # Demo data for the API
├── scripts/eval_order_parser.py        # Order parser evaluation (cases in evals/order_parser/)
│
└── tests/                              # pytest suite (mirrors source layout)
    └── conftest.py                     # Shared fixtures
```

## Code Formatting

```bash
# Check formatting
uv run black --check .

# Auto-format
uv run black .
```

Black is configured in ``pyproject.toml``:
- Line length: 120
- Target version: Python 3.12

## Type Checking

```bash
uv run mypy .
```

## CLI

The project provides a unified CLI via ``hgraph-tools``:

```bash
# Trade booking
hgraph-tools book --input_file trade.json --output_dir output/

# Entitlements management
hgraph-tools entitlements update trader1 Trader
hgraph-tools entitlements query trader1
hgraph-tools entitlements check trader1 execute_trade

# Static data
hgraph-tools static-admin --init-db --db-path static_data.db

# Notifications
hgraph-tools notify --file trade.json

# Parse XSD schema
hgraph-tools parse-xsd --xsd path/to/fpml.xsd --output output.json

# OMS API for the hgraph_oms_ui front end
hgraph-tools serve --port 8000 --db-dir ./data/

# MCP server giving an AI agent read access and the ability to draft order proposals
hgraph-tools mcp --api-url http://localhost:8000 --user-id ai-agent
```

## Dependencies

Core dependencies are minimal. Optional groups can be installed separately:

```bash
uv pip install -e ".[messaging]"      # Kafka
uv pip install -e ".[notification]"   # Jinja2 email templates
uv pip install -e ".[database]"       # MongoDB
uv pip install -e ".[web]"            # HTTP requests
uv pip install -e ".[api]"            # FastAPI service and Claude order parsing
uv pip install -e ".[ai]"             # MCP server for AI agents
uv pip install -e ".[postgres]"       # PostgreSQL for the order core (DATABASE_URL)
uv pip install -e ".[oap]"            # hg_oap (installed from the sibling ../hg_oap clone)
uv pip install -e ".[test]"           # pytest, mypy, black, coverage
uv pip install -e ".[dev]"            # Everything
```
