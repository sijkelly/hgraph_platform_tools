# hgraph_platform_tools

Platform tooling built on top of the [hgraph](https://github.com/hhenson/hgraph) forward propagation graph framework.
Provides an order management API, trade booking, entitlements management, static data administration, and
notification pipelines for commodity and financial trade processing.

## Features

### Order Management API (`hgraph_api`)
- **REST + WebSocket** — FastAPI service for orders, credit, parties, portfolios and entitlements
- **Pre-trade credit check** — Orders are rejected when they would breach a counterparty credit limit
- **Natural-language order entry** — Free text or pasted chat parsed into order fields via the Claude API
- **UI** — Served to the [hgraph_oms_ui](https://github.com/sijkelly/hgraph_oms_ui) React front end

### hg_oap Adapters (`hgraph_oap_adapter`)
- **Domain adapters** — Convert `hg_oap` swaps, parties, order lifecycle events and regulatory
  reports into booking-pipeline and data-lake records
- **EV rates** — Kafka subscriber and SQLite store for sales expected-value rates

### Trade Booking Pipeline
- **Commodity instruments** — Swap (fixed/float and float/float), Option, Forward, Future, Swaption, Physical (gas, bullion)
- **FX and Cash trades** — Currency pair mapping, fee/margin payments
- **FpML-like output** — Trades are mapped to a structured JSON format modelled on FpML
- **Message wrapping** — Headers, footers, and checksums for downstream systems
- **Batch processing** — Process directories of trade files with structured result reporting
- **Error recovery** — Per-trade error isolation, dead-letter quarantine for failed trades
- **Kafka integration** — Send messages with configurable retry and exponential backoff

### Entitlements
- **Role-based access control** — Static role definitions with action-level permissions
- **Pipeline integration** — Optional pre-check before trade execution
- **Event-driven** — Forward propagation graph for permission change events

### Static Data Administration
- **API ingestion** — Fetch counterparty data from external APIs
- **Data cleansing** — Regex-based validation and field mapping
- **SQLite storage** — Local database for reference data

### Notifications
- **Jinja2 templates** — HTML email rendering for trade events
- **SMTP delivery** — Configurable mail server via environment variables

## Quick Start

```bash
# 1. Install uv (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# 2. Clone the repository
git clone https://github.com/sijkelly/hgraph_platform_tools.git
cd hgraph_platform_tools

# 3. Clone the hg_oap fork next to this repo (pyproject.toml installs it from ../hg_oap)
git clone -b simon/extensions https://github.com/sijkelly/hg_oap.git ../hg_oap

# 4. Create virtual environment with Python 3.12
uv venv --python 3.12 .venv

# 5. Install the project with all dependencies
uv pip install -e ".[dev]"

# 6. Verify installation
uv run pytest tests/ -q
```

## CLI

The project provides a unified CLI via `hgraph-tools`:

```bash
# Trade booking
hgraph-tools book --input_file trade.json --output_dir output/
hgraph-tools book --input_dir trades/ --output_dir output/ --fail-fast

# Entitlements management
hgraph-tools entitlements update trader1 Trader
hgraph-tools entitlements query trader1
hgraph-tools entitlements check trader1 execute_trade

# Static data
hgraph-tools static-admin --init-db --db-path static_data.db
hgraph-tools static-admin --fetch --api-url http://example.com/api

# Notifications
hgraph-tools notify --file trade.json

# Parse FpML XSD schema
hgraph-tools parse-xsd --xsd path/to/fpml.xsd --output output.json

# OMS API (REST + WebSocket) for the hgraph_oms_ui front end
hgraph-tools serve --port 8000 --db-dir ./data/

# Kafka subscribers for static data
hgraph-tools party-subscribe --db-path party_data.db
hgraph-tools portfolio-subscribe --db-path portfolio_data.db
hgraph-tools credit-subscribe --db-path credit_data.db
```

## Project Structure

```
hgraph_platform_tools/
├── cli.py                              # Unified CLI entry point
├── secure_config.py                    # Centralised env-var configuration
├── pyproject.toml                      # PEP 621 project metadata (uv/hatch)
│
├── hgraph_trade/                       # Core trade processing
│   ├── logging_config/                 # Centralised logging setup
│   ├── hgraph_trade_booker/            # Booking pipeline
│   ├── hgraph_trade_model/             # FpML-like trade model builders
│   ├── hgraph_trade_mapping/           # Field & instrument mappings
│   └── fpml_xsd_reference_files/       # XSD parser and reference data
│
├── hgraph_api/                         # FastAPI OMS service (REST + WebSocket)
├── hgraph_oap_adapter/                 # Adapters between hg_oap domain objects and the pipeline
├── hgraph_entitlements/                # Role-based access control
├── hgraph_notification/                # Email notifications (Jinja2 + SMTP)
├── hgraph_static_admin/                # Static reference data (SQLite stores + Kafka subscribers)
├── scripts/                            # Demo data seeding
│
└── tests/                              # pytest suite (mirrors source layout)
```

## Development

### Dependencies

Core dependencies are minimal. Optional groups can be installed separately:

```bash
uv pip install -e ".[messaging]"      # Kafka
uv pip install -e ".[notification]"   # Jinja2 email templates
uv pip install -e ".[database]"       # MongoDB
uv pip install -e ".[web]"            # HTTP requests
uv pip install -e ".[api]"            # FastAPI service and Claude order parsing
uv pip install -e ".[oap]"            # hg_oap domain library (from ../hg_oap)
uv pip install -e ".[test]"           # pytest, mypy, black, coverage
uv pip install -e ".[dev]"            # Everything
```

### Testing

Tests use pytest with function-based style and `@pytest.mark.parametrize`:

```bash
# Run all tests
uv run pytest tests/ -q

# Run with coverage
uv run pytest tests/ --cov --cov-report=term-missing

# Run a single test file
uv run pytest tests/hgraph_trade/hgraph_trade_booker/test_pipeline_result.py -v
```

### Code Quality

```bash
# Format with Black (120-char line length)
uv run black .

# Type checking
uv run mypy .
```

The same three checks (Black, mypy, pytest) run in GitHub Actions on every pull request
(`.github/workflows/ci.yml`).

### Configuration

All configurable values are loaded from environment variables via `secure_config.py`.
For local development, create a `.env` file in the project root:

```env
SMTP_HOST=smtp.example.com
SMTP_PORT=587
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
MESSAGE_SENDER_ID=hgraph_platform
MESSAGE_TARGET_ID=booking_system
```

## Conventions

This project follows the coding standards of the upstream [hgraph](https://github.com/hhenson/hgraph) core repository:

- **Python 3.12** — aligned with hgraph core
- **uv** for dependency management
- **Black** formatter (line-length 120, target py312)
- **`__all__` tuples** in every module to control the public API
- **reStructuredText docstrings** (`:param:`, `:returns:`, `:raises:`)
- **pytest function-based tests** (not class-based) with `@pytest.mark.parametrize`
- **Test directory mirrors source** layout

See [CLAUDE.md](CLAUDE.md) for a quick-start AI/developer reference.
