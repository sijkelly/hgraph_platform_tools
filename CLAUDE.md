# AI Helper

**Last Updated:** 2026-03-01
**Purpose:** Quick-start reference for initialising debugging/development sessions on the hgraph_platform_tools project.

---

## Project Overview

### What is hgraph_platform_tools?

Platform tooling built on top of the [hgraph](https://github.com/hhenson/hgraph) forward propagation graph framework.
Provides trade booking, entitlements management, static data administration, and notification pipelines for
commodity and financial trade processing.

The project consumes hgraph as a dependency and follows its coding conventions.

### Key Principles

1. Follow the coding standards of the upstream **hgraph** core repository.
2. All configuration is centralised in ``secure_config.py`` and driven by environment variables (python-dotenv).
3. Tests use **pytest function-based style** (not class-based) with ``@pytest.mark.parametrize``.
4. All modules declare an ``__all__`` tuple to control the public API.
5. Code is formatted with **Black** (line-length 120, target Python 3.12).

---

## First Time Setup

```bash
# 1. Install uv (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# 2. Create virtual environment with Python 3.12
uv venv --python 3.12 .venv

# 3. Install the project with all dependencies (including dev/test)
uv pip install -e ".[dev]"

# 4. Verify installation
uv run pytest tests/ -q
```

---

## Testing

Tests live in ``tests/`` and mirror the source directory structure:

| Source Location | Test Location |
|-----------------|---------------|
| ``hgraph_trade/hgraph_trade_booker/`` | ``tests/hgraph_trade/hgraph_trade_booker/`` |
| ``hgraph_trade/hgraph_trade_model/`` | ``tests/hgraph_trade/hgraph_trade_model/`` |
| ``hgraph_trade/hgraph_trade_mapping/`` | ``tests/hgraph_trade/hgraph_trade_mapping/`` |
| ``hgraph_entitlements/`` | ``tests/hgraph_entitlements/`` |

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
├── hgraph_entitlements/                # Role-based access control
├── hgraph_notification/                # Email notifications
├── hgraph_static_admin/                # Static reference data
│
└── tests/                              # pytest suite (mirrors source layout)
    ├── conftest.py                     # Shared fixtures
    ├── hgraph_trade/
    │   ├── hgraph_trade_booker/
    │   ├── hgraph_trade_model/
    │   └── hgraph_trade_mapping/
    └── hgraph_entitlements/
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
```

## Dependencies

Core dependencies are minimal. Optional groups can be installed separately:

```bash
uv pip install -e ".[messaging]"      # Kafka
uv pip install -e ".[notification]"   # Jinja2 email templates
uv pip install -e ".[database]"       # MongoDB
uv pip install -e ".[web]"            # HTTP requests
uv pip install -e ".[test]"           # pytest, mypy, black, coverage
uv pip install -e ".[dev]"            # Everything
```
