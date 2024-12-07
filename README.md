# hgraph_platform_tools

This project provides tools for modeling and booking trades, focusing on commodities and financial instruments, using the `hgraph` framework. It converts incoming trade data into a structured, FpML-like JSON format for downstream systems.

## Features

- **Commodity Instruments:**  
  - Commodity Swap (supports `fixedFloat` and `floatFloat` sub-instruments)
  - Commodity Option
  - Commodity Forward
  - Commodity Future
  - Commodity Swaption
  - Commodity Physical (placeholder implementation, can be extended)
- **FX Trades**: Supports mapping FX trade data into a standardized format.
- **Trade Header and Footer**:  
  Creates standardized trade headers and footers for FpML-like messages.
- **Message Wrappers**:  
  Wraps the core trade data with headers, footers, and checksums.
- **Modular Structure**:  
  Each instrument and component (header, footer, message wrapper) is handled by its own module, improving clarity and maintainability.
- **Regex-Based Validation**:  
  Validates raw input files and certain fields (e.g., date formats) using regex before mapping to the model.

## Improvements Over Previous Versions

- **Centralized Logging**:  
  Instead of `print` statements and scattered logging configurations, the code is designed to use a centralized logging configuration (suggested to be done in `main.py` or a dedicated logging configuration module). All debugging and error messages now use `logging` at appropriate levels (`DEBUG`, `INFO`, `ERROR`), making it easier to diagnose issues.
  
- **Enhanced Documentation and Docstrings**:  
  Module-level docstrings and detailed function docstrings have been added throughout the codebase. These docstrings explain what each module and function does, improving understandability and ease of onboarding new developers.

- **Commented-Out Examples**:  
  Example usage blocks are now commented out rather than removed. This keeps the production code clean, while still providing developers with quick references or test snippets they can uncomment when needed.

- **Consistent Mappings and Data Flows**:  
  The code now uses consistent naming, clear mapping dictionaries, and structured error handling to ensure that instrument and sub-instrument types are validated and processed uniformly.

- **Separation of Concerns**:  
  The code is modularized into separate files, each focusing on a specific component or instrument type. Validation, mapping, trade model creation, and booking are clearly separated, making the code easier to maintain, extend, and test.

---

## Quick Start

To get started:

```bash
# Clone the repository
git clone https://github.com/your-repo/hgraph_platform_tools.git
cd hgraph_platform_tools

# Set up a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt