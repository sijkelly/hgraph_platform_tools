# hgraph_platform_tools

This project provides tools for booking and modeling trades, focusing on commodities and financial instruments, using the `hgraph` framework.

## Features
- Commodity Swap (fixedFloat, floatFloat)
- Commodity Option
- Commodity Forward
- Commodity Future
- Commodity Swaption
- FX Trades
- Trade Header, Footer, and Message Wrappers
- Modular components for trade modeling and booking

---
 
## Quick Start

Run the following commands to test the project with the provided example data:

```bash
# Clone the repository
git clone https://github.com/your-repo/hgraph_platform_tools.git
cd hgraph_platform_tools

# Set up virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run the example trade
python hgraph_trade_booker/main.py

## Running the Trade Booking Script

To run the `main.py` script with dynamic input and output paths:

```bash
python main.py --input_file <path_to_input_trade_file> \
               --output_dir <path_to_output_directory> \
               --output_file <output_file_name>