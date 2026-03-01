"""
hgraph_trade package initialization.

This is the core trade processing engine for the hgraph platform. It provides
a full pipeline for loading, validating, mapping, modelling, and booking
financial trades in FpML-compliant JSON format.

Sub-packages:
- hgraph_trade_model: FpML-like trade structure creation for all instrument types.
- hgraph_trade_mapping: Field and instrument mapping from hgraph to FpML format.
- hgraph_trade_booker: Trade validation, message wrapping, and booking pipeline.
- logging_config: Centralized logging configuration.
- fpml_xsd_reference_files: FpML XSD schema parsing and reference data.
"""
