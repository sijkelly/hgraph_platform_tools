#!/usr/bin/env python3
"""
fpml_XSD_parser.py

Parse FpML XSD schema files and generate a JSON dictionary of all elements,
their types, documentation, and children.

The generated JSON replaces the old 3-4 MB Python files (fpml_tags.py,
output_fpml_tags.py) that were committed to the repository. Those files
are now gitignored; run this script to regenerate them when the XSD
source files change.

Usage:
    python fpml_XSD_parser.py                         # parse default XSD, write JSON
    python fpml_XSD_parser.py --xsd path/to/file.xsd  # parse a specific XSD
    python fpml_XSD_parser.py --output custom.json     # custom output path
"""

import argparse
import json
import logging
import os
from typing import Any, Dict

import xmlschema

logger = logging.getLogger(__name__)

# Directory containing this script and the XSD reference files
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_XSD = os.path.join(_THIS_DIR, "fpml-com-5-12.xsd")
DEFAULT_OUTPUT = os.path.join(_THIS_DIR, "fpml_tags.json")


def strip_namespace(tag: str) -> str:
    """
    Remove the XML namespace prefix from a tag name.

    :param tag: Tag name, possibly with ``{namespace}`` prefix.
    :return: Tag name without namespace.
    """
    return tag.split("}")[-1] if "}" in tag else tag


def extract_documentation(element) -> str:
    """
    Extract documentation text from an XSD element's annotation, if present.

    :param element: An xmlschema element object.
    :return: Documentation string (empty string if none).
    """
    doc_text = ""
    if element.annotation is not None:
        for doc in element.annotation.documentation:
            doc_text += doc.text.strip() if doc.text else ""
    return doc_text


def map_xsd_type_to_python(xsd_type: str) -> str:
    """
    Map an XSD data type string to a Python type name.

    :param xsd_type: XSD type (e.g. ``xsd:decimal``).
    :return: Python type name string.
    """
    type_mapping = {
        "xsd:decimal": "float",
        "xsd:float": "float",
        "xsd:double": "float",
        "xsd:int": "int",
        "xsd:integer": "int",
        "xsd:long": "int",
        "xsd:string": "str",
        "xsd:boolean": "bool",
        "xsd:date": "date",
        "xsd:dateTime": "datetime",
    }
    return type_mapping.get(xsd_type, "unknownType")


def process_element(element) -> Dict[str, Any]:
    """
    Recursively process an XSD element and build a dictionary with its
    type, documentation, and children.

    :param element: An xmlschema element object.
    :return: Dict describing the element.
    """
    tag_info: Dict[str, Any] = {
        "type": None,
        "documentation": extract_documentation(element),
        "python_type": "unknownType",
        "children": {},
    }

    if element.type.is_simple() or isinstance(
        element.type, xmlschema.validators.XsdAtomicRestriction
    ):
        tag_info["type"] = "simpleType"
        base = str(element.type.base_type) if element.type.base_type else ""
        tag_info["python_type"] = map_xsd_type_to_python(base)

    elif element.type.is_complex():
        tag_info["type"] = "complexType"
        if hasattr(element.type.content, "iter_elements"):
            for child in element.type.content.iter_elements():
                tag_info["children"][strip_namespace(child.name)] = process_element(
                    child
                )

    return tag_info


def parse_xsd_schema(xsd_file: str) -> Dict[str, dict]:
    """
    Parse an XSD file and return a dictionary of all top-level elements.

    :param xsd_file: Path to the FpML XSD file.
    :return: Dict mapping element names to their parsed structure.
    """
    logger.info("Parsing XSD schema: %s", xsd_file)
    schema = xmlschema.XMLSchema(xsd_file)

    fpml_tags: Dict[str, dict] = {}
    for elem_name, element in schema.elements.items():
        fpml_tags[strip_namespace(elem_name)] = process_element(element)

    logger.info("Parsed %d top-level elements", len(fpml_tags))
    return fpml_tags


def save_fpml_tags(fpml_tags: Dict[str, dict], output_file: str) -> None:
    """
    Save the parsed FpML tags dictionary to a JSON file.

    :param fpml_tags: The parsed tags dictionary.
    :param output_file: Destination file path.
    """
    with open(output_file, "w", encoding="utf-8") as fh:
        json.dump(fpml_tags, fh, indent=2)
    size_mb = os.path.getsize(output_file) / (1024 * 1024)
    logger.info("Saved %d elements to %s (%.1f MB)", len(fpml_tags), output_file, size_mb)


def main() -> None:
    """CLI entry point for generating the FpML tags JSON."""
    parser = argparse.ArgumentParser(
        description="Parse FpML XSD schemas and generate a JSON tag dictionary."
    )
    parser.add_argument(
        "--xsd",
        type=str,
        default=DEFAULT_XSD,
        help=f"Path to the XSD file to parse (default: {DEFAULT_XSD})",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=DEFAULT_OUTPUT,
        help=f"Output JSON file path (default: {DEFAULT_OUTPUT})",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging.",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    fpml_tags = parse_xsd_schema(args.xsd)
    save_fpml_tags(fpml_tags, args.output)
    print(f"Done. {len(fpml_tags)} elements written to {args.output}")


if __name__ == "__main__":
    main()
