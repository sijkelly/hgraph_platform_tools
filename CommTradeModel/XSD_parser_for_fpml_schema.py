import xmlschema
from typing import Dict
import json


def parse_xsd_schema(xsd_file: str) -> Dict[str, dict]:
    """
    Parse an XSD file and convert it to a Python dictionary format for FPML tags.

    :param xsd_file: Path to the FPML XSD file
    :return: Python dictionary representation of FPML schema
    """
    schema = xmlschema.XMLSchema(xsd_file)
    elements = schema.elements

    # Recursive function to parse XSD elements into dictionary format
    def process_element(element):
        tag_info = {}
        if element.is_complex():
            tag_info['type'] = 'complexType'
            tag_info['children'] = {}
            for child in element.type.content_type:
                tag_info['children'][child.name] = process_element(child)
        else:
            tag_info['type'] = element.type.name or 'simpleType'
        return tag_info

    # Generate the FPML_TAGS dictionary from the XSD schema
    fpml_tags = {}
    for elem_name, element in elements.items():
        fpml_tags[elem_name] = process_element(element)

    return fpml_tags


def save_fpml_tags_to_file(fpml_tags: Dict[str, dict], output_file: str):
    """
    Save the generated FPML tags dictionary to a Python file.

    :param fpml_tags: The FPML tags dictionary
    :param output_file: Path to the output Python file
    """
    with open(output_file, 'w') as f:
        f.write('FPML_TAGS = ' + json.dumps(fpml_tags, indent=4))


# Example usage
xsd_file_path = "/Users/simonkelly/Documents/GitHub/hgraph_platform_tools/CommTradeModel/fpml-com-5-12.xsd"  # Path to root XSD
fpml_tags = parse_xsd_schema(xsd_file_path)

# Save the FPML tags to a Python file
output_file = "/Users/simonkelly/Documents/GitHub/hgraph_platform_tools/CommTradeModel/fpml_tags.py"
save_fpml_tags_to_file(fpml_tags, output_file)

# Print the parsed FPML tags (Optional)
for tag, details in fpml_tags.items():
    print(f"{tag}: {details}")
