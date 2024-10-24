import xmlschema
from typing import Dict, Any
import json


def strip_namespace(tag: str) -> str:
    """
    Remove namespace from tag names.

    :param tag: Tag name with namespace
    :return: Tag name without namespace
    """
    return tag.split('}')[-1] if '}' in tag else tag


def extract_documentation(element) -> str:
    """
    Extract documentation from XSD elements if available.

    :param element: The XSD element object
    :return: Documentation text if available
    """
    doc_text = ""
    if element.annotation is not None:
        for doc in element.annotation.documentation:
            doc_text += doc.text.strip() if doc.text else ''
    return doc_text


def map_xsd_type_to_python(xsd_type: str) -> str:
    """
    Map XSD data types to Python data types.

    :param xsd_type: XSD type
    :return: Corresponding Python data type
    """
    type_mapping = {
        'xsd:decimal': 'float',
        'xsd:float': 'float',
        'xsd:double': 'float',
        'xsd:int': 'int',
        'xsd:integer': 'int',
        'xsd:long': 'int',
        'xsd:string': 'str',
        'xsd:boolean': 'bool',
        'xsd:date': 'date',
        'xsd:dateTime': 'datetime'
    }
    return type_mapping.get(xsd_type, 'unknownType')


def process_element(element) -> Dict[str, Any]:
    """
    Recursively process XSD elements and build the dictionary structure for FPML tags.

    :param element: The XSD element object
    :return: Dictionary representing the element and its children
    """
    tag_info = {
        'type': None,
        'description': extract_documentation(element),
        'python_type': 'unknownType',  # Default placeholder for input types
        'children': {}
    }

    # Handle simple types or atomic restrictions
    if element.type.is_simple() or isinstance(element.type, xmlschema.validators.XsdAtomicRestriction):
        tag_info['type'] = 'simpleType'
        tag_info['python_type'] = map_xsd_type_to_python(str(element.type.base_type))

    # Handle complex types
    elif element.type.is_complex():
        tag_info['type'] = 'complexType'
        # Check if content is available and is iterable
        if hasattr(element.type.content, 'iter_elements'):
            for child in element.type.content.iter_elements():
                tag_info['children'][strip_namespace(child.name)] = process_element(child)

    return tag_info


def parse_xsd_schema(xsd_file: str) -> Dict[str, dict]:
    """
    Parse an XSD file and convert it to a Python dictionary format for FPML tags.

    :param xsd_file: Path to the FPML XSD file
    :return: Python dictionary representation of FPML schema
    """
    schema = xmlschema.XMLSchema(xsd_file)
    elements = schema.elements

    fpml_tags = {}
    for elem_name, element in elements.items():
        fpml_tags[strip_namespace(elem_name)] = process_element(element)

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
xsd_file_path = "/Users/simonkelly/Documents/GitHub/hgraph_platform_tools/CommTradeModel/fpml-com-5-12.xsd"  # Correct path to the XSD
fpml_tags = parse_xsd_schema(xsd_file_path)

# Save the FPML tags to a Python file
output_file = "/Users/simonkelly/Documents/GitHub/hgraph_platform_tools/CommTradeModel/output_fpml_tags.py"
save_fpml_tags_to_file(fpml_tags, output_file)

# Print the parsed FPML tags (Optional)
for tag, details in fpml_tags.items():
    print(f"{tag}: {details}")