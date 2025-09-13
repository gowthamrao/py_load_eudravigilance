import pytest
from lxml import etree
from py_load_eudravigilance.parser import (
    _element_to_dict,
    parse_icsr_xml,
    parse_icsr_xml_for_audit,
    validate_xml_with_xsd,
    InvalidICSRError,
)
import io
from pathlib import Path

# Get the directory of the current test file to build a path to the sample data.
TEST_DIR = Path(__file__).parent

# A minimal, valid XSD for testing
MINIMAL_XSD = """<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
    <xs:element name="root">
        <xs:complexType>
            <xs:sequence>
                <xs:element name="required_child" type="xs:string" />
            </xs:sequence>
        </xs:complexType>
    </xs:element>
</xs:schema>
"""

@pytest.fixture
def minimal_xsd_path(tmp_path):
    """A pytest fixture to write the MINIMAL_XSD to a temporary file."""
    xsd_file = tmp_path / "minimal.xsd"
    xsd_file.write_text(MINIMAL_XSD)
    return str(xsd_file)


def test_validate_xml_with_syntax_error(minimal_xsd_path):
    """
    Tests that `validate_xml_with_xsd` correctly identifies XML with syntax
    errors (e.g., unclosed tags).
    """
    xml_content = b"<root><required_child>test</required_child></unclosed_root>"
    xml_file = io.BytesIO(xml_content)
    is_valid, errors = validate_xml_with_xsd(xml_file, minimal_xsd_path)
    assert not is_valid
    assert len(errors) == 1
    assert "no element found" in errors[0]


def test_validate_xml_with_schema_violation(minimal_xsd_path):
    """
    Tests that `validate_xml_with_xsd` identifies XML that is well-formed
    but does not conform to the XSD schema (e.g., missing a required element).
    """
    xml_content = b"<root></root>"  # Missing 'required_child'
    xml_file = io.BytesIO(xml_content)
    is_valid, errors = validate_xml_with_xsd(xml_file, minimal_xsd_path)
    assert not is_valid
    assert len(errors) == 1
    assert "Element 'root': Missing child element(s)" in errors[0]


def test_validate_xml_with_empty_file(minimal_xsd_path):
    """
    Tests that `validate_xml_with_xsd` handles an empty file gracefully.
    """
    xml_content = b""
    xml_file = io.BytesIO(xml_content)
    is_valid, errors = validate_xml_with_xsd(xml_file, minimal_xsd_path)
    assert not is_valid
    assert len(errors) == 1
    assert "no element found" in errors[0]


def test_element_to_dict_with_attributes():
    """
    Tests that `_element_to_dict` ignores attributes, as is the current
    implementation's behavior.
    """
    xml_string = b'<root an_attr="123"><child>text</child></root>'
    root_element = etree.fromstring(xml_string)
    result = _element_to_dict(root_element)
    expected = {"root": {"child": "text"}}
    assert result == expected


def test_parse_with_iso_8859_1_encoding():
    """
    Tests that the parser can handle XML files with non-UTF-8 encodings
    when specified in the XML declaration.
    """
    # "Résumé" contains a non-UTF-8 character 'é'
    xml_content = """<?xml version="1.0" encoding="ISO-8859-1"?>
    <ichicsrMessage xmlns="urn:hl7-org:v3">
        <safetyreport>
            <safetyreportid>TEST-ENCODING-001</safetyreportid>
            <narrativeincludeclinical>Résumé</narrativeincludeclinical>
        </safetyreport>
    </ichicsrMessage>
    """.encode("ISO-8859-1")

    xml_file = io.BytesIO(xml_content)
    results = list(parse_icsr_xml(xml_file))

    assert len(results) == 1
    assert isinstance(results[0], dict)
    assert results[0]["safetyreportid"] == "TEST-ENCODING-001"
    assert results[0]["narrative"] == "Résumé"


def test_parse_icsr_xml_for_audit_success():
    """
    Tests the successful parsing of a valid message by `parse_icsr_xml_for_audit`.
    """
    xml_content = b"""
    <ichicsrBatch xmlns="urn:hl7-org:v3">
        <ichicsrMessage>
            <safetyreport>
                <safetyreportid>TEST-AUDIT-001</safetyreportid>
                <primarysource>
                    <reportergivename>John</reportergivename>
                </primarysource>
            </safetyreport>
        </ichicsrMessage>
    </ichicsrBatch>
    """
    xml_file = io.BytesIO(xml_content)
    results = list(parse_icsr_xml_for_audit(xml_file))

    assert len(results) == 1
    expected_dict = {
        "ichicsrMessage": {
            "safetyreport": {
                "safetyreportid": "TEST-AUDIT-001",
                "primarysource": {"reportergivename": "John"},
            }
        }
    }
    assert results[0] == expected_dict


def test_parse_icsr_xml_for_audit_with_syntax_error():
    """
    Tests that `parse_icsr_xml_for_audit` raises an InvalidICSRError for
    XML with a syntax error, as it uses a non-recovering parser.
    """
    xml_content = b"<root><unclosed></root>"
    xml_file = io.BytesIO(xml_content)
    with pytest.raises(InvalidICSRError) as excinfo:
        list(parse_icsr_xml_for_audit(xml_file))

    assert "Fatal XML syntax error for audit" in str(excinfo.value)


def test_element_to_dict_with_mixed_content():
    """
    Tests `_element_to_dict` with mixed content (text and child elements at
    the same level). The current implementation is expected to only capture
    the child elements and ignore the text outside of them.
    """
    xml_string = b"<root>Some text <child>Child text</child> more text</root>"
    root_element = etree.fromstring(xml_string)
    result = _element_to_dict(root_element)
    expected = {"root": {"child": "Child text"}}
    assert result == expected


def test_element_to_dict_with_multiple_namespaces():
    """
    Tests that `_element_to_dict` correctly strips different namespaces.
    """
    xml_string = b"""
    <ns1:root xmlns:ns1="http://example.com/ns1" xmlns:ns2="http://example.com/ns2">
        <ns1:child>value1</ns1:child>
        <ns2:child>value2</ns2:child>
    </ns1:root>
    """
    root_element = etree.fromstring(xml_string)
    result = _element_to_dict(root_element)
    expected = {"root": {"child": ["value1", "value2"]}}
    assert result == expected


def test_element_to_dict_with_no_namespace():
    """
    Tests `_element_to_dict` with an XML that has no namespace defined.
    """
    xml_string = b"<root><child>text</child></root>"
    root_element = etree.fromstring(xml_string)
    result = _element_to_dict(root_element)
    expected = {"root": {"child": "text"}}
    assert result == expected


def test_parse_with_repeated_singleton_element():
    """
    Tests that the parser correctly handles a case where an element that is
    expected to be a singleton (like <patient>) appears multiple times.
    The parser should only process the first instance.
    """
    xml_path = TEST_DIR / "data" / "repeated_singleton_element.xml"
    with open(xml_path, "rb") as f:
        xml_content = f.read()

    results = list(parse_icsr_xml(io.BytesIO(xml_content)))

    assert len(results) == 1
    assert isinstance(results[0], dict)

    # The parser should have extracted data from the *first* patient element
    assert results[0]["patientinitials"] == "FIRST"
    assert results[0]["patientsex"] == "1"
