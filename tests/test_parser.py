from pathlib import Path

from py_load_eudravigilance.parser import InvalidICSRError, parse_icsr_xml

# Get the directory of the current test file to build a path to the sample data.
TEST_DIR = Path(__file__).parent


def test_parse_icsr_xml_with_nested_data():
    """
    Tests the `parse_icsr_xml` function with the enhanced sample E2B(R3) file.

    It verifies that:
    1. The parser correctly processes all ICSR messages.
    2. It extracts top-level fields correctly.
    3. It correctly parses and nests one-to-many elements (reactions, drugs).
    4. It handles cases with and without nested elements gracefully.
    """
    sample_file_path = TEST_DIR / "sample_e2b.xml"
    with open(sample_file_path, "rb") as f:
        parsed_icsrs = list(parse_icsr_xml(f))

    # 1. Assert that all five ICSR messages in the file were parsed.
    assert len(parsed_icsrs) == 5

    # 2. Assert the content of the first ICSR (with nested data) is correct.
    case1 = parsed_icsrs[0]
    assert case1["senderidentifier"] == "TESTSENDER"
    assert case1["receiveridentifier"] == "TESTRECEIVER"
    assert case1["safetyreportid"] == "TEST-CASE-001"
    assert case1["receiptdate"] == "20240101"
    # The sample file uses <primarysourcecountry> but not <reportercountry>
    # This tests that the parser correctly returns None if the specific sub-field isn't found.
    assert case1["reportercountry"] is None
    assert case1["qualification"] is None
    assert case1["patientinitials"] == "FN"
    assert case1["patientonsetage"] == "55"
    assert case1["patientsex"] == "1"

    # Assert reactions (one-to-many)
    assert "reactions" in case1
    assert len(case1["reactions"]) == 2
    assert case1["reactions"][0]["primarysourcereaction"] == "Nausea"
    assert case1["reactions"][0]["reactionmeddrapt"] == "Nausea"
    assert case1["reactions"][1]["reactionmeddrapt"] == "Headache"

    # Assert drugs (one-to-many) and nested substances
    assert "drugs" in case1
    assert len(case1["drugs"]) == 2
    drug1 = case1["drugs"][0]
    drug2 = case1["drugs"][1]

    assert drug1["medicinalproduct"] == "DrugA"
    assert drug1["drugcharacterization"] == "1"
    assert "substances" in drug1
    assert len(drug1["substances"]) == 1
    assert drug1["substances"][0]["activesubstancename"] == "SubstanceX"

    assert drug2["medicinalproduct"] == "DrugB"
    assert drug2["drugcharacterization"] == "2"
    assert "substances" in drug2
    assert len(drug2["substances"]) == 2
    assert drug2["substances"][0]["activesubstancename"] == "SubstanceY"
    assert drug2["substances"][1]["activesubstancename"] == "SubstanceZ"

    # 3. Assert the content of the second ICSR (no nested data) is correct.
    case2 = parsed_icsrs[1]
    assert case2["safetyreportid"] == "TEST-CASE-002"
    assert "reactions" in case2
    assert len(case2["reactions"]) == 0  # Should be an empty list
    assert "drugs" in case2
    assert len(case2["drugs"]) == 0  # Should be an empty list

    # 4. Assert the content of the third ICSR (no patient or nested data) is correct.
    case3 = parsed_icsrs[2]
    assert case3["safetyreportid"] == "TEST-CASE-003"
    assert case3["patientinitials"] is None
    assert "reactions" in case3
    assert len(case3["reactions"]) == 0
    assert "drugs" in case3
    assert len(case3["drugs"]) == 0


def test_parse_mixed_validity_xml():
    """
    Tests that a file with both valid and invalid ICSRs is handled correctly.
    The parser should yield dicts for valid records and InvalidICSRError
    objects for invalid ones.
    """
    xml_path = TEST_DIR / "mixed_validity_e2b.xml"
    with open(xml_path, "rb") as f:
        results = list(parse_icsr_xml(f))

    # There are 3 messages in the file
    assert len(results) == 3

    valid_reports = [r for r in results if isinstance(r, dict)]
    errors = [r for r in results if isinstance(r, InvalidICSRError)]

    assert len(valid_reports) == 2
    assert len(errors) == 1

    # Check the error
    error = errors[0]
    assert "Missing required field: safetyreportid" in error.message
    assert error.partial_data["senderidentifier"] == "SENDER2"

    # Check the valid reports
    valid_ids = {r["safetyreportid"] for r in valid_reports}
    assert valid_ids == {"TEST-VALID-001", "TEST-VALID-003"}


from lxml import etree
from py_load_eudravigilance.parser import _element_to_dict


def test_element_to_dict_conversion():
    """
    Tests the _element_to_dict helper function directly.

    It verifies that:
    1. XML namespaces are correctly stripped from tags.
    2. Simple text content is captured.
    3. Nested elements are converted to nested dictionaries.
    4. Repeated elements at the same level are aggregated into a list.
    """
    xml_string = """
    <root xmlns="http://example.com/ns">
        <simple>value1</simple>
        <nested>
            <item>A</item>
            <item>B</item>
        </nested>
        <repeated>
            <subitem>S1</subitem>
        </repeated>
        <repeated>
            <subitem>S2</subitem>
            <anothersub>S3</anothersub>
        </repeated>
        <empty></empty>
    </root>
    """
    # Parse the string into an lxml element
    root_element = etree.fromstring(xml_string.encode("utf-8"))

    # Call the function to be tested
    result_dict = _element_to_dict(root_element)

    # Define the expected dictionary structure
    expected_dict = {
        "root": {
            "simple": "value1",
            "nested": {"item": ["A", "B"]},
            "repeated": [
                {"subitem": "S1"},
                {"subitem": "S2", "anothersub": "S3"},
            ],
            "empty": None,
        }
    }

    # Assert that the result matches the expected structure
    assert result_dict == expected_dict


from unittest.mock import patch

from py_load_eudravigilance.parser import validate_xml_with_xsd


def test_validate_xml_uses_streaming_parser(tmp_path):
    """
    Tests that `validate_xml_with_xsd` uses a streaming approach.

    It verifies this by:
    1. Asserting that `etree.parse` is only called for the small XSD schema,
       not the main (potentially large) XML data file.
    2. Asserting that the `parser.feed()` method of a streaming parser IS called,
       proving the streaming code path is executed.
    """
    # Create dummy XML and XSD files
    xml_file = tmp_path / "test.xml"
    xml_file.write_text("<root/>")
    xsd_file = tmp_path / "test.xsd"
    xsd_file.write_text(
        """<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
           <xs:element name="root"/>
           </xs:schema>"""
    )

    # We patch `etree.parse` and `etree.XMLParser` itself.
    with patch("py_load_eudravigilance.parser.etree.parse") as mock_etree_parse, patch(
        "py_load_eudravigilance.parser.etree.XMLParser"
    ) as mock_xml_parser:
        # The XMLParser class will now return a mock instance.
        mock_parser_instance = mock_xml_parser.return_value
        # Make the parse mock return a valid, minimal ElementTree object
        # so that etree.XMLSchema() doesn't fail.
        minimal_xsd_doc = etree.fromstring(
            """<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
               <xs:element name="root"/>
               </xs:schema>"""
        )
        mock_etree_parse.return_value = etree.ElementTree(minimal_xsd_doc)

        with open(xml_file, "rb") as f:
            validate_xml_with_xsd(f, str(xsd_file))

        # 1. Assert that `etree.parse` was called for the XSD file.
        mock_etree_parse.assert_called_once_with(str(xsd_file))

        # 2. Assert that an XMLParser was instantiated with our schema.
        mock_xml_parser.assert_called_once()

        # 3. Assert that the `feed` method was called on the parser instance.
        mock_parser_instance.feed.assert_called()

        # 4. Assert that the `close` method was called to finalize parsing.
        mock_parser_instance.close.assert_called_once()
