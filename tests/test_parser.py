import os
from pathlib import Path

from py_load_eudravigilance.parser import (
    parse_icsr_xml,
    InvalidICSRError,
)

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
