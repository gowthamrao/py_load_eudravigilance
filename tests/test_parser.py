import os
from pathlib import Path

from py_load_eudravigilance.parser import parse_icsr_xml

# Get the directory of the current test file to build a path to the sample data.
TEST_DIR = Path(__file__).parent


def test_parse_icsr_xml_with_patient_data():
    """
    Tests the `parse_icsr_xml` function with an updated sample E2B(R3) file.

    It verifies that:
    1. The parser correctly processes multiple ICSR messages.
    2. It extracts basic fields ('safetyreportid', 'receiptdate').
    3. It correctly extracts patient data ('patientinitials', 'patientonsetage', 'patientsex').
    4. It correctly handles cases where the patient block is missing, yielding None for those fields.
    """
    # Construct the full path to the sample XML file.
    sample_file_path = TEST_DIR / "sample_e2b.xml"

    # The parser expects a file-like object opened in bytes mode.
    with open(sample_file_path, "rb") as f:
        # The parser returns a generator, so we convert it to a list to inspect all results.
        parsed_icsrs = list(parse_icsr_xml(f))

    # 1. Assert that all three ICSR messages in the file were parsed.
    assert len(parsed_icsrs) == 3

    # 2. Assert the content of the first parsed ICSR is correct.
    assert parsed_icsrs[0]["safetyreportid"] == "TEST-CASE-001"
    assert parsed_icsrs[0]["receiptdate"] == "20240101"
    assert parsed_icsrs[0]["patientinitials"] == "FN"
    assert parsed_icsrs[0]["patientonsetage"] == "55"
    assert parsed_icsrs[0]["patientsex"] == "1"

    # 3. Assert the content of the second parsed ICSR is correct.
    assert parsed_icsrs[1]["safetyreportid"] == "TEST-CASE-002"
    assert parsed_icsrs[1]["receiptdate"] == "20240102"
    assert parsed_icsrs[1]["patientinitials"] == "LW"
    assert parsed_icsrs[1]["patientonsetage"] == "78"
    assert parsed_icsrs[1]["patientsex"] == "2"

    # 4. Assert the content of the third ICSR (with no patient data) is correct.
    assert parsed_icsrs[2]["safetyreportid"] == "TEST-CASE-003"
    assert parsed_icsrs[2]["receiptdate"] == "20240103"
    assert parsed_icsrs[2]["patientinitials"] is None
    assert parsed_icsrs[2]["patientonsetage"] is None
    assert parsed_icsrs[2]["patientsex"] is None
