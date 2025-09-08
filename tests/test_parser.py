import os
from pathlib import Path

from py_load_eudravigilance.parser import parse_icsr_xml

# Get the directory of the current test file to build a path to the sample data.
TEST_DIR = Path(__file__).parent


def test_parse_icsr_xml():
    """
    Tests the `parse_icsr_xml` function with a sample E2B(R3) file.

    It verifies that:
    1. The parser correctly identifies and processes multiple ICSR messages.
    2. The key fields ('safetyreportid' and 'receiptdate') are extracted
       accurately from each message.
    """
    # Construct the full path to the sample XML file.
    sample_file_path = TEST_DIR / "sample_e2b.xml"

    # The parser expects a file-like object opened in bytes mode.
    with open(sample_file_path, "rb") as f:
        # The parser returns a generator, so we convert it to a list to
        # inspect all the results.
        parsed_icsrs = list(parse_icsr_xml(f))

    # 1. Assert that both ICSR messages in the file were parsed.
    assert len(parsed_icsrs) == 2

    # 2. Assert the content of the first parsed ICSR is correct.
    assert parsed_icsrs[0]["safetyreportid"] == "TEST-CASE-001"
    assert parsed_icsrs[0]["receiptdate"] == "20240101"

    # 3. Assert the content of the second parsed ICSR is correct.
    assert parsed_icsrs[1]["safetyreportid"] == "TEST-CASE-002"
    assert parsed_icsrs[1]["receiptdate"] == "20240102"
