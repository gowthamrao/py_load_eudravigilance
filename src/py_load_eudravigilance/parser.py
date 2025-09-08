"""
Handles the Extraction phase of the ETL process.

This module is responsible for finding, parsing, and validating
EudraVigilance ICSR XML files from various sources.
"""
from typing import Any, Dict, Generator, IO

from lxml import etree

# Define the primary XML namespace for ICH E2B(R3) based on HL7 v3.
# This is crucial for lxml to find elements correctly.
NAMESPACES = {"hl7": "urn:hl7-org:v3"}


def parse_icsr_xml(xml_source: IO[bytes]) -> Generator[Dict[str, Any], None, None]:
    """
    Parses an ICH E2B(R3) XML file and yields individual ICSRs.

    This function uses an iterative parsing approach (iterparse) to handle
    very large XML files with minimal memory usage, as required by the FRD.
    It identifies and processes each 'ichicsrMessage' element, which represents
    a single Individual Case Safety Report.

    Args:
        xml_source: A file-like object opened in bytes mode containing the XML data.

    Yields:
        A dictionary for each parsed ICSR, containing a subset of key fields.
    """
    # Use iterparse to process the XML iteratively. We listen for the 'end'
    # event on the 'ichicsrMessage' tag.
    context = etree.iterparse(
        xml_source, events=("end",), tag=f"{{{NAMESPACES['hl7']}}}ichicsrMessage"
    )

    for _, elem in context:
        # Helper function to safely find an element and return its text content.
        def _find_text(start_element, xpath, default=None):
            found_elem = start_element.find(xpath, namespaces=NAMESPACES)
            return found_elem.text if found_elem is not None else default

        # All data is within the <safetyreport> tag.
        report_elem = elem.find("hl7:safetyreport", namespaces=NAMESPACES)
        if report_elem is None:
            # If there's no safetyreport, skip this ichicsrMessage
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]
            continue

        # C.1.1: Safety Report Unique Identifier
        safety_report_id = _find_text(report_elem, ".//hl7:safetyreportid")

        # C.1.5: Date of Most Recent Information (using receiptdate as a proxy)
        receipt_date = _find_text(report_elem, ".//hl7:receiptdate")

        # D: Patient Characteristics
        patient_elem = report_elem.find("hl7:patient", namespaces=NAMESPACES)
        patient_initials = None
        patient_age = None
        patient_sex = None

        if patient_elem is not None:
            # D.1: Patient Initials
            patient_initials = _find_text(patient_elem, "hl7:patientinitials")

            # D.2.2: Age (using patientonsetage as the field)
            patient_age = _find_text(patient_elem, "hl7:patientonsetage")

            # D.5: Sex
            patient_sex = _find_text(patient_elem, "hl7:patientsex")

        # Yield a dictionary representing the parsed ICSR.
        if safety_report_id:
            yield {
                "safetyreportid": safety_report_id,
                "receiptdate": receipt_date,
                "patientinitials": patient_initials,
                "patientonsetage": patient_age,
                "patientsex": patient_sex,
            }

        # Crucial for memory efficiency: clear the element from memory.
        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]

    del context
