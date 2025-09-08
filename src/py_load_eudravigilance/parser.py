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
    # event on the 'ichicsrMessage' tag. This means the element is fully
    # available in memory, but we haven't read the rest of the file yet.
    # The tag must be namespace-qualified.
    context = etree.iterparse(
        xml_source, events=("end",), tag=f"{{{NAMESPACES['hl7']}}}ichicsrMessage"
    )

    for _, elem in context:
        # For each 'ichicsrMessage' element found, extract the required data fields.
        # We use .find() with the namespace map. The './/' prefix ensures we
        # find the element regardless of its depth within the current context.

        # C.1.1: Safety Report Unique Identifier
        safety_report_id_elem = elem.find(".//hl7:safetyreportid", namespaces=NAMESPACES)
        safety_report_id = (
            safety_report_id_elem.text if safety_report_id_elem is not None else None
        )

        # C.1.5: Date of Most Recent Information
        # This is a simplification; a real file might have multiple dates.
        # We'll look for 'receiptdate' as a proxy.
        receipt_date_elem = elem.find(".//hl7:receiptdate", namespaces=NAMESPACES)
        receipt_date = receipt_date_elem.text if receipt_date_elem is not None else None

        # Yield a dictionary representing the parsed ICSR.
        # We only yield if the primary identifier is present.
        if safety_report_id:
            yield {
                "safetyreportid": safety_report_id,
                "receiptdate": receipt_date,
            }

        # Crucial for memory efficiency: clear the element and its predecessors
        # from memory after processing. This prevents the entire XML tree
        # from being held in memory.
        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]

    # Clean up the context variable to free any remaining memory
    del context
