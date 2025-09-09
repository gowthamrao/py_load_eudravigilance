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

        # A.1.1: Sender Identifier
        sender_id = _find_text(elem, ".//hl7:messagesenderidentifier")

        # A.1.2: Receiver Identifier
        receiver_id = _find_text(elem, ".//hl7:messagereceiveridentifier")

        # C.1.1: Safety Report Unique Identifier
        safety_report_id = _find_text(report_elem, ".//hl7:safetyreportid")

        # C.1.5: Date of Most Recent Information (using receiptdate as a proxy)
        receipt_date = _find_text(report_elem, ".//hl7:receiptdate")

        # C.1.11: Nullification
        # The E2B(R3) guide specifies this field. We'll look for 'reportnullification'
        # and treat a value of 'true' as a nullification instruction.
        nullification_text = _find_text(report_elem, ".//hl7:reportnullification")
        is_nullified = nullification_text and nullification_text.lower() == "true"

        # C.2.r: Primary Source(s) - we take the first for the master table
        primary_source_elem = report_elem.find("hl7:primarysource", namespaces=NAMESPACES)
        reporter_country = None
        qualification = None

        if primary_source_elem is not None:
            reporter_country = _find_text(primary_source_elem, "hl7:reportercountry")
            qualification = _find_text(primary_source_elem, "hl7:qualification")

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

        # E.i: Reaction(s) / Event(s)
        reactions_list = []
        for reaction_elem in report_elem.findall("hl7:reaction", namespaces=NAMESPACES):
            reactions_list.append(
                {
                    "primarysourcereaction": _find_text(
                        reaction_elem, "hl7:primarysourcereaction"
                    ),
                    "reactionmeddrapt": _find_text(reaction_elem, "hl7:reactionmeddrapt"),
                }
            )

        # G.k: Drug(s)
        drugs_list = []
        for drug_elem in report_elem.findall("hl7:drug", namespaces=NAMESPACES):
            # G.k.2.2: Active Substance Name
            substances_list = []
            for substance_elem in drug_elem.findall("hl7:activesubstance", namespaces=NAMESPACES):
                substances_list.append({
                    "activesubstancename": _find_text(substance_elem, "hl7:activesubstancename")
                })

            drugs_list.append(
                {
                    "drugcharacterization": _find_text(
                        drug_elem, "hl7:drugcharacterization"
                    ),
                    "medicinalproduct": _find_text(drug_elem, "hl7:medicinalproduct"),
                    "drugstructuredosagenumb": _find_text(
                        drug_elem, "hl7:drugstructuredosagenumb"
                    ),
                    "drugstructuredosageunit": _find_text(
                        drug_elem, "hl7:drugstructuredosageunit"
                    ),
                    "drugdosagetext": _find_text(drug_elem, "hl7:drugdosagetext"),
                    "substances": substances_list,
                }
            )

        # F.r: Results of Tests and Procedures
        tests_list = []
        for test_elem in report_elem.findall("hl7:test", namespaces=NAMESPACES):
            tests_list.append(
                {
                    "testdate": _find_text(test_elem, "hl7:testdate"),
                    "testname": _find_text(test_elem, "hl7:testname"),
                    "testresult": _find_text(test_elem, "hl7:testresult"),
                    "testresultunit": _find_text(test_elem, "hl7:testresultunit"),
                    "testcomments": _find_text(test_elem, "hl7:testcomments"),
                }
            )

        # H.1: Case Narrative
        narrative = _find_text(report_elem, "hl7:narrativeincludeclinical")

        # Yield a dictionary representing the parsed ICSR with nested lists.
        if safety_report_id:
            yield {
                # A Section
                "senderidentifier": sender_id,
                "receiveridentifier": receiver_id,
                # C Section
                "safetyreportid": safety_report_id,
                "receiptdate": receipt_date,
                "is_nullified": is_nullified,
                "reportercountry": reporter_country,
                "qualification": qualification,
                # D Section
                "patientinitials": patient_initials,
                "patientonsetage": patient_age,
                "patientsex": patient_sex,
                # E, G, F, H Sections
                "reactions": reactions_list,
                "drugs": drugs_list,
                "tests": tests_list,
                "narrative": narrative,
            }

        # Crucial for memory efficiency: clear the element from memory.
        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]

    del context


def _element_to_dict(elem) -> Dict[str, Any]:
    """
    Recursively converts an lxml Element to a dictionary.
    Strips the namespace from the tag name for cleaner JSON.
    """
    # Ignore comments and other non-element nodes
    if not isinstance(elem.tag, str):
        return {}

    tag = etree.QName(elem.tag).localname

    # Base case: if the element has no children, return its text
    if not list(elem):
        return {tag: elem.text}

    # Recursive step: process children
    children = {}
    for child in elem:
        child_dict = _element_to_dict(child)
        if not child_dict:  # Skip empty dicts from comments
            continue

        child_tag = next(iter(child_dict)) # Get the single key from the returned dict

        # If the tag already exists, turn it into a list
        if child_tag in children:
            if not isinstance(children[child_tag], list):
                children[child_tag] = [children[child_tag]]
            children[child_tag].append(child_dict[child_tag])
        else:
            children[child_tag] = child_dict[child_tag]

    return {tag: children}


def parse_icsr_xml_for_audit(
    xml_source: IO[bytes],
) -> Generator[Dict[str, Any], None, None]:
    """
    Parses an ICH E2B(R3) XML file and yields individual ICSRs as
    a complete dictionary, suitable for JSON serialization.

    This is used for the 'Full Representation' (Audit) schema load.
    """
    context = etree.iterparse(
        xml_source, events=("end",), tag=f"{{{NAMESPACES['hl7']}}}ichicsrMessage"
    )

    for _, elem in context:
        # Convert the entire element to a dictionary
        icsr_dict = _element_to_dict(elem)
        yield icsr_dict

        # Memory cleanup
        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]

    del context
