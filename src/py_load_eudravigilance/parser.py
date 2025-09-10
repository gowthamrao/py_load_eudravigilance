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


class InvalidICSRError(Exception):
    """Custom exception for an ICSR that fails parsing validation."""

    def __init__(self, message, partial_data=None):
        self.message = message
        self.partial_data = partial_data or {}
        super().__init__(self.message)

    def __str__(self):
        return f"{self.message} (Partial Data: {self.partial_data})"


def parse_icsr_xml(
    xml_source: IO[bytes],
) -> Generator[Dict[str, Any] | InvalidICSRError, None, None]:
    """
    Parses an ICH E2B(R3) XML file and yields individual ICSRs.

    This function uses an iterative parsing approach (iterparse) to handle
    very large XML files with minimal memory usage, as required by the FRD.
    It identifies and processes each 'ichicsrMessage' element. If an ICSR
    is invalid (e.g., missing a required field), it yields an
    InvalidICSRError instead of the data dictionary.

    Args:
        xml_source: A file-like object opened in bytes mode containing the XML data.

    Yields:
        A dictionary for each successfully parsed ICSR, or an InvalidICSRError
        for each record that fails validation.
    """
    context = etree.iterparse(
        xml_source, events=("end",), tag=f"{{{NAMESPACES['hl7']}}}ichicsrMessage"
    )

    for _, elem in context:
        try:
            # Helper function to safely find an element and return its text content.
            def _find_text(start_element, xpath, default=None):
                found_elem = start_element.find(xpath, namespaces=NAMESPACES)
                return found_elem.text if found_elem is not None else default

            # All data is within the <safetyreport> tag.
            report_elem = elem.find("hl7:safetyreport", namespaces=NAMESPACES)
            if report_elem is None:
                raise InvalidICSRError("Missing required element: safetyreport")

            # C.1.1: Safety Report Unique Identifier is mandatory
            safety_report_id = _find_text(report_elem, ".//hl7:safetyreportid")
            if not safety_report_id:
                # Try to get some identifying info for the error message
                partial_data = {
                    "senderidentifier": _find_text(
                        elem, ".//hl7:messagesenderidentifier"
                    ),
                    "messagedate": _find_text(elem, ".//hl7:messagedate"),
                }
                raise InvalidICSRError(
                    "Missing required field: safetyreportid", partial_data=partial_data
                )

            # A.1.1: Sender Identifier
            sender_id = _find_text(elem, ".//hl7:messagesenderidentifier")
            # A.1.2: Receiver Identifier
            receiver_id = _find_text(elem, ".//hl7:messagereceiveridentifier")
            # C.1.4: Date of Receipt
            receipt_date = _find_text(report_elem, ".//hl7:receiptdate")
            # C.1.5: Date of Most Recent Information (the true version key)
            date_of_most_recent_info = _find_text(
                report_elem, ".//hl7:dateofmostrecentinformation"
            )
            # C.1.11: Nullification
            nullification_text = _find_text(report_elem, ".//hl7:reportnullification")
            is_nullified = nullification_text and nullification_text.lower() == "true"
            # C.2.r: Primary Source(s)
            primary_source_elem = report_elem.find(
                "hl7:primarysource", namespaces=NAMESPACES
            )
            reporter_country = None
            qualification = None
            if primary_source_elem is not None:
                reporter_country = _find_text(
                    primary_source_elem, "hl7:reportercountry"
                )
                qualification = _find_text(primary_source_elem, "hl7:qualification")

            # D: Patient Characteristics
            patient_elem = report_elem.find("hl7:patient", namespaces=NAMESPACES)
            patient_initials, patient_age, patient_sex = None, None, None
            if patient_elem is not None:
                patient_initials = _find_text(patient_elem, "hl7:patientinitials")
                patient_age = _find_text(patient_elem, "hl7:patientonsetage")
                patient_sex = _find_text(patient_elem, "hl7:patientsex")

            # E.i: Reaction(s) / Event(s)
            reactions_list = [
                {
                    "primarysourcereaction": _find_text(r, "hl7:primarysourcereaction"),
                    "reactionmeddrapt": _find_text(r, "hl7:reactionmeddrapt"),
                }
                for r in report_elem.findall("hl7:reaction", namespaces=NAMESPACES)
            ]

            # G.k: Drug(s)
            drugs_list = []
            for drug_elem in report_elem.findall("hl7:drug", namespaces=NAMESPACES):
                substances_list = [
                    {"activesubstancename": _find_text(s, "hl7:activesubstancename")}
                    for s in drug_elem.findall(
                        "hl7:activesubstance", namespaces=NAMESPACES
                    )
                ]
                drugs_list.append(
                    {
                        "drugcharacterization": _find_text(
                            drug_elem, "hl7:drugcharacterization"
                        ),
                        "medicinalproduct": _find_text(
                            drug_elem, "hl7:medicinalproduct"
                        ),
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
            tests_list = [
                {
                    "testdate": _find_text(t, "hl7:testdate"),
                    "testname": _find_text(t, "hl7:testname"),
                    "testresult": _find_text(t, "hl7:testresult"),
                    "testresultunit": _find_text(t, "hl7:testresultunit"),
                    "testcomments": _find_text(t, "hl7:testcomments"),
                }
                for t in report_elem.findall("hl7:test", namespaces=NAMESPACES)
            ]

            # H.1: Case Narrative
            narrative = _find_text(report_elem, "hl7:narrativeincludeclinical")

            yield {
                "senderidentifier": sender_id,
                "receiveridentifier": receiver_id,
                "safetyreportid": safety_report_id,
                "receiptdate": receipt_date,
                "date_of_most_recent_info": date_of_most_recent_info,
                "is_nullified": is_nullified,
                "reportercountry": reporter_country,
                "qualification": qualification,
                "patientinitials": patient_initials,
                "patientonsetage": patient_age,
                "patientsex": patient_sex,
                "reactions": reactions_list,
                "drugs": drugs_list,
                "tests": tests_list,
                "narrative": narrative,
            }

        except InvalidICSRError as e:
            yield e  # Yield the specific validation error
        except Exception as e:
            # Yield a generic error for any other unexpected issue
            yield InvalidICSRError(f"Unexpected parsing error: {e}")
        finally:
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


from typing import Tuple, List


def validate_xml_with_xsd(xml_source: IO[bytes], xsd_path: str) -> Tuple[bool, List[str]]:
    """
    Validates an XML source against a given XSD schema file using a streaming
    parser to ensure low memory usage.

    Args:
        xml_source: A file-like object containing the XML data.
        xsd_path: The file path to the XSD schema.

    Returns:
        A tuple containing:
        - A boolean indicating if the validation was successful.
        - A list of validation error messages, or an empty list if valid.
    """
    try:
        # Load the XSD schema. XSD files are assumed to be small and can be
        # parsed into memory directly.
        xmlschema_doc = etree.parse(xsd_path)
        xmlschema = etree.XMLSchema(xmlschema_doc)

        # Create a streaming parser with the schema attached.
        parser = etree.XMLParser(schema=xmlschema)

        # Feed the XML source to the parser in chunks.
        # This avoids loading the entire XML file into memory.
        # The parser will raise an error as soon as it finds an invalid chunk.
        for chunk in iter(lambda: xml_source.read(16384), b""):
            parser.feed(chunk)

        # Finalize the parsing. If the document is invalid, this will raise.
        parser.close()
        return True, []

    except etree.XMLSyntaxError as e:
        # This exception is raised by the streaming parser for both syntax
        # errors and schema validation failures.
        return False, [str(e)]
    except Exception as e:
        # Catch any other unexpected errors.
        return False, [f"An unexpected error occurred: {e}"]


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
