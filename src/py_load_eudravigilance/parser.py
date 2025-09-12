"""
Handles the Extraction phase of the ETL process.

This module is responsible for finding, parsing, and validating
EudraVigilance ICSR XML files from various sources.
"""
from typing import IO, Any, Dict, Generator, List, Tuple

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


def _find_text(start_element, xpath, default=None):
    found_elem = start_element.find(xpath, namespaces=NAMESPACES)
    return found_elem.text if found_elem is not None else default


def _parse_patient(report_elem):
    patient_elem = report_elem.find("hl7:patient", namespaces=NAMESPACES)
    if patient_elem is None:
        return {}
    return {
        "patientinitials": _find_text(patient_elem, "hl7:patientinitials"),
        "patientonsetage": _find_text(patient_elem, "hl7:patientonsetage"),
        "patientsex": _find_text(patient_elem, "hl7:patientsex"),
    }


def _parse_reactions(report_elem):
    return [
        {
            "primarysourcereaction": _find_text(r, "hl7:primarysourcereaction"),
            "reactionmeddrapt": _find_text(r, "hl7:reactionmeddrapt"),
        }
        for r in report_elem.findall("hl7:reaction", namespaces=NAMESPACES)
    ]


def _parse_drugs(report_elem):
    drugs_list = []
    for drug_elem in report_elem.findall("hl7:drug", namespaces=NAMESPACES):
        substances_list = [
            {"activesubstancename": _find_text(s, "hl7:activesubstancename")}
            for s in drug_elem.findall("hl7:activesubstance", namespaces=NAMESPACES)
        ]
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
    return drugs_list


def _parse_tests(report_elem):
    return [
        {
            "testdate": _find_text(t, "hl7:testdate"),
            "testname": _find_text(t, "hl7:testname"),
            "testresult": _find_text(t, "hl7:testresult"),
            "testresultunit": _find_text(t, "hl7:testresultunit"),
            "testcomments": _find_text(t, "hl7:testcomments"),
        }
        for t in report_elem.findall("hl7:test", namespaces=NAMESPACES)
    ]


def parse_icsr_xml(
    xml_source: IO[bytes],
) -> Generator[Dict[str, Any] | InvalidICSRError, None, None]:
    """
    Parses an ICH E2B(R3) XML file and yields individual ICSRs.
    It is designed to be resilient to errors within a single ICSR message,
    allowing the processing of subsequent valid messages in the same file.
    """
    # Using recover=True allows the parser to make a best-effort attempt to
    # continue processing even if it encounters a structural XML error within
    # a tagged section. This prevents one bad record from failing the whole file.
    context = etree.iterparse(
        xml_source,
        events=("end",),
        tag=f"{{{NAMESPACES['hl7']}}}ichicsrMessage",
        recover=True,
    )

    for _, elem in context:
        try:
            report_elem = elem.find("hl7:safetyreport", namespaces=NAMESPACES)
            if report_elem is None:
                raise InvalidICSRError("Missing required element: safetyreport")

            safety_report_id = _find_text(report_elem, ".//hl7:safetyreportid")
            if not safety_report_id:
                partial_data = {
                    "senderidentifier": _find_text(
                        elem, ".//hl7:messagesenderidentifier"
                    ),
                    "messagedate": _find_text(elem, ".//hl7:messagedate"),
                }
                raise InvalidICSRError(
                    "Missing required field: safetyreportid",
                    partial_data=partial_data,
                )

            nullification_text = _find_text(report_elem, ".//hl7:reportnullification")

            # The primary source country is a direct child of safetyreport
            reporter_country = _find_text(report_elem, "hl7:primarysourcecountry")

            primary_source_elem = report_elem.find(
                "hl7:primarysource", namespaces=NAMESPACES
            )
            qualification = None
            if primary_source_elem is not None:
                qualification = _find_text(primary_source_elem, "hl7:qualification")

            data = {
                "senderidentifier": _find_text(elem, ".//hl7:messagesenderidentifier"),
                "receiveridentifier": _find_text(
                    elem, ".//hl7:messagereceiveridentifier"
                ),
                "safetyreportid": safety_report_id,
                "receiptdate": _find_text(report_elem, ".//hl7:receiptdate"),
                "date_of_most_recent_info": _find_text(
                    report_elem,
                    ".//hl7:dateofmostrecentinformation",
                ),
                "is_nullified": (
                    nullification_text and nullification_text.lower() == "true"
                ),
                "reportercountry": reporter_country,
                "qualification": qualification,
                "narrative": _find_text(report_elem, "hl7:narrativeincludeclinical"),
                "patientinitials": None,
                "patientonsetage": None,
                "patientsex": None,
                **_parse_patient(report_elem),
                "reactions": _parse_reactions(report_elem),
                "drugs": _parse_drugs(report_elem),
                "tests": _parse_tests(report_elem),
            }
            yield data

        except InvalidICSRError as e:
            yield e
        except Exception as e:
            yield InvalidICSRError(f"Unexpected parsing error: {e}")
        finally:
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

        child_tag = next(iter(child_dict))  # Get the single key from the returned dict

        # If the tag already exists, turn it into a list
        if child_tag in children:
            if not isinstance(children[child_tag], list):
                children[child_tag] = [children[child_tag]]
            children[child_tag].append(child_dict[child_tag])
        else:
            children[child_tag] = child_dict[child_tag]

    return {tag: children}


def validate_xml_with_xsd(
    xml_source: IO[bytes], xsd_path: str
) -> Tuple[bool, List[str]]:
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
