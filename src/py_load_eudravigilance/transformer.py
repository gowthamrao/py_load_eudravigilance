"""
Handles the Transformation phase of the ETL process.

This module takes the parsed XML data from the Parser and transforms it
into a relational format suitable for bulk loading into the target database.
"""

import csv
import io
import json
from typing import Any, Dict, Generator, IO


from typing import Dict, Tuple

def transform_and_normalize(
    icsr_generator: Generator[Dict[str, Any], None, None]
) -> Tuple[Dict[str, io.StringIO], Dict[str, int]]:
    """
    Transforms and normalizes a generator of ICSR dictionaries into multiple
    in-memory CSV buffers, one for each target relational table.

    This function implements the "T" phase of the ETL, creating normalized,
    linkable data streams suitable for bulk loading.

    Args:
        icsr_generator: A generator yielding nested dictionaries from the parser.

    Returns:
        A tuple containing:
        - A dictionary mapping table names to `io.StringIO` CSV buffers.
        - A dictionary mapping table names to their respective row counts.
    """
    # Define the schemas for our target tables
    schemas = {
        "icsr_master": ["safetyreportid", "receiptdate"],
        "patient_characteristics": [
            "safetyreportid",
            "patientinitials",
            "patientonsetage",
            "patientsex",
        ],
        "reactions": ["safetyreportid", "primarysourcereaction", "reactionmeddrapt"],
        "drugs": [
            "safetyreportid",
            "drugcharacterization",
            "medicinalproduct",
            "drugstructuredosagenumb",
            "drugstructuredosageunit",
            "drugdosagetext",
        ],
        "tests_procedures": [
            "safetyreportid",
            "testdate",
            "testname",
            "testresult",
            "testresultunit",
            "testcomments",
        ],
        "case_summary_narrative": ["safetyreportid", "narrative"],
    }

    # Initialize buffers, writers, and counts for each table
    buffers = {table: io.StringIO() for table in schemas}
    writers = {
        table: csv.DictWriter(buffers[table], fieldnames=fields)
        for table, fields in schemas.items()
    }
    row_counts = {table: 0 for table in schemas}

    # Write headers to all buffers
    for writer in writers.values():
        writer.writeheader()

    # Process each ICSR from the parser
    for icsr_dict in icsr_generator:
        safetyreportid = icsr_dict.get("safetyreportid")
        if not safetyreportid:
            continue  # Skip if the core identifier is missing

        # 1. Populate the master and patient tables (one-to-one)
        master_row = {k: icsr_dict.get(k) for k in schemas["icsr_master"]}
        writers["icsr_master"].writerow(master_row)
        row_counts["icsr_master"] += 1

        patient_row = {k: icsr_dict.get(k) for k in schemas["patient_characteristics"]}
        patient_row["safetyreportid"] = safetyreportid # Add foreign key
        writers["patient_characteristics"].writerow(patient_row)
        row_counts["patient_characteristics"] += 1


        # 2. Populate the reactions table (one-to-many)
        for reaction in icsr_dict.get("reactions", []):
            reaction["safetyreportid"] = safetyreportid  # Add foreign key
            writers["reactions"].writerow(reaction)
            row_counts["reactions"] += 1

        # 3. Populate the drugs table (one-to-many)
        for drug in icsr_dict.get("drugs", []):
            drug["safetyreportid"] = safetyreportid  # Add foreign key
            writers["drugs"].writerow(drug)
            row_counts["drugs"] += 1

        # 4. Populate the tests_procedures table (one-to-many)
        for test in icsr_dict.get("tests", []):
            test["safetyreportid"] = safetyreportid  # Add foreign key
            writers["tests_procedures"].writerow(test)
            row_counts["tests_procedures"] += 1

        # 5. Populate the narrative table (one-to-one)
        if icsr_dict.get("narrative"):
            narrative_row = {"safetyreportid": safetyreportid, "narrative": icsr_dict["narrative"]}
            writers["case_summary_narrative"].writerow(narrative_row)
            row_counts["case_summary_narrative"] += 1

    # Rewind all buffers to be ready for reading
    for buffer in buffers.values():
        buffer.seek(0)

    return buffers, row_counts


def transform_for_audit(
    icsr_generator: Generator[Dict[str, Any], None, None]
) -> Tuple[io.StringIO, int]:
    """
    Transforms a generator of full ICSR dictionaries into an in-memory CSV
    buffer containing the data for the audit log table.

    Each row contains the safety report ID, the version (receipt date), and
    the full ICSR payload as a JSON string.

    Args:
        icsr_generator: A generator yielding full nested dictionaries from
                        the audit parser.

    Returns:
        A tuple containing:
        - An `io.StringIO` CSV buffer for the `icsr_audit_log` table.
        - The total row count for the buffer.
    """
    schema = ["safetyreportid", "receiptdate", "icsr_payload"]
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=schema)
    writer.writeheader()
    row_count = 0

    for icsr_dict in icsr_generator:
        # The dictionary is nested, e.g., {'ichicsrMessage': {'safetyreport': ...}}
        safety_report = icsr_dict.get("ichicsrMessage", {}).get("safetyreport", {})
        if not safety_report:
            continue

        safetyreportid = safety_report.get("safetyreportid")
        receiptdate = safety_report.get("receiptdate")

        if not safetyreportid:
            continue

        row = {
            "safetyreportid": safetyreportid,
            "receiptdate": receiptdate,
            "icsr_payload": json.dumps(safety_report),
        }
        writer.writerow(row)
        row_count += 1

    buffer.seek(0)
    return buffer, row_count
