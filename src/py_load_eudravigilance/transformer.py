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

from . import schema as db_schema


def transform_and_normalize(
    icsr_generator: Generator[Dict[str, Any], None, None]
) -> Tuple[Dict[str, io.StringIO], Dict[str, int]]:
    """
    Transforms and normalizes a generator of ICSR dictionaries into multiple
    in-memory CSV buffers, one for each target relational table.

    This function implements the "T" phase of the ETL, creating normalized,
    linkable data streams suitable for bulk loading. It uses the central
    `schema.py` module as the single source of truth for table structures.

    Args:
        icsr_generator: A generator yielding nested dictionaries from the parser.

    Returns:
        A tuple containing:
        - A dictionary mapping table names to `io.StringIO` CSV buffers.
        - A dictionary mapping table names to their respective row counts.
    """
    # Define the target tables we will be transforming data for.
    # This order is preserved when creating buffers and writers.
    target_tables = [
        "icsr_master",
        "patient_characteristics",
        "reactions",
        "drugs",
        "drug_substances",
        "tests_procedures",
        "case_summary_narrative",
    ]

    # Dynamically build the schema from the central schema definition.
    # This ensures the transformer is always in sync with the database DDL.
    # We exclude columns with server-side defaults (e.g., load_timestamp).
    schemas = {}
    for table_name in target_tables:
        table_obj = db_schema.metadata.tables[table_name]
        schemas[table_name] = [
            c.name for c in table_obj.columns if c.server_default is None
        ]

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
        # Explicitly convert boolean to string for CSV writer, handling None
        is_nullified_val = master_row.get("is_nullified")
        master_row["is_nullified"] = str(is_nullified_val if is_nullified_val is not None else False)
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

        # 3. Populate the drugs and drug_substances tables (one-to-many)
        drug_seq = 0
        for drug in icsr_dict.get("drugs", []):
            drug_seq += 1
            drug["safetyreportid"] = safetyreportid  # Add foreign key
            drug["drug_seq"] = drug_seq
            writers["drugs"].writerow({k: drug.get(k) for k in schemas["drugs"]})
            row_counts["drugs"] += 1

            for substance in drug.get("substances", []):
                substance_row = {
                    "safetyreportid": safetyreportid,
                    "drug_seq": drug_seq,
                    "activesubstancename": substance.get("activesubstancename"),
                }
                writers["drug_substances"].writerow(substance_row)
                row_counts["drug_substances"] += 1

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
    buffer for the audit log table.

    This function de-duplicates ICSRs from the source based on safetyreportid,
    keeping only the most recent version according to receiptdate.

    Args:
        icsr_generator: A generator yielding full nested dictionaries.

    Returns:
        A tuple containing the CSV buffer and the total row count.
    """
    schema = ["safetyreportid", "receiptdate", "icsr_payload"]
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=schema)
    writer.writeheader()

    # De-duplication logic
    latest_icsrs = {}
    for icsr_dict in icsr_generator:
        safety_report = icsr_dict.get("ichicsrMessage", {}).get("safetyreport", {})
        if not safety_report:
            continue

        safetyreportid = safety_report.get("safetyreportid")
        receiptdate = safety_report.get("receiptdate")

        if not safetyreportid or not receiptdate:
            continue

        if safetyreportid not in latest_icsrs or receiptdate > latest_icsrs[safetyreportid]['receiptdate']:
            latest_icsrs[safetyreportid] = {
                "receiptdate": receiptdate,
                "payload": safety_report
            }

    # Write de-duplicated records to buffer
    for safetyreportid, data in latest_icsrs.items():
        row = {
            "safetyreportid": safetyreportid,
            "receiptdate": data["receiptdate"],
            "icsr_payload": json.dumps(data["payload"]),
        }
        writer.writerow(row)

    row_count = len(latest_icsrs)
    buffer.seek(0)
    return buffer, row_count
