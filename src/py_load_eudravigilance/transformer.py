"""
Handles the Transformation phase of the ETL process.

This module takes the parsed XML data from the Parser and transforms it
into a relational format suitable for bulk loading into the target database.
"""

import csv
import io
import json
from typing import Any, Dict, Generator, List, Tuple

from . import schema as db_schema
from .parser import InvalidICSRError


def _write_master_and_patient(writers, row_counts, schemas, icsr_dict, safetyreportid):
    master_row = {k: icsr_dict.get(k) for k in schemas["icsr_master"]}
    is_nullified_val = master_row.get("is_nullified")
    master_row["is_nullified"] = str(is_nullified_val is not None and is_nullified_val)
    writers["icsr_master"].writerow(master_row)
    row_counts["icsr_master"] += 1

    patient_row = {k: icsr_dict.get(k) for k in schemas["patient_characteristics"]}
    patient_row["safetyreportid"] = safetyreportid
    writers["patient_characteristics"].writerow(patient_row)
    row_counts["patient_characteristics"] += 1


def _write_reactions(writers, row_counts, icsr_dict, safetyreportid):
    for reaction in icsr_dict.get("reactions", []):
        reaction["safetyreportid"] = safetyreportid
        writers["reactions"].writerow(reaction)
        row_counts["reactions"] += 1


def _write_drugs_and_substances(
    writers, row_counts, schemas, icsr_dict, safetyreportid
):
    drug_seq = 0
    for drug in icsr_dict.get("drugs", []):
        drug_seq += 1
        drug["safetyreportid"] = safetyreportid
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


def _write_tests(writers, row_counts, icsr_dict, safetyreportid):
    for test in icsr_dict.get("tests", []):
        test["safetyreportid"] = safetyreportid
        writers["tests_procedures"].writerow(test)
        row_counts["tests_procedures"] += 1


def _write_narrative(writers, row_counts, icsr_dict, safetyreportid):
    if icsr_dict.get("narrative"):
        narrative_row = {
            "safetyreportid": safetyreportid,
            "narrative": icsr_dict["narrative"],
        }
        writers["case_summary_narrative"].writerow(narrative_row)
        row_counts["case_summary_narrative"] += 1


def transform_and_normalize(
    icsr_generator: Generator[Dict[str, Any] | InvalidICSRError, None, None]
) -> Tuple[Dict[str, io.StringIO], Dict[str, int], List[InvalidICSRError]]:
    target_tables = [
        "icsr_master",
        "patient_characteristics",
        "reactions",
        "drugs",
        "drug_substances",
        "tests_procedures",
        "case_summary_narrative",
    ]
    schemas = {
        name: [
            c.name
            for c in db_schema.metadata.tables[name].columns
            if c.server_default is None
        ]
        for name in target_tables
    }
    buffers = {table: io.StringIO() for table in schemas}
    writers = {
        table: csv.DictWriter(buffers[table], fieldnames=fields)
        for table, fields in schemas.items()
    }
    row_counts = {table: 0 for table in schemas}
    for writer in writers.values():
        writer.writeheader()

    parsing_errors = []
    for item in icsr_generator:
        if isinstance(item, InvalidICSRError):
            parsing_errors.append(item)
            continue

        icsr_dict = item
        safetyreportid = icsr_dict["safetyreportid"]

        _write_master_and_patient(
            writers, row_counts, schemas, icsr_dict, safetyreportid
        )
        _write_reactions(writers, row_counts, icsr_dict, safetyreportid)
        _write_drugs_and_substances(
            writers, row_counts, schemas, icsr_dict, safetyreportid
        )
        _write_tests(writers, row_counts, icsr_dict, safetyreportid)
        _write_narrative(writers, row_counts, icsr_dict, safetyreportid)

    for buffer in buffers.values():
        buffer.seek(0)

    return buffers, row_counts, parsing_errors


def transform_for_audit(
    icsr_generator: Generator[Dict[str, Any], None, None]
) -> Tuple[io.StringIO, int]:
    """
    Transforms a generator of full ICSR dictionaries into an in-memory CSV
    buffer for the audit log table.

    This function de-duplicates ICSRs from the source based on safetyreportid,
    keeping only the most recent version according to dateofmostrecentinformation.

    Args:
        icsr_generator: A generator yielding full nested dictionaries.

    Returns:
        A tuple containing the CSV buffer and the total row count.
    """
    # Align the schema with the corrected database schema
    schema = [
        "safetyreportid",
        "date_of_most_recent_info",
        "receiptdate",
        "icsr_payload",
    ]
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=schema)
    writer.writeheader()

    # De-duplication logic using the correct version key
    latest_icsrs = {}
    for icsr_dict in icsr_generator:
        # The structure from the parser is {'ichicsrMessage': {...}}
        safety_report = icsr_dict.get("ichicsrMessage", {}).get("safetyreport", {})
        if not safety_report:
            continue

        safetyreportid = safety_report.get("safetyreportid")
        # Use the correct version key for comparison
        version_date = safety_report.get("dateofmostrecentinformation")

        if not safetyreportid or not version_date:
            continue

        # Keep only the latest version of each report
        if (
            safetyreportid not in latest_icsrs
            or version_date > latest_icsrs[safetyreportid]["version_date"]
        ):
            latest_icsrs[safetyreportid] = {
                "version_date": version_date,
                "receiptdate": safety_report.get("receiptdate"),
                "payload": safety_report,
            }

    # Write de-duplicated records to buffer
    for safetyreportid, data in latest_icsrs.items():
        row = {
            "safetyreportid": safetyreportid,
            "date_of_most_recent_info": data["version_date"],
            "receiptdate": data["receiptdate"],
            "icsr_payload": json.dumps(data["payload"]),
        }
        writer.writerow(row)

    row_count = len(latest_icsrs)
    buffer.seek(0)
    return buffer, row_count
