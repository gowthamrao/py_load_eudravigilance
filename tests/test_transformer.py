"""
Unit tests for the Transformer module.
"""
import csv

from py_load_eudravigilance.transformer import transform_and_normalize

# A sample nested dictionary, mimicking the output of the enhanced parser.
SAMPLE_ICSR_1 = {
    "senderidentifier": "SENDER1",
    "receiveridentifier": "RECEIVER1",
    "safetyreportid": "TEST-CASE-001",
    "receiptdate": "20240101",
    "is_nullified": False,
    "reportercountry": "US",
    "qualification": "Physician",
    "patientinitials": "FN",
    "patientonsetage": "55",
    "patientsex": "1",
    "reactions": [
        {"primarysourcereaction": "Nausea", "reactionmeddrapt": "Nausea"},
        {"primarysourcereaction": "Headache", "reactionmeddrapt": "Headache"},
    ],
    "drugs": [
        {
            "drugcharacterization": "1",
            "medicinalproduct": "DrugA",
            "drugstructuredosagenumb": "10",
            "drugstructuredosageunit": "032",
            "drugdosagetext": "10 mg",
            "substances": [{"activesubstancename": "SubstanceX"}],
        },
        {
            "drugcharacterization": "2",
            "medicinalproduct": "DrugB",
            "drugdosagetext": "50 mg",
            "substances": [
                {"activesubstancename": "SubstanceY"},
                {"activesubstancename": "SubstanceZ"},
            ],
        },
    ],
}

# A second sample ICSR to test multiple records.
SAMPLE_ICSR_2 = {
    "senderidentifier": "SENDER2",
    "receiveridentifier": "RECEIVER2",
    "safetyreportid": "TEST-CASE-002",
    "receiptdate": "20240102",
    "is_nullified": True,
    "reportercountry": "GB",
    "qualification": "Pharmacist",
    "patientinitials": "LW",
    "patientonsetage": "78",
    "patientsex": "2",
    "reactions": [
        {"primarysourcereaction": "Rash", "reactionmeddrapt": "Rash"},
    ],
    "drugs": [],  # Test case with no drugs
}


def test_transform_and_normalize():
    """
    Tests the main transformation and normalization logic.

    It verifies that:
    1. The function returns the correct structure (dicts of buffers and counts).
    2. It creates buffers for all expected tables.
    3. The content of each buffer (headers and rows) is correct.
    4. Foreign keys (`safetyreportid`) are correctly added to child records.
    5. Row counts are accurate.
    """
    # The transformer expects a generator.
    icsr_generator = (i for i in [SAMPLE_ICSR_1, SAMPLE_ICSR_2])

    # Run the transformation
    buffers, row_counts, errors = transform_and_normalize(icsr_generator)

    # 1. Check for errors
    assert not errors, "Should be no errors for valid test data"

    # 2. Check the overall structure and keys
    expected_tables = [
        "icsr_master",
        "patient_characteristics",
        "reactions",
        "drugs",
        "drug_substances",
        "tests_procedures",
        "case_summary_narrative",
    ]
    assert set(buffers.keys()) == set(expected_tables)
    assert set(row_counts.keys()) == set(expected_tables)

    # 2. Check row counts
    assert row_counts["icsr_master"] == 2
    assert row_counts["patient_characteristics"] == 2
    assert row_counts["reactions"] == 3  # 2 from first case, 1 from second
    assert row_counts["drugs"] == 2  # Now two drugs in the first case
    assert row_counts["drug_substances"] == 3  # 1 for DrugA, 2 for DrugB
    assert row_counts["tests_procedures"] == 0  # No tests in sample data
    assert row_counts["case_summary_narrative"] == 0  # No narrative in sample

    # 3. Check the content of each buffer in a more robust way
    # Rewind buffers before reading
    for buffer in buffers.values():
        buffer.seek(0)

    # Parse CSV content into a list of dicts for easier validation
    parsed_data = {}
    for name, buffer in buffers.items():
        # Skip empty buffers
        if row_counts[name] > 0:
            reader = csv.DictReader(buffer)
            parsed_data[name] = list(reader)

    # Validate icsr_master
    master_rows = parsed_data.get("icsr_master", [])
    assert len(master_rows) == 2
    case1_master = next(
        r for r in master_rows if r["safetyreportid"] == "TEST-CASE-001"
    )
    assert case1_master["senderidentifier"] == "SENDER1"
    assert case1_master["is_nullified"] == "False"
    case2_master = next(
        r for r in master_rows if r["safetyreportid"] == "TEST-CASE-002"
    )
    assert case2_master["qualification"] == "Pharmacist"
    assert case2_master["is_nullified"] == "True"

    # Validate reactions
    reaction_rows = parsed_data.get("reactions", [])
    assert len(reaction_rows) == 3
    case1_reactions = [
        r for r in reaction_rows if r["safetyreportid"] == "TEST-CASE-001"
    ]
    assert len(case1_reactions) == 2
    assert {"Nausea", "Headache"} == {r["reactionmeddrapt"] for r in case1_reactions}

    # Validate drugs
    drug_rows = parsed_data.get("drugs", [])
    assert len(drug_rows) == 2
    assert all(r["safetyreportid"] == "TEST-CASE-001" for r in drug_rows)
    drug_a = next(r for r in drug_rows if r["medicinalproduct"] == "DrugA")
    assert drug_a["drugcharacterization"] == "1"
    assert drug_a["drugstructuredosagenumb"] == "10"

    # Validate drug_substances
    substance_rows = parsed_data.get("drug_substances", [])
    assert len(substance_rows) == 3
    drug_b_substances = [r for r in substance_rows if r["drug_seq"] == "2"]
    assert len(drug_b_substances) == 2
    assert {"SubstanceY", "SubstanceZ"} == {
        r["activesubstancename"] for r in drug_b_substances
    }
