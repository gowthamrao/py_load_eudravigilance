"""
Unit tests for the Transformer module.
"""
import io

from py_load_eudravigilance.transformer import transform_and_normalize

# A sample nested dictionary, mimicking the output of the enhanced parser.
SAMPLE_ICSR_1 = {
    "safetyreportid": "TEST-CASE-001",
    "receiptdate": "20240101",
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
        }
    ],
}

# A second sample ICSR to test multiple records.
SAMPLE_ICSR_2 = {
    "safetyreportid": "TEST-CASE-002",
    "receiptdate": "20240102",
    "patientinitials": "LW",
    "patientonsetage": "78",
    "patientsex": "2",
    "reactions": [
        {"primarysourcereaction": "Rash", "reactionmeddrapt": "Rash"},
    ],
    "drugs": [], # Test case with no drugs
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
    buffers, row_counts = transform_and_normalize(icsr_generator)

    # 1. Check the overall structure and keys
    expected_tables = [
        "icsr_master",
        "patient_characteristics",
        "reactions",
        "drugs",
    ]
    assert set(buffers.keys()) == set(expected_tables)
    assert set(row_counts.keys()) == set(expected_tables)

    # 2. Check row counts
    assert row_counts["icsr_master"] == 2
    assert row_counts["patient_characteristics"] == 2
    assert row_counts["reactions"] == 3  # 2 from first case, 1 from second
    assert row_counts["drugs"] == 1

    # 3. Check the content of each buffer
    # Icsr_master table
    master_content = buffers["icsr_master"].read()
    assert "safetyreportid,receiptdate" in master_content
    assert "TEST-CASE-001,20240101" in master_content
    assert "TEST-CASE-002,20240102" in master_content

    # Reactions table
    reactions_content = buffers["reactions"].read()
    assert "safetyreportid,primarysourcereaction,reactionmeddrapt" in reactions_content
    assert "TEST-CASE-001,Nausea,Nausea" in reactions_content
    assert "TEST-CASE-001,Headache,Headache" in reactions_content
    assert "TEST-CASE-002,Rash,Rash" in reactions_content

    # Drugs table
    drugs_content = buffers["drugs"].read()
    assert (
        "safetyreportid,drugcharacterization,medicinalproduct,drugstructuredosagenumb,drugstructuredosageunit,drugdosagetext"
        in drugs_content
    )
    assert "TEST-CASE-001,1,DrugA,10,032,10 mg" in drugs_content
    # Ensure no rows from the second case are present
    assert "TEST-CASE-002" not in drugs_content
