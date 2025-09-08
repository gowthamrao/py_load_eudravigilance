import io
import pytest
from py_load_eudravigilance.transformer import transform_to_csv_buffer


def test_transform_to_csv_buffer_with_data():
    """
    Tests that the transformer correctly converts a list of dicts to a CSV buffer.
    """
    # Sample data mimicking the output of the parser
    sample_data = [
        {
            "safetyreportid": "CASE-1",
            "patientinitials": "AB",
            "patientsex": "1",
        },
        {
            "safetyreportid": "CASE-2",
            "patientinitials": "CD",
            "patientsex": None,  # Important to test None handling
        },
    ]

    # The function expects a generator
    data_generator = (item for item in sample_data)

    # Call the function under test
    csv_buffer, row_count = transform_to_csv_buffer(data_generator)

    # 1. Verify the return types and values
    assert isinstance(csv_buffer, io.StringIO)
    assert row_count == 2

    # 2. Verify the content
    expected_csv = (
        "safetyreportid,patientinitials,patientsex\r\n"
        "CASE-1,AB,1\r\n"
        "CASE-2,CD,\r\n"
    )
    assert csv_buffer.read() == expected_csv


def test_transform_to_csv_buffer_empty_input():
    """
    Tests that the transformer handles an empty generator correctly.
    """
    # Call the function with an empty generator
    csv_buffer, row_count = transform_to_csv_buffer(iter([]))

    # 1. Verify the return types and values
    assert isinstance(csv_buffer, io.StringIO)
    assert row_count == 0

    # 2. Verify the content is empty
    assert csv_buffer.read() == ""
