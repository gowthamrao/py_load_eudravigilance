from pathlib import Path

import pytest
from py_load_eudravigilance.cli import app
from py_load_eudravigilance.parser import validate_xml_with_xsd
from typer.testing import CliRunner

runner = CliRunner()


@pytest.fixture(scope="module")
def test_files(tmpdir_factory):
    """Creates temporary valid and invalid XML files for testing."""
    tmpdir = tmpdir_factory.mktemp("xml_data")
    valid_xml_path = tmpdir.join("valid.xml")
    invalid_xml_path = tmpdir.join("invalid.xml")
    xsd_path = tmpdir.join("schema.xsd")

    valid_xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<ichicsrMessage xmlns="urn:hl7-org:v3">
    <safetyreport>
        <safetyreportid>TEST-VALID-001</safetyreportid>
        <patient>
            <patientinitials>AB</patientinitials>
            <patientsex>1</patientsex>
        </patient>
    </safetyreport>
</ichicsrMessage>
"""
    invalid_xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<ichicsrMessage xmlns="urn:hl7-org:v3">
    <safetyreport>
        <safetyreportid>TEST-INVALID-001</safetyreportid>
        <patient>
            <patientinitials>CD</patientinitials>
        </patient>
    </safetyreport>
</ichicsrMessage>
"""
    xsd_content = """<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
           xmlns:hl7="urn:hl7-org:v3"
           targetNamespace="urn:hl7-org:v3"
           elementFormDefault="qualified">

    <xs:element name="ichicsrMessage">
        <xs:complexType>
            <xs:sequence>
                <xs:element name="safetyreport" type="hl7:safetyreportType"/>
            </xs:sequence>
        </xs:complexType>
    </xs:element>

    <xs:complexType name="safetyreportType">
        <xs:sequence>
            <xs:element name="safetyreportid" type="xs:string"/>
            <xs:element name="patient" type="hl7:patientType"/>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="patientType">
        <xs:sequence>
            <xs:element name="patientinitials" type="xs:string"/>
            <xs:element name="patientsex" type="xs:string"/>
        </xs:sequence>
    </xs:complexType>
</xs:schema>
"""
    valid_xml_path.write(valid_xml_content)
    invalid_xml_path.write(invalid_xml_content)
    xsd_path.write(xsd_content)

    return str(valid_xml_path), str(invalid_xml_path), str(xsd_path)


def test_validate_command_success(test_files):
    """Test the validate command with a valid XML file."""
    valid_xml, _, xsd_file = test_files
    result = runner.invoke(app, ["validate", valid_xml, f"--schema={xsd_file}"])
    assert result.exit_code == 0
    assert "[VALID]" in result.stdout
    assert "valid.xml" in result.stdout
    assert "Validation summary: 1 file(s) valid, 0 file(s) invalid." in result.stdout


def test_validate_command_failure(test_files):
    """Test the validate command with an invalid XML file."""
    _, invalid_xml, xsd_file = test_files
    result = runner.invoke(app, ["validate", invalid_xml, f"--schema={xsd_file}"])
    assert result.exit_code == 1
    assert "[INVALID]" in result.stdout
    assert "invalid.xml" in result.stdout
    assert "Missing child element(s)" in result.stdout
    assert "Validation summary: 0 file(s) valid, 1 file(s) invalid." in result.stdout


def test_xsd_validation_logic(test_files):
    """Unit test for the validate_xml_with_xsd function in parser.py."""
    valid_xml_path, invalid_xml_path, xsd_path = test_files

    # Test with valid file
    with open(valid_xml_path, "rb") as f:
        is_valid, errors = validate_xml_with_xsd(f, xsd_path)
    assert is_valid is True
    assert errors == []

    # Test with invalid file
    with open(invalid_xml_path, "rb") as f:
        is_valid, errors = validate_xml_with_xsd(f, xsd_path)
    assert is_valid is False
    assert len(errors) > 0
    # Check for a specific, expected error message from lxml
    assert (
        "Missing child element(s). Expected is ( {urn:hl7-org:v3}patientsex )"
        in errors[0]
    )


def test_validate_command_glob_pattern(test_files):
    """Test the validate command with a glob pattern matching multiple files."""
    # The test_files fixture already created files in a temp dir
    tmpdir = Path(test_files[0]).parent
    glob_pattern = str(tmpdir / "*.xml")
    xsd_file = test_files[2]

    result = runner.invoke(app, ["validate", glob_pattern, f"--schema={xsd_file}"])
    assert result.exit_code == 1  # Should fail because one file is invalid
    assert "[VALID]" in result.stdout
    assert "valid.xml" in result.stdout
    assert "[INVALID]" in result.stdout
    assert "invalid.xml" in result.stdout
    assert "Validation summary: 1 file(s) valid, 1 file(s) invalid." in result.stdout
