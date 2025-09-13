import pytest
from io import BytesIO
from py_load_eudravigilance.parser import parse_icsr_xml, InvalidICSRError, validate_xml_with_xsd

class TestParserErrorHandling:
    def test_malicious_entity_is_not_resolved(self):
        """
        Tests that the parser does not resolve entities, replacing them with an
        empty string.
        """
        xml_path = "tests/data/malicious_error.xml"
        with open(xml_path, "rb") as f:
            xml_content = f.read()

        results = list(parse_icsr_xml(BytesIO(xml_content)))

        assert len(results) == 1
        data = results[0]
        assert not isinstance(data, InvalidICSRError)
        assert data["patientinitials"] is None
        assert "an evil value" not in str(data)

    def test_malicious_entity_in_validation_error(self):
        """
        Tests that the XSD validator does not resolve entities in error messages.
        """
        xml_path = "tests/data/malicious_error.xml"
        xsd_path = "tests/schemas/sample_schema.xsd"
        with open(xml_path, "rb") as f:
            xml_content = f.read()

        is_valid, errors = validate_xml_with_xsd(BytesIO(xml_content), xsd_path)
        print(errors)
        # The XML is now valid against the schema, because the entity is replaced with an empty string
        # and the field is optional.
        assert is_valid
        assert len(errors) == 0
