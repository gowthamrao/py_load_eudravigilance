import pytest
from lxml import etree
from io import BytesIO
from py_load_eudravigilance.parser import parse_icsr_xml, InvalidICSRError

class TestParserSecurity:
    def test_xxe_attack_internal_entity(self):
        """
        Tests that the parser does not resolve internal entities.
        """
        xml_path = "tests/data/test_xxe.xml"
        with open(xml_path, "rb") as f:
            xml_content = f.read()

        results = list(parse_icsr_xml(BytesIO(xml_content)))

        assert len(results) == 1
        assert isinstance(results[0], InvalidICSRError)
        assert "Missing required field: safetyreportid" in results[0].message

    def test_xxe_attack_external_entity(self):
        """
        Tests that the parser is protected against XXE attacks that try to
        read local files.
        """
        xml_path = "tests/data/test_xxe_external.xml"
        with open(xml_path, "rb") as f:
            xml_content = f.read()

        results = list(parse_icsr_xml(BytesIO(xml_content)))

        assert len(results) == 1
        assert isinstance(results[0], InvalidICSRError)
        assert "Missing required field: safetyreportid" in results[0].message

    def test_billion_laughs_attack(self):
        """
        Tests that the parser is protected against the "Billion Laughs"
        attack, which is a type of DoS attack.
        """
        xml_path = "tests/data/billion_laughs.xml"
        with open(xml_path, "rb") as f:
            xml_content = f.read()

        # The parser should not yield any results, as it will fail early
        # on the DTD validation.
        results = list(parse_icsr_xml(BytesIO(xml_content)))
        assert len(results) == 0
