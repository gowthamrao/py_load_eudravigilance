import pytest
from lxml import etree
from io import BytesIO
from py_load_eudravigilance.parser import parse_icsr_xml, InvalidICSRError

class TestParserDoS:
    def test_deeply_nested_elements_attack(self):
        """
        Tests that the parser is protected against attacks using deeply
        nested elements, which can cause a denial of service.
        This is mitigated by the `huge_tree` option in the lxml parser.
        The parser should not yield any results.
        """
        xml_path = "tests/data/deeply_nested.xml"
        with open(xml_path, "rb") as f:
            xml_content = f.read()

        results = list(parse_icsr_xml(BytesIO(xml_content)))

        assert len(results) == 0
