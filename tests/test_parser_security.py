"""
Tests for security vulnerabilities in the XML parser.
"""
import os
from io import BytesIO

from py_load_eudravigilance.parser import parse_icsr_xml


class TestParserSecurity:
    def test_placeholder(self):
        assert True
