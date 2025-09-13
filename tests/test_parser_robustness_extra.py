"""
Tests for parser robustness against various attacks.
"""
import pytest
from lxml import etree
from py_load_eudravigilance.parser import parse_icsr_xml, InvalidICSRError

def test_quadratic_blowup_attack():
    """
    Tests that the parser is not vulnerable to the quadratic blowup attack.
    The parser should not hang or crash, and it should not return any valid data.
    """
    with open("tests/data/quadratic_blowup.xml", "rb") as f:
        results = list(parse_icsr_xml(f))
        # The parser should not return any valid data.
        # It might return an InvalidICSRError, or it might return nothing.
        # The important thing is that it doesn't crash and doesn't return
        # a dictionary of parsed data.
        for result in results:
            assert isinstance(result, InvalidICSRError)
