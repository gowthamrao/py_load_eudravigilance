import pytest
from lxml import etree
from py_load_eudravigilance.parser import parse_icsr_xml, InvalidICSRError
import io

def test_svg_script_injection():
    """
    Test for CVE-2021-43818.
    This test ensures that scripts within SVG images in data URIs are not processed.
    """
    malicious_xml = b'''
    <ichicsrMessage xmlns="urn:hl7-org:v3">
        <safetyreport>
            <safetyreportid>12345</safetyreportid>
            <narrativeincludeclinical>
                <p>Here is an SVG with a script:</p>
                <svg xmlns="http://www.w3.org/2000/svg" width="100" height="100">
                    <circle cx="50" cy="50" r="40" stroke="black" stroke-width="3" fill="red" />
                    <script>alert('XSS')</script>
                </svg>
            </narrativeincludeclinical>
        </safetyreport>
    </ichicsrMessage>
    '''
    xml_file = io.BytesIO(malicious_xml)
    results = list(parse_icsr_xml(xml_file))

    assert len(results) == 1
    assert isinstance(results[0], dict)

    narrative = results[0].get("narrative", "")
    assert narrative is not None
    assert "<script>" not in narrative
    assert "alert('XSS')" not in narrative
