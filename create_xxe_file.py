import os

xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE root [
  <!ENTITY xxe SYSTEM "file://{file_path}">
]>
<ichicsrMessage xmlns="urn:hl7-org:v3">
  <safetyreport>
    <safetyreportid>&xxe;</safetyreportid>
  </safetyreport>
</ichicsrMessage>
"""

# Get the absolute path to the target file
target_file = os.path.abspath('tests/data/xxe_target.txt')

# Create the malicious XML content
malicious_xml = xml_content.format(file_path=target_file)

# Write the content to the file
with open('tests/data/test_xxe.xml', 'w') as f:
    f.write(malicious_xml)

print("File 'tests/data/test_xxe.xml' created successfully.")
