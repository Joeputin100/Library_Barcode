import sys
sys.path.append('/data/data/com.termux/files/home/projects/barcode')
from test_loc_api_no_st import get_book_metadata, load_cache, save_cache
import threading
import json
import xml.etree.ElementTree as ET

cache = load_cache()
event = threading.Event()
metadata = get_book_metadata("Huésped", "Meyer, Stephenie", cache, event)
raw_response = metadata.get('raw_response', '')

classification_xml = ""
if raw_response:
    try:
        root = ET.fromstring(raw_response)
        ns_marc = {'marc': 'http://www.loc.gov/MARC21/slim'}
        classification_node = root.find('.//marc:datafield[@tag="082"]', ns_marc)
        if classification_node is not None:
            classification_xml = ET.tostring(classification_node, encoding='unicode')
    except ET.ParseError:
        classification_xml = "Error parsing XML"

print(classification_xml)
save_cache(cache)
