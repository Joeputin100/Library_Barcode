
import re
import requests
from lxml import etree
import time
import json
import os
import threading

SUGGESTION_FLAG = "🐒"
CACHE_FILE = "loc_cache.json"


def load_cache():
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, 'r') as f:
            return json.load(f)
    return {}


def save_cache(cache):
    with open(CACHE_FILE, 'w') as f:
        json.dump(cache, f, indent=4)


def get_book_metadata(title, author, cache, event):
    safe_title = re.sub(r'[^a-zA-Z0-9\s\.:\\]', '', title)
    safe_author = re.sub(r'[^a-zA-Z0-9\s,]', '', author)
    cache_key = f"{safe_title}|{safe_author}".lower()
    if cache_key in cache:
        return cache[cache_key]

    base_url = "http://lx2.loc.gov:210/LCDB"
    if safe_author:
        query = f'bath.title="{safe_title}" and bath.author="{safe_author}"'
    else:
        query = f'bath.title="{safe_title}"'
    params = {"version": "1.1", "operation": "searchRetrieve", "query": query, "maximumRecords": "1", "recordSchema": "marcxml"}
    metadata = {'classification': "", 'series_name': "", 'volume_number': "", 'publication_year': "", 'error': None}

    retry_delays = [5, 30, 60]
    for i in range(len(retry_delays) + 1):
        try:
            response = requests.get(base_url, params=params, timeout=30)
            response.raise_for_status()
            root = etree.fromstring(response.content)
            ns_diag = {'diag': 'http://www.loc.gov/zing/srw/diagnostic/'}
            error_message = root.find('.//diag:message', ns_diag)
            if error_message is not None:
                metadata['error'] = f"API Error: {error_message.text}"
            else:
                ns_marc = {'marc': 'http://www.loc.gov/MARC21/slim'}
                classification_node = root.find('.//marc:datafield[@tag="082"]/marc:subfield[@code="a"]', ns_marc)
                if classification_node is not None: metadata['classification'] = classification_node.text.strip()
                series_node = root.find('.//marc:datafield[@tag="490"]/marc:subfield[@code="a"]', ns_marc)
                if series_node is not None: metadata['series_name'] = series_node.text.strip().rstrip(' ;')
                volume_node = root.find('.//marc:datafield[@tag="490"]/marc:subfield[@code="v"]', ns_marc)
                if volume_node is not None: metadata['volume_number'] = volume_node.text.strip()
                pub_year_node = root.find('.//marc:datafield[@tag="264"]/marc:subfield[@code="c"]', ns_marc) or root.find('.//marc:datafield[@tag="260"]/marc:subfield[@code="c"]', ns_marc)
                if pub_year_node is not None and pub_year_node.text:
                    years = re.findall(r'(1[7-9]\d{2}|20\d{2})', pub_year_node.text)
                    if years: metadata['publication_year'] = str(min([int(y) for y in years]))
                cache[cache_key] = metadata
            event.set() # Signal completion
            return metadata # Success
        except requests.exceptions.RequestException as e:
            # This is where the streamlit warning was
            time.sleep(retry_delays[i])
            continue
        except Exception as e:
            metadata['error'] = f"An unexpected error occurred: {e}"
            break
    event.set() # Signal completion even on failure
    return metadata


def test_api():
    cache = load_cache()
    event = threading.Event()
    metadata = get_book_metadata("A spectacle of corruption.", "Liss, David", cache, event)

    save_cache(cache)


if __name__ == "__main__":
    test_api()
