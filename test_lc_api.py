import requests
from lxml import etree
import re
import time
import json
import os
import pandas as pd

# --- Helper Functions (copied from streamlit_app.py for standalone testing) ---
def get_book_metadata(title, author):
    """Queries the Library of Congress API for book metadata using correct CQL syntax."""
    base_url = "http://lx2.loc.gov:210/LCDB"
    
    # Sanitize inputs for the API query
    safe_title = re.sub(r'[^a-zA-Z0-9\s]', '', title)
    safe_author = re.sub(r'[^a-zA-Z0-9\s,]', '', author) # Keep commas for author names

    query = f'bath.title="{safe_title}" and bath.author="{safe_author}"'
    
    params = {
        "version": "1.1",
        "operation": "searchRetrieve",
        "query": query,
        "maximumRecords": "1",
        "recordSchema": "marcxml"
    }
    metadata = {
        'classification': "No DDC found",
        'series_name': "No series found",
        'volume_number': "No volume found",
        'publication_year': "",
        'error': None
    }
    print(f"\n--- Querying for: Title='{title}', Author='{author}' ---")
    print(f"Using CQL Query: {query}")

    try:
        session = requests.Session()
        response = session.get(base_url, params=params, timeout=30)
        response.raise_for_status()
        
        print(f"Request URL: {response.url}")
        print(f"Response Status: {response.status_code}")

        # Print the raw XML content for debugging
        print("\n--- Raw XML Response ---")
        print(response.content.decode('utf-8'))
        print("--- End Raw XML Response ---\n")

        if not response.content or not response.content.strip().startswith(b'<'):
            metadata['error'] = "Received empty or non-XML response."
            print(f"Error: {metadata['error']}")
            return metadata

        root = etree.fromstring(response.content)
        
        # Check for diagnostic errors from the server (like query syntax errors)
        ns_diag = {'diag': 'http://www.loc.gov/zing/srw/diagnostic/'}
        error_message = root.find('.//diag:message', ns_diag)
        if error_message is not None:
            metadata['error'] = f"API returned an error: {error_message.text}"
            print(f"Error: {metadata['error']}")
            return metadata

        ns_marc = {'marc': 'http://www.loc.gov/MARC21/slim'}
        
        # Extract data
        classification_node = root.find('.//marc:datafield[@tag="082"]/marc:subfield[@code="a"]', ns_marc)
        if classification_node is not None: metadata['classification'] = classification_node.text.strip()

        series_node = root.find('.//marc:datafield[@tag="490"]/marc:subfield[@code="a"]', ns_marc)
        if series_node is not None: metadata['series_name'] = series_node.text.strip().rstrip(' ;')
        
        volume_node = root.find('.//marc:datafield[@tag="490"]/marc:subfield[@code="v"]', ns_marc)
        if volume_node is not None: metadata['volume_number'] = volume_node.text.strip()

        pub_year_node = root.find('.//marc:datafield[@tag="264"]/marc:subfield[@code="c"]', ns_marc) or \
                        root.find('.//marc:datafield[@tag="260"]/marc:subfield[@code="c"]', ns_marc)
        if pub_year_node is not None and pub_year_node.text:
            years = re.findall(r'(1[7-9]\d{2}|20\d{2})', pub_year_node.text)
            if years: metadata['publication_year'] = str(min([int(y) for y in years]))

        print(f"Success! Found: {metadata}")

    except requests.exceptions.RequestException as e:
        metadata['error'] = f"API request failed: {e}"
        print(f"Error: {metadata['error']}")
    except etree.XMLSyntaxError as e:
        metadata['error'] = f"XML parsing failed: {e}"
        print(f"Error: {metadata['error']}")
    except Exception as e:
        metadata['error'] = f"An unexpected error occurred: {e}"
        print(f"Error: {metadata['error']}")
        
    return metadata

# --- Main Test Logic ---
if __name__ == "__main__":
    df = pd.read_csv("test_batch.csv", encoding='latin1', dtype=str).fillna('')
    
    print("--- Starting Library of Congress API Test with test_batch.csv ---")
    for i, row in df.iterrows():
        title = row.get('Title', '').strip()
        author = row.get("Author's Name", '').strip()
        if title and author:
             get_book_metadata(title, author)
             time.sleep(1) # Being respectful

    print("\n--- Test Complete ---")
