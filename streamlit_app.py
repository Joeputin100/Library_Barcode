import streamlit as st
import pandas as pd
import re
import requests
from lxml import etree
import time
import json
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import xml.etree.ElementTree as ET

# --- Page Title ---
st.title("LOC API Processor")

# --- Feature List ---
st.header("Features")
st.markdown(r'''
- [x] CSV file uploading
- [x] Library of Congress API integration
- [x] Data cleaning and processing
- [ ] Editable data table
- [ ] PDF label generation
''')

# --- Constants & Cache ---
SUGGESTION_FLAG = "🐒"
CACHE_FILE = "loc_cache.json"

# --- Caching Functions ---
def load_cache():
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, 'r') as f:
            return json.load(f)
    return {}

def save_cache(cache):
    with open(CACHE_FILE, 'w') as f:
        json.dump(cache, f, indent=4)

# --- Helper Functions ---
# Updated keyword lists
fiction_keywords = [
    r'\bfiction\b',
    r'\bnovel\b',
    r'\bnovels\b',
    r'\bstory\b',
    r'\bstories\b',
    r'\bshort stories\b',
    r'\bgraphic novel\b',
    r'\bnarrative\b',
    r'\bdrama\b',
    r'\bromance\b',
    r'\bmystery\b',
    r'\bthriller\b',
    r'\bsci[- ]?fi\b',
    r'\bfantasy\b',
    r'\bhistorical fiction\b',
    r'\bliterary fiction\b',
    r'\badventure\b',
    r'\bcoming[- ]of[- ]age\b',
    r'\bfairy tale\b',
    r'\bfable\b',
    r'\bsatire\b',
    r'\bparody\b',
    r'\ballegory\b',
    r'\bmanga\b'
]

nonfiction_exclusions = [
    r'\bnon[- ]?fiction\b',
    r'\bbiography\b',
    r'\bautobiography\b',
    r'\bmemoir\b',
    r'\bself[- ]?help\b',
    r'\bguide\b',
    r'\bmanual\b',
    r'\btextbook\b',
    r'\bhow[- ]to\b',
    r'\btrue story\b',
    r'\bdocumentary\b',
    r'\bhistory\b',
    r'\bscience\b',
    r'\bphilosophy\b',
    r'\bpsychology\b',
    r'\bpolitics\b',
    r'\beconomics\b'
]

def is_likely_fiction(text: str) -> bool:
    """Check if text matches fiction patterns and not nonfiction exclusions."""
    low = text.lower()
    return (
        any(re.search(p, low) for p in fiction_keywords)
        and not any(re.search(p, low) for p in nonfiction_exclusions)
    )

def parse_raw_response(xml_string: str) -> str:
    """
    Parse raw LOC API XML and return 'FIC' if it looks like fiction.
    Scans genreForm, subject, description, summary, title, and notes.
    """
    root = ET.fromstring(xml_string)
    pieces = []
    # MARCXML tags to search for textual content
    # 655: Genre/Form, 650: Subject, 520: Summary, 245: Title, 500: General Note
    tags_to_search = ['655', '650', '520', '245', '500'] 
    
    for tag in tags_to_search:
        # Find all subfields within the datafield
        for datafield in root.findall(f'.//marc:datafield[@tag="{tag}"]', {'marc': 'http://www.loc.gov/MARC21/slim'}):
            for subfield in datafield.findall('marc:subfield', {'marc': 'http://www.loc.gov/MARC21/slim'}):
                if subfield.text:
                    pieces.append(subfield.text)

    combined_text = ' '.join(pieces)
    if is_likely_fiction(combined_text):
        return "FIC"
    return "" # Return empty string if not fiction

def is_fiction_ddn(ddn: str) -> bool:
    """
    Determines if a Dewey Decimal Number (DDN) represents fiction,
    based on an expanded list that includes literature, poetry, drama, and fiction.

    Args:
        ddn (str): The DDN string, e.g., '813', '822.33', '873.01'

    Returns:
        bool: True if the DDN is classified as fiction, False otherwise.
    """
    # Regex pattern for accepted fiction-related DDNs
    fiction_pattern = re.compile(
        r'^('
        r'810(\.\d+)?|'  # American literature
        r'811(\.\d+)?|'  # American poetry
        r'812(\.\d+)?|'  # American drama
        r'813(\.\d+)?|'  # American fiction
        r'822(\.\d+)?|'  # English drama
        r'823(\.\d+)?|'  # English fiction
        r'833(\.\d+)?|'  # German fiction
        r'843(\.\d+)?|'  # French fiction
        r'853(\.\d+)?|'  # Italian fiction
        r'862(\.\d+)?|'  # Spanish drama
        r'863(\.\d+)?|'  # Spanish fiction
        r'872(\.\d+)?|'  # Latin drama
        r'873(\.\d+)?|'  # Latin epic poetry and fiction
        r'883(\.\d+)?'   # Classical Greek epic poetry and fiction
        r')$'
    )
    
    return bool(fiction_pattern.match(ddn.strip()))

def clean_call_number(call_num_str, genres, raw_response_xml): # Added raw_response_xml
    if not isinstance(call_num_str, str):
        return ""
    cleaned = call_num_str.strip().lstrip(SUGGESTION_FLAG)
    cleaned = cleaned.replace('/', '')
    if cleaned.upper().startswith("[FIC]"):
        return "FIC"
    if cleaned.upper().startswith("FIC"):
        return "FIC"
    if is_fiction_ddn(cleaned): # Use the new function
        return "FIC"
    
    # New genre check using raw_response_xml
    if raw_response_xml:
        fiction_from_raw = parse_raw_response(raw_response_xml)
        if fiction_from_raw == "FIC":
            return "FIC"

    match = re.match(r'^(\d+(\.\d+)?)\\', cleaned) # Corrected regex for DDN matching
    if match:
        return match.group(1)
    return cleaned

def get_book_metadata(title, author, cache):
    # Sanitize title and author for API query
    # Allow alphanumeric, spaces, and common punctuation like . : '
    safe_title = re.sub(r'[^a-zA-Z0-9\s\.:\\]', '', title)
    safe_author = re.sub(r'[^a-zA-Z0-9\s,]', '', author)

    # Debugging statements using st.write (assuming 'st' is Streamlit)
    # If not using Streamlit, these should be removed or adapted.
    try:
        # Attempt to import streamlit, if it fails, these lines will be commented out.
        import streamlit as st
        st.write(f"**Debug: Original Title:** {title}")
        st.write(f"**Debug: Sanitized Title:** {safe_title}")
        st.write(f"**Debug: Original Author:** {author}")
        st.write(f"**Debug: Sanitized Author:** {safe_author}")
    except ImportError:
        # If streamlit is not available, print to stdout instead.
        print(f"**Debug: Original Title:** {title}")
        print(f"**Debug: Sanitized Title:** {safe_title}")
        print(f"**Debug: Original Author:** {author}")
        print(f"**Debug: Sanitized Author:** {safe_author}")

    cache_key = f"{safe_title}|{safe_author}".lower()
    if cache_key in cache:
        try:
            import streamlit as st
            st.write(f"**Debug: Cache Hit for:** {cache_key}")
        except ImportError:
            print(f"**Debug: Cache Hit for:** {cache_key}")
        return cache[cache_key]
    try:
        import streamlit as st
        st.write(f"**Debug: Cache Miss for:** {cache_key}")
    except ImportError:
        print(f"**Debug: Cache Miss for:** {cache_key}")

    base_url = "http://lx2.loc.gov:210/LCDB"
    query = f'bath.title="{safe_title}" and bath.author="{safe_author}"'
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
    params = {"version": "1.1", "operation": "searchRetrieve", "query": query, "maximumRecords": "1", "recordSchema": "marcxml"}
    metadata = {'classification': "", 'series_name': "", 'volume_number': "", 'publication_year': "", 'genres': [], 'error': None, 'query': query, "raw_response": ''} 
    
    try:
        import streamlit as st
        st.write(f"**Debug: API Query:** {query}")
    except ImportError:
        print(f"**Debug: API Query:** {query}")

    try:
        response = requests.get(base_url, params=params, timeout=30, headers=headers)
        response.raise_for_status()
        metadata['raw_response'] = response.content.decode('utf-8')
        try:
            import streamlit as st
            st.write(f"**Debug: Raw API Response (first 500 chars):** {metadata['raw_response'][:500]}")
        except ImportError:
            print(f"**Debug: Raw API Response (first 500 chars):** {metadata['raw_response'][:500]}")
        root = etree.fromstring(response.content)
        ns_diag = {'diag': 'http://www.loc.gov/zing/srw/diagnostic/'}
        error_message = root.find('.//diag:message', ns_diag)
        if error_message is not None:
            metadata['error'] = f"API Error: {error_message.text}"
            try:
                import streamlit as st
                st.error(f"**Debug: API Error Message:** {error_message.text}")
            except ImportError:
                print(f"**Debug: API Error Message:** {error_message.text}")
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
            genre_nodes = root.findall('.//marc:datafield[@tag="655"]/marc:subfield[@code="a"]', ns_marc)
            metadata['genres'] = [node.text.strip() for node in genre_nodes if node.text]
            try:
                import streamlit as st
                st.write(f"**Debug: Extracted Metadata:** {metadata}")
            except ImportError:
                print(f"**Debug: Extracted Metadata:** {metadata}")
    except requests.exceptions.RequestException as e:
        metadata['error'] = f"API request failed: {e}"
        try:
            import streamlit as st
            st.error(f"**Debug: Request Exception:** {e}")
        except ImportError:
            print(f"**Debug: Request Exception:** {e}")
    except etree.XMLSyntaxError as e:
        metadata['error'] = f"XML parsing error: {e}"
        try:
            import streamlit as st
            st.error(f"**Debug: XML Parsing Error:** {e}")
        except ImportError:
            print(f"**Debug: XML Parsing Error:** {e}")
    except Exception as e:
        metadata['error'] = f"An unexpected error occurred: {e}"
        try:
            import streamlit as st
            st.error(f"**Debug: Unexpected Error:** {e}")
        except ImportError:
            print(f"**Debug: Unexpected Error:** {e}")
    finally:
        cache[cache_key] = metadata
        return metadata

def main():
    uploaded_file = st.file_uploader("Upload your Atriuum CSV Export", type="csv")

    if uploaded_file:
        df = pd.read_csv(uploaded_file, encoding='latin1', dtype=str).fillna('')
        loc_cache = load_cache()
        
        st.write("Processing rows...")
        progress_bar = st.progress(0)
        
        results = []

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(get_book_metadata, row.get('Title', '').strip(), row.get("Author's Name", '').strip(), loc_cache): i for i, row in df.iterrows()}
            
            for i, future in enumerate(as_completed(futures)):
                row_index = futures[future]
                lc_meta = future.result()
                row = df.iloc[row_index]
                title = row.get('Title', '').strip()
                author = row.get("Author's Name", '').strip()
                
                api_call_number = lc_meta.get('classification', '')
                cleaned_call_number = clean_call_number(api_call_number, lc_meta.get('genres', []), lc_meta.get('raw_response', ''))
                
                results.append({
                    'Title': title,
                    'Author': author,
                    'API Call Number': api_call_number,
                    'Cleaned Call Number': cleaned_call_number
                })
                progress_bar.progress((i + 1) / len(df))

        save_cache(loc_cache)
        
        st.write("Processing complete!")
        
        st.dataframe(pd.DataFrame(results))

if __name__ == "__main__":
    main()