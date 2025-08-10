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
def clean_call_number(call_num_str, genres):
    if not isinstance(call_num_str, str):
        return ""
    cleaned = call_num_str.strip().lstrip(SUGGESTION_FLAG)
    cleaned = cleaned.replace('/', '')
    if cleaned.upper().startswith("[FIC]"):
        return "FIC"
    if cleaned.upper().startswith("FIC"):
        return "FIC"
    if re.match(r'^8\\d{2}\\.\\d*$', cleaned):
        return "FIC"
    # Check for fiction genres
    fiction_genres = ["fiction", "novel", "stories"]
    if any(genre.lower() in fiction_genres for genre in genres):
        return "FIC"
    match = re.match(r'^(\d+(\\.\\d+)?)', cleaned)
    if match:
        return match.group(1)
    return cleaned

def get_book_metadata(title, author, cache):
    safe_title = re.sub(r'[^'a-zA-Z0-9\\s\\.\\:]', '', title)
    safe_author = re.sub(r'[^a-zA-Z0-9\\s,]', '', author)
    cache_key = f"{safe_title}|{safe_author}".lower()
    if cache_key in cache:
        return cache[cache_key]

    base_url = "http://lx2.loc.gov:210/LCDB"
    query = f'bath.title="{safe_title}" and bath.author="{safe_author}"'
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
    params = {"version": "1.1", "operation": "searchRetrieve", "query": query, "maximumRecords": "1", "recordSchema": "marcxml"}
    metadata = {'classification': "", 'series_name': "", 'volume_number': "", 'publication_year': "", 'genres': [], 'error': None, 'query': query, 'raw_response': ''} 
    
    try:
        response = requests.get(base_url, params=params, timeout=30, headers=headers)
        response.raise_for_status()
        metadata['raw_response'] = response.content.decode('utf-8')
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
            genre_nodes = root.findall('.//marc:datafield[@tag="655"]/marc:subfield[@code="a"]', ns_marc)
            if genre_nodes:
                metadata['genres'] = [g.text.strip().rstrip('.') for g in genre_nodes]
            cache[cache_key] = metadata
        return metadata
    except requests.exceptions.RequestException as e:
        metadata['error'] = f"API request failed: {e}"
    except Exception as e:
        metadata['error'] = f"An unexpected error occurred: {e}"
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
                cleaned_call_number = clean_call_number(api_call_number, lc_meta.get('genres', []))
                
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