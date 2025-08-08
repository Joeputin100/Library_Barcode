import streamlit as st
import pandas as pd
from label_generator import generate_pdf_sheet
import re
import requests
from lxml import etree
import time
import json
import os

# --- Page Configuration ---
st.set_page_config(
    page_title="Barcode & QR Code Label Generator",
    page_icon="✨",
    layout="wide",
)

st.title("Test - Step 2")

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

# --- Instruction Display Function ---
def show_instructions():
    with st.expander("How to Generate the CSV File from Atriuum on Android"):
        st.markdown("""1. Open Atriuum, login to your library, and tap on "Reports".""")
        st.image("images/image4.jpg") # D
        st.markdown("""2. Select 'Shelf List' from the report options.""")
        st.markdown("""3. Configure the report as follows: On the left side of the window, Change Data type to “Holdings Barcode.” Change Qualifier to “is greater than or equal to.” Enter Search Term {The first Holding Number in the range}. Tap Add New.""")
        st.markdown("""5. Change Data type to “Holdings Barcode.” Change Qualifier to “is less than or equal to.” Enter Search Term {The last Holding Number in the range}. Tap Add New.""")
        st.image("images/image3.jpg") # C
        st.markdown("""8.  the red top bar, tap “Columns”.  Change Possible Columns to “Holdings Barcode”.  Tap ➡️. Do the same for “Call Number”, “Author’s name”, “Publication Date”, “Copyright”, “Series Volume”, “Series Title”, and “Title”.  If you tap on “Selected Columns”, you should see all 7 fields.  Tap “Generate Report”.""")
        st.image("images/image5.jpg") # E
        st.image("images/image1.jpg") # A
        st.markdown("""9. Tap “Export Report as CSV”.""")
        st.image("images/image7.jpg") # G
        st.markdown("""10. Tap “Download Exported Report”.  Save as a file name with a .CSV extension.""")
        st.markdown("""11. Locate the file in your device's 'Download' folder.""")

# --- Helper Functions ---
def clean_call_number(call_num_str):
    if not isinstance(call_num_str, str):
        return ""
    cleaned = call_num_str.strip().lstrip(SUGGESTION_FLAG)
    cleaned = cleaned.replace('/', '')
    if cleaned.upper().startswith("FIC"):
        return "FIC"
    if re.match(r'^8\\d{2}\\.\\d+', cleaned):
        return "FIC"
    match = re.match(r'^(\\d+(\\.\\d+)?)', cleaned)
    if match:
        return match.group(1)
    return cleaned

def extract_oldest_year(*date_strings):
    years = []
    for s in date_strings:
        if s and isinstance(s, str):
            found_years = re.findall(r'(1[7-9]\\d{2}|20\\d{2})', s)
            if found_years:
                years.extend([int(y) for y in found_years])
    return str(min(years)) if years else ""

def get_book_metadata(title, author, cache):
    safe_title = re.sub(r'[^a-zA-Z0-9\\s]', '', title)
    safe_author = re.sub(r'[^a-zA-Z0-9\\s,]', '', author)
    cache_key = f"{safe_title}|{safe_author}".lower()
    if cache_key in cache:
        return cache[cache_key]

    base_url = "http://lx2.loc.gov:210/LCDB"
    query = f'bath.title="{safe_title}" and bath.author="{safe_author}"'
    params = {"version": "1.1", "operation": "searchRetrieve", "query": query, "maximumRecords": "1", "recordSchema": "marcxml"}
    metadata = {'classification': "", 'series_name': "", 'volume_number': "", 'publication_year': "", 'error': None}
    
    retry_delays = [5, 30, 60]
    for i, delay in enumerate(retry_delays + [0]): # Add 0 for the initial attempt
        if i > 0:
            st.warning(f"API call failed. Retrying in {delay} seconds...")
            time.sleep(delay)
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
                    years = re.findall(r'(1[7-9]\\d{2}|20\\d{2})', pub_year_node.text)
                    if years: metadata['publication_year'] = str(min([int(y) for y in years]))
                cache[cache_key] = metadata
                return metadata # Success
        except requests.exceptions.RequestException as e:
            if i < len(retry_delays):
                continue # Go to next retry
            metadata['error'] = f"API request failed after multiple retries: {e}"
        except Exception as e:
            metadata['error'] = f"An unexpected error occurred: {e}"
            break # Don't retry on unexpected errors

    return metadata