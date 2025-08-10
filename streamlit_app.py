import streamlit as st
import pandas as pd
from label_generator import generate_pdf_sheet
import re
import requests
from lxml import etree
import time
import json
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- Page Configuration ---
st.set_page_config(
    page_title="Barcode & QR Code Label Generator",
    page_icon="✨",
    layout="wide",
)

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


# --- Helper Functions ---
def clean_call_number(call_num_str, genres):
    if not isinstance(call_num_str, str):
        return ""
    cleaned = call_num_str.strip().lstrip(SUGGESTION_FLAG)
    cleaned = cleaned.replace('/', '')
    if cleaned.upper().startswith("FIC"):
        return "FIC"
    if re.match(r'^8\\d{2}\.5\\d*$', cleaned):
        return "FIC"
    # Check for fiction genres
    fiction_genres = ["fiction", "novel", "stories"]
    if any(genre.lower() in fiction_genres for genre in genres):
        return "FIC"
    match = re.match(r'^(\d+(\.\d+)?)', cleaned)
    if match:
        return match.group(1)
    return cleaned

def extract_oldest_year(*date_strings):
    years = []
    for s in date_strings:
        if s and isinstance(s, str):
            found_years = re.findall(r'(1[7-9]\d{2}|20\d{2})', s)
            if found_years:
                years.extend([int(y) for y in found_years])
    return str(min(years)) if years else ""

def get_book_metadata(title, author, cache, event):
    st.write(f"**Debug: Entering get_book_metadata for:** {title}") # NEW DEBUG LINE
    safe_title = re.sub(r'[^a-zA-Z0-9\\s\\.]', '', title) # Allow periods
    safe_author = re.sub(r'[^a-zA-Z0-9\\s,]', '', author)
    cache_key = f"{safe_title}|{safe_author}".lower()
    st.write(f"**Debug: Cache Key:** {cache_key}") # NEW DEBUG LINE
    if cache_key in cache:
        return cache[cache_key]

    base_url = "http://lx2.loc.gov:210/LCDB"
    query = f'bath.title="{safe_title}" and bath.author="{safe_author}"'
    st.write(f"**Debug: API Query:** {query}") # DEBUG LINE
    params = {"version": "1.1", "operation": "searchRetrieve", "query": query, "maximumRecords": "1", "recordSchema": "marcxml"}
    metadata = {'classification': "", 'series_name': "", 'volume_number': "", 'publication_year': "", 'genres': [], 'error': None} 
    
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
                genre_nodes = root.findall('.//marc:datafield[@tag="655"]/marc:subfield[@code="a"]', ns_marc)
                if genre_nodes:
                    metadata['genres'] = [g.text.strip().rstrip('.') for g in genre_nodes]
                cache[cache_key] = metadata
            event.set() # Signal completion
            return metadata # Success
        except requests.exceptions.RequestException as e:
            if i < len(retry_delays):
                st.warning(f"API call failed with error: {e}. Retrying in {retry_delays[i]} seconds...")
                time.sleep(retry_delays[i])
                continue
            metadata['error'] = f"API request failed after multiple retries: {e}"
        except Exception as e:
            metadata['error'] = f"An unexpected error occurred: {e}"
            break
    event.set() # Signal completion even on failure
    return metadata

# --- UI & Logic ---
st.title("✨ Barcode & QR Code Label Generator")

if st.button("Clear Cache & Start Over"):
    st.session_state.clear()
    if os.path.exists(CACHE_FILE):
        os.remove(CACHE_FILE)
    st.rerun()



st.write("Upload your Atriuum CSV export, review the data, and generate printable labels.")

if 'processed_df' not in st.session_state:
    st.session_state.processed_df = None

uploaded_file = st.file_uploader("Upload your Atriuum CSV Export", type="csv")

if uploaded_file and st.session_state.processed_df is None:
    try:
        df = pd.read_csv(uploaded_file, encoding='latin1', dtype=str).fillna('')
        st.success("CSV file read successfully!")
        st.write("**Debug: CSV Columns:**", df.columns.tolist()) # DEBUG LINE
        st.write("**Debug: First 5 Rows of Call Numbers:**", df['Call Number'].head().tolist()) # DEBUG LINE
        loc_cache = load_cache()
        st.write("Processing rows and fetching suggestions...")
        progress_bar = st.progress(0)
        progress_text = st.empty()
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(get_book_metadata, row.get('Title', '').strip(), row.get("Author's Name", '').strip(), loc_cache, threading.Event()): i for i, row in df.iterrows()}
            
            processed_data = [None] * len(df)
            errors = []
            for future in as_completed(futures):
                i = futures[future]
                lc_meta = future.result()
                row = df.iloc[i]
                title = row.get('Title', '').strip()
                
                st.write(f"**Debug: LoC Metadata for Row {i+1}:**", lc_meta) # DEBUG LINE
                entry = {
                    'Holdings Barcode': row['Holdings Barcode'],
                    'Title': row['Title'],
                    "Author's Name": row["Author's Name"],
                    'Publication Year': extract_oldest_year(row.get('Copyright', ''), row.get('Publication Date', '')),
                    'Series Title': row['Series Title'],
                    'Series Volume': row['Series Volume'],
                    'Call Number': row['Call Number'],
                }
                use_loc = False
                if lc_meta and not lc_meta.get('error'):
                    if not entry['Series Title'] and lc_meta['series_name']:
                        entry['Series Title'] = f"{SUGGESTION_FLAG}{lc_meta['series_name']}"
                        use_loc = True
                    if not entry['Series Volume'] and lc_meta['volume_number']:
                        entry['Series Volume'] = f"{SUGGESTION_FLAG}{lc_meta['volume_number']}"
                        use_loc = True
                    if not entry['Call Number'] and lc_meta['classification']:
                        entry['Call Number'] = f"{SUGGESTION_FLAG}{lc_meta['classification']}"
                        use_loc = True
                    if not entry['Publication Year'] and lc_meta['publication_year']:
                        entry['Publication Year'] = f"{SUGGESTION_FLAG}{lc_meta['publication_year']}"
                        use_loc = True
                elif lc_meta and lc_meta.get('error'):
                    errors.append(f"Row {i+2}: '{title}' - {lc_meta['error']}")

                entry['Call Number'] = clean_call_number(entry['Call Number'], lc_meta.get('genres', []))
                entry['✅ Use LoC'] = use_loc
                processed_data[i] = entry
                progress_text.text(f"Processing {i+1}/{len(df)}: {title[:40]}...")
                progress_bar.progress((i + 1) / len(df))

        save_cache(loc_cache)
        progress_text.text("Processing complete!")
        st.session_state.processed_df = pd.DataFrame(processed_data)
        st.session_state.original_df = st.session_state.processed_df.copy()
        if errors:
            st.warning("Some suggestions could not be fetched:")
            with st.expander("View Errors"):
                for error in errors: st.write(error)
    except Exception as e:
        st.error(f"An error occurred during data processing: {e}")
        st.exception(e)
        st.session_state.processed_df = None

if st.session_state.get('processed_df') is not None:
    st.subheader("Review and Edit Label Data")
    st.info(f"{SUGGESTION_FLAG} indicates data from the Library of Congress. Uncheck the box to revert to original.")
    df_to_edit = st.session_state.processed_df.copy()
    for i, row in df_to_edit.iterrows():
        if not row['✅ Use LoC']:
            original_row = st.session_state.original_df.loc[i]
            for col in ['Publication Year', 'Series Title', 'Series Volume', 'Call Number']:
                if col in df_to_edit.columns and col in original_row:
                    df_to_edit.at[i, col] = original_row[col]
                    if col == 'Call Number':
                        df_to_edit.at[i, col] = clean_call_number(df_to_edit.at[i, col], [])
    edited_df = st.data_editor(
        df_to_edit,
        key="data_editor",
        use_container_width=True,
        column_config={
            "✅ Use LoC": st.column_config.CheckboxColumn("Use LoC?", default=False),
            "Title": st.column_config.TextColumn(width="large"),
            "Call Number": st.column_config.TextColumn("Call Number"),
            "Publication Year": st.column_config.TextColumn("Year"),
        }
    )
    st.session_state.processed_df = edited_df
    st.subheader("Spine Label Identifier")
    spine_label_id = st.radio("Select ID for spine label:", ('A', 'B', 'C', 'D'), index=1, key="spine_id_radio", horizontal=True)
    st.subheader("Generate Labels")
    if st.button("Generate PDF Labels"):
        pdf_data = edited_df.to_dict(orient='records')
        for item in pdf_data:
            item['Call Number'] = clean_call_number(item.get('Call Number', ''), [])
            if '✅ Use LoC' in item:
                del item['✅ Use LoC']
            for key, value in item.items():
                if isinstance(value, str) and value.startswith(SUGGESTION_FLAG):
                    item[key] = value.lstrip(SUGGESTION_FLAG)
            item['spine_label_id'] = spine_label_id
        with st.spinner("Generating PDF..."):
            pdf_bytes = generate_pdf_sheet(pdf_data)
            st.success("PDF generated!")
            st.download_button("Download Labels PDF", pdf_bytes, "book_labels.pdf", "application/pdf")
            st.subheader("Printing Instructions for Avery 5160")
            st.markdown("1. Open in Adobe Acrobat Reader.\n2. Go to `File > Print`.\n3. Under **Page Sizing & Handling**, select **\"Actual Size\"**.\n4. Print. **DO NOT** use \"Fit\" or \"Shrink\".")