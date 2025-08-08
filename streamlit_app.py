import streamlit as st
import pandas as pd
from label_generator import generate_pdf_sheet
import re
import requests
from lxml import etree
import time

# --- Page Configuration ---
st.set_page_config(
    page_title="Barcode & QR Code Label Generator",
    page_icon="✨",
    layout="wide", # Use wide layout for more space
)

# --- Helper Functions ---

def extract_oldest_year(*date_strings):
    """Extracts the oldest 4-digit year from a list of strings."""
    years = []
    for s in date_strings:
        if s and isinstance(s, str):
            found_years = re.findall(r'(1[7-9]\d{2}|20\d{2})', s)
            if found_years:
                years.extend([int(y) for y in found_years])
    return str(min(years)) if years else ""

def get_book_metadata(title, author):
    """Queries the Library of Congress API for book metadata using CQL."""
    base_url = "http://lx2.loc.gov:210/LCDB"
    query = f'bath.title="{title}" and bath.author="{author}"'
    params = {
        "version": "1.1", "operation": "searchRetrieve", "query": query,
        "maximumRecords": "1", "recordSchema": "marcxml"
    }
    metadata = {
        'classification': "", 'series_name': "", 'volume_number': "",
        'publication_year': "", 'error': None
    }
    try:
        session = requests.Session()
        response = session.get(base_url, params=params, timeout=20)
        response.raise_for_status()
        root = etree.fromstring(response.content)
        ns_diag = {'diag': 'http://www.loc.gov/zing/srw/diagnostic/'}
        error_message = root.find('.//diag:message', ns_diag)
        if error_message is not None:
            metadata['error'] = f"API Error: {error_message.text}"
            return metadata
        ns_marc = {'marc': 'http://www.loc.gov/MARC21/slim'}
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
    except requests.exceptions.RequestException as e:
        metadata['error'] = f"API request failed: {e}"
    except Exception as e:
        metadata['error'] = f"An unexpected error occurred: {e}"
    return metadata


# --- UI & Logic ---
st.title("✨ Barcode & QR Code Label Generator")
st.write("Upload your Atriuum CSV export, review the data, and generate printable labels.")

# Initialize session state
if 'processed_df' not in st.session_state:
    st.session_state.processed_df = None
if 'original_for_revert' not in st.session_state:
    st.session_state.original_for_revert = {}

SUGGESTION_FLAG = "🐒"

uploaded_file = st.file_uploader("Upload your Atriuum CSV Export", type="csv")

if uploaded_file:
    if st.session_state.processed_df is None:
        try:
            df = pd.read_csv(uploaded_file, encoding='latin1', dtype=str).fillna('')
            st.success("CSV file read successfully!")

            st.write("Processing rows and fetching suggestions...")
            progress_bar = st.progress(0)
            progress_text = st.empty()
            
            processed_data = []
            errors = []
            total_rows = len(df)

            for i, row in df.iterrows():
                progress_text.text(f"Processing row {i+1}/{total_rows}...")
                original_row_data = row.to_dict()
                st.session_state.original_for_revert[i] = original_row_data
                
                entry = {
                    'Holdings Barcode': row.get('Holdings Barcode', row.get('Line Number', '')).strip(),
                    'Title': row.get('Title', '').strip(),
                    "Author's Name": row.get("Author's Name", '').strip(),
                    'Publication Year': extract_oldest_year(row.get('Copyright', ''), row.get('Publication Date', '')),
                    'Series Title': row.get('Series Title', '').strip(),
                    'Series Volume': row.get('Series Volume', '').strip(),
                    'Call Number': row.get('Call Number', '').strip(),
                }
                
                use_loc = False
                if entry['Title'] and entry["Author's Name"]:
                    progress_text.text(f"Processing row {i+1}/{total_rows}: Looking up '{entry['Title'][:30]}...'")
                    time.sleep(0.5)
                    lc_meta = get_book_metadata(entry['Title'], entry["Author's Name"])
                    
                    if lc_meta['error']:
                        errors.append(f"Row {i+2}: '{entry['Title']}' - {lc_meta['error']}")
                    else:
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
                
                entry['✅ Use LoC'] = use_loc
                processed_data.append(entry)
                progress_bar.progress((i + 1) / total_rows)
            
            progress_text.text("Processing complete!")
            st.session_state.processed_df = pd.DataFrame(processed_data)
            if errors:
                st.warning("Some suggestions could not be fetched:")
                with st.expander("View Errors"):
                    for error in errors: st.write(error)

        except Exception as e:
            st.error(f"An error occurred during data processing: {e}")
            st.exception(e)
            st.session_state.processed_df = None # Reset on error

if st.session_state.processed_df is not None and not st.session_state.processed_df.empty:
    st.subheader("Review and Edit Label Data")
    st.info(f"Edits are temporary. {SUGGESTION_FLAG} indicates data from the Library of Congress. Uncheck the box to revert.")
    
    edited_df = st.data_editor(
        st.session_state.processed_df,
        key="data_editor",
        use_container_width=True,
        column_config={
            "✅ Use LoC": st.column_config.CheckboxColumn("Use LoC?", default=False),
            "Title": st.column_config.TextColumn(width="large"),
        }
    )

    # Logic to revert changes if checkbox is unchecked
    for i, row in edited_df.iterrows():
        if not row['✅ Use LoC']:
            original_row = st.session_state.original_for_revert.get(i, {})
            for col in ['Publication Year', 'Series Title', 'Series Volume', 'Call Number']:
                if isinstance(row[col], str) and row[col].startswith(SUGGESTION_FLAG):
                     edited_df.at[i, col] = extract_oldest_year(original_row.get('Copyright', ''), original_row.get('Publication Date', '')) if col == 'Publication Year' else original_row.get(col, '')

    st.session_state.processed_df = edited_df

    st.subheader("Spine Label Identifier")
    spine_label_id = st.radio("Select ID for spine label:", ('A', 'B', 'C', 'D'), index=1, key="spine_id_radio", horizontal=True)
    
    st.subheader("Generate Labels")
    if st.button("Generate PDF Labels"):
        # Create a clean copy for PDF generation, stripping the flag
        pdf_data = edited_df.to_dict(orient='records')
        for item in pdf_data:
            for key, value in item.items():
                if isinstance(value, str) and value.startswith(SUGGESTION_FLAG):
                    item[key] = value.lstrip(SUGGESTION_FLAG)
            item['spine_label_id'] = spine_label_id
            del item['✅ Use LoC'] # Remove checkbox column before sending to generator

        with st.spinner("Generating PDF..."):
            pdf_bytes = generate_pdf_sheet(pdf_data)
            st.success("PDF generated!")
            st.download_button("Download Labels PDF", pdf_bytes, "book_labels.pdf", "application/pdf")
            
            st.subheader("Printing Instructions for Avery 5160")
            st.markdown("1. Open in Adobe Acrobat Reader.\n2. Go to `File > Print`.\n3. Under **Page Sizing & Handling**, select **\"Actual Size\"**.\n4. Print. **DO NOT** use \"Fit\" or \"Shrink\".")

