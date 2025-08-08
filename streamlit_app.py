import streamlit as st
import pandas as pd
import re
import requests
from lxml import etree
import random
from label_generator import generate_pdf_sheet

# --- Page Configuration ---
st.set_page_config(layout="wide", page_title="Barcode & QR Code Label Generator")

# --- Helper Functions ---
def extract_oldest_year(copyright_str, pub_date_str):
    """Extracts the oldest valid 4-digit year from given strings."""
    all_text = f"{copyright_str} {pub_date_str}"
    years = re.findall(r'\b(17\d{2}|18\d{2}|19\d{2}|20\d{2})\b', all_text)
    if years:
        return str(min([int(y) for y in years]))
    return ''

def get_book_metadata(title, author):
    """Queries the Library of Congress API for book metadata."""
    base_url = "http://lx2.loc.gov:210/LCDB"
    query = f'query=@and @attr 1=1003 "{author}" @attr 1=4 "{title}"'
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
        'error': None
    }
    try:
        response = requests.get(base_url, params=params, timeout=15)
        response.raise_for_status()
        root = etree.fromstring(response.content)
        ns = {'marc': 'http://www.loc.gov/MARC21/slim'}
        
        classification_node = root.find('.//marc:datafield[@tag="082"]/marc:subfield[@code="a"]', ns)
        if classification_node is not None: metadata['classification'] = classification_node.text.strip()

        series_node = root.find('.//marc:datafield[@tag="490"]/marc:subfield[@code="a"]', ns)
        if series_node is not None: metadata['series_name'] = series_node.text.strip().rstrip(' ;')
        
        volume_node = root.find('.//marc:datafield[@tag="490"]/marc:subfield[@code="v"]', ns)
        if volume_node is not None: metadata['volume_number'] = volume_node.text.strip()
            
    except requests.exceptions.RequestException as e:
        metadata['error'] = f"API request failed: {e}"
    except Exception as e:
        metadata['error'] = f"An unexpected error occurred: {e}"
        
    return metadata

# --- UI ---
emojis = ["📚", "📖", "🔖", "✨", "🚀", "💡", "🎉"]
st.title(f"{random.choice(emojis)} Barcode & QR Code Label Generator")
st.markdown("Upload your Atriuum CSV export, review the data, and generate printable labels.")

# --- Session State ---
if 'processed_df' not in st.session_state: st.session_state.processed_df = pd.DataFrame()

# --- File Uploader ---
uploaded_file = st.file_uploader("Upload your Atriuum CSV Export", type=["csv"])

if uploaded_file is not None:
    try:
        df = pd.read_csv(uploaded_file, encoding='latin1', dtype=str).fillna('')
        st.success("CSV file read successfully! Here's a preview:")
        st.write(df.head())
    except Exception as e:
        st.error(f"Error reading CSV file: {e}")
        st.stop()

    try:
        st.info("Processing rows and fetching suggestions from the Library of Congress...")
        progress_bar = st.progress(0)
        processed_data = []
        errors = []
        total_rows = len(df)

        for i, (_, row) in enumerate(df.iterrows()):
            entry = {
                'Holdings Barcode': row.get('Holdings Barcode', row.get('Line Number', '')).strip(),
                'Title': row.get('Title', '').strip(),
                "Author's Name": row.get("Author's Name", '').strip(),
                'Publication Year': extract_oldest_year(row.get('Copyright', ''), row.get('Publication Date', '')),
                'Series Title': row.get('Series Title', '').strip(),
                'Series Volume': row.get('Series Volume', '').strip(),
                'Call Number': row.get('Call Number', '').strip(),
                'Suggested Series Title': '', 'Suggested Series Volume': '', 'Suggested Call Number': ''
            }
            
            if entry['Title'] and entry["Author's Name"] and (not entry['Series Title'] or not entry['Series Volume'] or not entry['Call Number']):
                lc_meta = get_book_metadata(entry['Title'], entry["Author's Name"])
                if lc_meta['error']:
                    errors.append(f"Row {i+2}: Could not fetch metadata for '{entry['Title']}'. Reason: {lc_meta['error']}")
                else:
                    if not entry['Series Title'] and lc_meta['series_name'] != "No series found": entry['Suggested Series Title'] = lc_meta['series_name']
                    if not entry['Series Volume'] and lc_meta['volume_number'] != "No volume found": entry['Suggested Series Volume'] = lc_meta['volume_number']
                    if not entry['Call Number'] and lc_meta['classification'] != "No DDC found": entry['Suggested Call Number'] = lc_meta['classification']
            
            processed_data.append(entry)
            progress_bar.progress((i + 1) / total_rows)

        st.session_state.processed_df = pd.DataFrame(processed_data)
        st.success("Data processing complete!")
        if errors:
            st.warning("Some suggestions could not be fetched:")
            with st.expander("View Errors"):
                for error in errors: st.write(error)

    except Exception as e:
        st.error(f"An error occurred during data processing:")
        st.exception(e)
        st.stop()

if not st.session_state.processed_df.empty:
    st.subheader("Review and Edit Data")
    st.info("Edit cells directly. Your original CSV is not modified.")
    
    edited_df = st.data_editor(st.session_state.processed_df, key="data_editor", num_rows="dynamic", use_container_width=True)

    st.subheader("Spine Label Identifier")
    spine_label_id = st.radio("Select ID for spine label:", ('A', 'B', 'C', 'D'), index=1, key="spine_id_radio")

    st.subheader("Generate Labels")
    if st.button("Generate PDF Labels"):
        with st.spinner("Generating PDF..."):
            label_data = edited_df.to_dict(orient='records')
            for item in label_data: item['spine_label_id'] = spine_label_id
            pdf_bytes = generate_pdf_sheet(label_data)
            st.success("PDF generated!")
            st.download_button("Download Labels PDF", pdf_bytes, "book_labels.pdf", "application/pdf")
            st.subheader("Printing Instructions for Avery 5160")
            st.markdown("1. Open in Adobe Acrobat Reader.\n2. Go to `File > Print`.\n3. Under **Page Sizing & Handling**, select **\"Actual Size\"**.\n4. Print. **DO NOT** use \"Fit\" or \"Shrink\".")
else:
    st.info("Upload a CSV file to begin.")
