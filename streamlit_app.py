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
    layout="centered",
)

# --- Helper Functions ---
def extract_oldest_year(*date_strings):
    """Extracts the oldest 4-digit year from a list of strings."""
    years = []
    for s in date_strings:
        if s and isinstance(s, str):
            # Find all 4-digit numbers that look like years
            found_years = re.findall(r'\b(1[7-9]\d{2}|20\d{2})\b', s)
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
            # More robust regex to find year within text like 'c2003.'
            years = re.findall(r'(1[7-9]\d{2}|20\d{2})', pub_year_node.text)
            if years: metadata['publication_year'] = str(min([int(y) for y in years]))
            
    except requests.exceptions.RequestException as e:
        metadata['error'] = f"API request failed: {e}"
    except Exception as e:
        metadata['error'] = f"An unexpected error occurred: {e}"
        
    return metadata

# --- UI ---
st.title("✨ Barcode & QR Code Label Generator")
st.write("Upload your Atriuum CSV export, review the data, and generate printable labels.")

# Initialize session state
if 'processed_df' not in st.session_state:
    st.session_state.processed_df = pd.DataFrame()

uploaded_file = st.file_uploader("Upload your Atriuum CSV Export", type="csv")

if uploaded_file:
    try:
        # Use latin1 encoding and treat all data as strings
        df = pd.read_csv(uploaded_file, encoding='latin1', dtype=str)
        df = df.fillna('') # Replace NaN with empty strings
        st.success("CSV file read successfully! Here's a preview:")
        
        # Use st.markdown for a gray background preview
        st.markdown(
            f"<div style='background-color: #f0f2f6; padding: 10px; border-radius: 5px;'>{df.head().to_html(index=False)}</div>",
            unsafe_allow_html=True
        )

        st.write("Processing rows and fetching suggestions from the Library of Congress...")
        
        progress_bar = st.progress(0)
        progress_text = st.empty()
        
        processed_data = []
        errors = []
        total_rows = len(df)

        for i, row in df.iterrows():
            progress_text.text(f"Processing row {i+1}/{total_rows}...")
            
            entry = {
                'Holdings Barcode': row.get('Holdings Barcode', row.get('Line Number', '')).strip(),
                'Title': row.get('Title', '').strip(),
                "Author's Name": row.get("Author's Name", '').strip(),
                'Publication Year': extract_oldest_year(row.get('Copyright', ''), row.get('Publication Date', '')),
                'Series Title': row.get('Series Title', '').strip(),
                'Series Volume': row.get('Series Volume', '').strip(),
                'Call Number': row.get('Call Number', '').strip(),
                'Suggested Series Title': '',
                'Suggested Series Volume': '',
                'Suggested Call Number': '',
                'Suggested Publication Year': ''
            }

            # Get suggestions if key fields are missing
            if entry['Title'] and entry["Author's Name"]:
                progress_text.text(f"Processing row {i+1}/{total_rows}: Looking up '{entry['Title']}'...")
                time.sleep(0.5) # Be respectful to the API server
                lc_meta = get_book_metadata(entry['Title'], entry["Author's Name"])
                
                if lc_meta['error']:
                    errors.append(f"Row {i+2}: Could not fetch metadata for '{entry['Title']}'. Reason: {lc_meta['error']}")
                else:
                    if not entry['Series Title'] and lc_meta['series_name']: entry['Suggested Series Title'] = lc_meta['series_name']
                    if not entry['Series Volume'] and lc_meta['volume_number']: entry['Suggested Series Volume'] = lc_meta['volume_number']
                    if not entry['Call Number'] and lc_meta['classification']: entry['Suggested Call Number'] = lc_meta['classification']
                    # Use LoC year as fallback
                    if not entry['Publication Year'] and lc_meta['publication_year']:
                        entry['Publication Year'] = lc_meta['publication_year']
                        entry['Suggested Publication Year'] = f"Suggested: {lc_meta['publication_year']}"


            processed_data.append(entry)
            progress_bar.progress((i + 1) / total_rows)
        
        progress_text.text("Processing complete!")
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
            st.markdown("1. Open in Adobe Acrobat Reader.\n2. Go to `File > Print`.\n3. Under **Page Sizing & Handling**, select **"Actual Size"**.\n4. Print. **DO NOT** use \"Fit\" or \"Shrink\".")
else:
    st.info("Upload a CSV file to begin.")