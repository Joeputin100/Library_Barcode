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
    # Regex to find 4-digit numbers starting with 17, 18, 19, or 20
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
        response = requests.get(base_url, params=params, timeout=10)
        response.raise_for_status()
        
        xml_content = response.content
        root = etree.fromstring(xml_content)
        
        ns = {'marc': 'http://www.loc.gov/MARC21/slim'}
        
        # DDC Classification
        classification_node = root.find('.//marc:datafield[@tag="082"]/marc:subfield[@code="a"]', ns)
        if classification_node is not None:
            metadata['classification'] = classification_node.text.strip()

        # Series Information
        series_node = root.find('.//marc:datafield[@tag="490"]/marc:subfield[@code="a"]', ns)
        if series_node is not None:
            metadata['series_name'] = series_node.text.strip().rstrip(' ;')
        
        volume_node = root.find('.//marc:datafield[@tag="490"]/marc:subfield[@code="v"]', ns)
        if volume_node is not None:
            metadata['volume_number'] = volume_node.text.strip()
            
    except requests.exceptions.RequestException as e:
        metadata['error'] = f"API request failed: {e}"
    except etree.XMLSyntaxError as e:
        metadata['error'] = f"XML parsing failed: {e}"
    except Exception as e:
        metadata['error'] = f"An unexpected error occurred: {e}"
        
    return metadata

# --- UI ---
emojis = ["📚", "📖", "🔖", "✨", "🚀", "💡", "🎉"]
random_emoji = random.choice(emojis)
st.title(f"{random_emoji} Barcode & QR Code Label Generator")

st.markdown("Upload your Atriuum CSV export, map the fields, and generate printable barcode and QR code labels.")

# --- Session State Initialization ---
if 'original_df' not in st.session_state:
    st.session_state.original_df = pd.DataFrame()
if 'processed_df' not in st.session_state:
    st.session_state.processed_df = pd.DataFrame()

# --- File Uploader ---
uploaded_file = st.file_uploader("Upload your Atriuum CSV Export", type=["csv"])

if uploaded_file is not None:
    try:
        # Read and process the uploaded file
        df = pd.read_csv(uploaded_file, encoding='latin1', dtype=str).fillna('')
        st.session_state.original_df = df.copy()
        
        processed_data = []
        for _, row in df.iterrows():
            entry = {
                'Holdings Barcode': row.get('Holdings Barcode', row.get('Line Number', '')).strip(),
                'Title': row.get('Title', '').strip(),
                'Author's Name': row.get('Author's Name', '').strip(),
                'Publication Year': extract_oldest_year(row.get('Copyright', ''), row.get('Publication Date', '')),
                'Series Title': row.get('Series Title', '').strip(),
                'Series Volume': row.get('Series Volume', '').strip(),
                'Call Number': row.get('Call Number', '').strip(),
                'Suggested Series Title': '',
                'Suggested Series Volume': '',
                'Suggested Call Number': ''
            }
            
            # Get suggestions if key fields are missing
            if entry['Title'] and entry['Author's Name'] and (not entry['Series Title'] or not entry['Series Volume'] or not entry['Call Number']):
                lc_meta = get_book_metadata(entry['Title'], entry['Author's Name'])
                if not lc_meta['error']:
                    if not entry['Series Title'] and lc_meta['series_name'] != "No series found":
                        entry['Suggested Series Title'] = lc_meta['series_name']
                    if not entry['Series Volume'] and lc_meta['volume_number'] != "No volume found":
                        entry['Suggested Series Volume'] = lc_meta['volume_number']
                    if not entry['Call Number'] and lc_meta['classification'] != "No DDC found":
                        entry['Suggested Call Number'] = lc_meta['classification']
            processed_data.append(entry)
            
        st.session_state.processed_df = pd.DataFrame(processed_data)
        st.success("CSV processed! Review and edit the data below.")

    except Exception as e:
        st.error(f"Failed to process CSV: {e}")
        st.stop()

if not st.session_state.processed_df.empty:
    st.subheader("Review and Edit Data")
    st.info("Edit cells directly. Suggested values are provided for convenience. Your original CSV is not modified.")
    
    # --- Editable Data Table ---
    edited_df = st.data_editor(
        st.session_state.processed_df,
        key="data_editor",
        num_rows="dynamic",
        use_container_width=True
    )

    # --- Spine Label ID Selection ---
    st.subheader("Spine Label Identifier")
    spine_label_id = st.radio(
        "Select the identifier for the spine label:",
        ('A', 'B', 'C', 'D'), index=1, key="spine_id_radio"
    )

    # --- Generate PDF Button ---
    st.subheader("Generate Labels")
    if st.button("Generate PDF Labels"):
        if edited_df.empty:
            st.warning("No data available to generate labels.")
        else:
            with st.spinner("Generating PDF..."):
                # Prepare data for label generator
                label_data = edited_df.to_dict(orient='records')
                for item in label_data:
                    item['spine_label_id'] = spine_label_id
                
                pdf_bytes = generate_pdf_sheet(label_data)
                st.success("PDF generated!")
                
                st.download_button(
                    label="Download Labels PDF",
                    data=pdf_bytes,
                    file_name="book_labels.pdf",
                    mime="application/pdf"
                )
                
                # --- Printing Instructions ---
                st.subheader("Printing Instructions for Avery 5160")
                st.markdown(
                    """
                    1.  Open the PDF in Adobe Acrobat Reader.
                    2.  Go to `File > Print`.
                    3.  Under **Page Sizing & Handling**, select **"Actual Size"**.
                    4.  Ensure paper size is "Letter" (8.5 x 11 inches).
                    5.  Print. **DO NOT** use "Fit" or "Shrink".
                    """
                )
else:
    st.info("Upload a CSV file to begin.")