import streamlit as st
import pandas as pd
import io
from label_generator import generate_pdf_sheet # Assuming label_generator.py is in the same directory

import random
import requests
from lxml import etree

st.set_page_config(layout="wide", page_title="Barcode & QR Code Label Generator")

emojis = ["📚", "📖", "🔖", "✨", "🚀", "💡", "🎉"]
random_emoji = random.choice(emojis)
st.title(f"{random_emoji} Barcode & QR Code Label Generator")

st.markdown(
    """
    Upload your Atriuum CSV export, map the fields, and generate printable barcode and QR code labels.
    """
)

# --- Session State for Tracking Changes ---
if 'original_df' not in st.session_state:
    st.session_state.original_df = pd.DataFrame()
if 'edited_df' not in st.session_state:
    st.session_state.edited_df = pd.DataFrame()
if 'changes_made' not in st.session_state:
    st.session_state.changes_made = False
if 'change_log' not in st.session_state:
    st.session_state.change_log = []

# --- CSV Upload ---
uploaded_file = st.file_uploader("Upload your Atriuum CSV Export", type=["csv"])

if uploaded_file is not None:
    try:
        df = pd.read_csv(uploaded_file, encoding='latin1', dtype=str)
        st.session_state.original_df = df.copy() # Store original for change tracking
        st.session_state.edited_df = df.copy() # Initialize edited_df
        st.session_state.changes_made = False
        st.session_state.change_log = []

        st.success("CSV uploaded successfully! Now, review and edit your data.")

        # --- Hardcoded Field Mapping and Data Preparation ---
        # Define the expected fields for label_generator.py and their mapping from Atriuum CSV
        # Note: 'inventory_number' will be derived from 'Line Number' or 'Holdings Barcode'
        # 'publication_year' will be derived from 'Copyright' or 'Publication Date'
        
        if uploaded_file is not None:
    try:
        df = pd.read_csv(uploaded_file, encoding='latin1', dtype=str)
        st.session_state.original_df = df.copy() # Store original for change tracking
        st.session_state.edited_df = df.copy() # Initialize edited_df
        st.session_state.changes_made = False
        st.session_state.change_log = []

        st.success("CSV uploaded successfully! Now, review and edit your data.")

        # --- Hardcoded Field Mapping and Data Preparation ---
        # Create a list to hold processed book data for the label generator
        processed_book_data = []
        
        for index, row in st.session_state.edited_df.iterrows():
            book_entry = {}
            
            # Problem 1: Inventory Number (prioritize 'Holdings Barcode')
            if 'Holdings Barcode' in row and pd.notna(row['Holdings Barcode']):
                book_entry['Holdings Barcode'] = str(row['Holdings Barcode']).strip()
            elif 'Line Number' in row and pd.notna(row['Line Number']):
                book_entry['Holdings Barcode'] = str(row['Line Number']).strip()
            else:
                book_entry['Holdings Barcode'] = '' # Handle missing inventory number

            # Problem 3: Ensure no 'nan' strings, use empty strings for nulls
            # All assignments below use .get() with default '' and .strip() which handles this.

            # Problem 5: Rename local script field names to match Atriuum field names
            book_entry['Title'] = str(row.get('Title', '')).strip()
            book_entry['Author\'s Name'] = str(row.get('Author\'s Name', '')).strip()

            # Problem 2: Publication Year (oldest valid year from Copyright or Publication Date)
            copyright_val = row.get('Copyright', '')
            pub_date_val = row.get('Publication Date', '')
            book_entry['Publication Year'] = extract_oldest_year(copyright_val, pub_date_val)

            book_entry['Series Title'] = str(row.get('Series Title', '')).strip()
            book_entry['Series Volume'] = str(row.get('Series Volume', '')).strip()
            book_entry['Call Number'] = str(row.get('Call Number', '')).strip()

            # New Feature: Suggested values for blank fields using LC API
            # Only query if Title and Author are available and relevant fields are blank
            if book_entry['Title'] and book_entry['Author\'s Name'] and 
               (not book_entry['Series Title'] or not book_entry['Series Volume'] or not book_entry['Call Number']):
                
                # st.write(f"Querying LC API for: {book_entry['Title']} by {book_entry['Author\'s Name']}") # Debugging
                lc_metadata = get_book_metadata(book_entry['Title'], book_entry['Author\'s Name'])
                
                if lc_metadata['error']:
                    st.warning(f"LC API Error for '{book_entry['Title']}': {lc_metadata['error']}")
                else:
                    if not book_entry['Series Title'] and lc_metadata['series_name'] != "No series found":
                        book_entry['Suggested Series Title'] = lc_metadata['series_name']
                    if not book_entry['Series Volume'] and lc_metadata['volume_number'] != "No volume found":
                        book_entry['Suggested Series Volume'] = lc_metadata['volume_number']
                    if not book_entry['Call Number'] and lc_metadata['classification'] != "No DDC found":
                        book_entry['Suggested Call Number'] = lc_metadata['classification']
            
            processed_book_data.append(book_entry)

        st.session_state.processed_book_data = processed_book_data

        # --- Editable Data Table ---
        st.subheader("Review and Edit Data")
        st.info("You can edit the cells directly in the table below. Changes will only affect the generated labels, not your original CSV file.")
        st.info("Suggested values from the Library of Congress API are provided in separate columns. You can copy them into the main fields if desired.")

        # Convert processed_book_data back to DataFrame for editing
        editable_df = pd.DataFrame(st.session_state.processed_book_data)
        
        # Display data editor
        edited_data = st.data_editor(
            editable_df,
            key="data_editor",
            num_rows="dynamic",
            use_container_width=True
        )

        # Update session state with edited data and track changes
        if not edited_data.equals(editable_df):
            st.session_state.edited_df = edited_data
            st.session_state.changes_made = True
            
            # Generate change log
            st.session_state.change_log = []
            # Iterate over all columns, including newly added suggested ones
            for col in st.session_state.original_df.columns.union(st.session_state.edited_df.columns):
                # Skip suggested columns for change tracking as they are not original data
                if col.startswith('Suggested '):
                    continue

                # Check if column exists in both original and edited (it might be a new column in edited_df)
                if col in st.session_state.original_df.columns and col in st.session_state.edited_df.columns:
                    # Compare only rows that exist in both (handle row additions/deletions separately if needed)
                    common_indices = st.session_state.original_df.index.intersection(st.session_state.edited_df.index)
                    
                    # Use .loc for consistent indexing and comparison
                    original_series = st.session_state.original_df.loc[common_indices, col]
                    edited_series = st.session_state.edited_df.loc[common_indices, col]

                    # Compare and find differences, handling potential NaN/empty string differences
                    diff = (original_series.fillna('') != edited_series.fillna(''))
                    changed_rows_indices = common_indices[diff]
                    
                    for idx in changed_rows_indices:
                        original_val = st.session_state.original_df.loc[idx, col]
                        edited_val = st.session_state.edited_df.loc[idx, col]
                        
                        # Get inventory number for context
                        inventory_num = st.session_state.original_df.loc[idx, 'Line Number'] if 'Line Number' in st.session_state.original_df.columns else 
                                        st.session_state.original_df.loc[idx, 'Holdings Barcode'] if 'Holdings Barcode' in st.session_state.original_df.columns else 'N/A'
                        
                        st.session_state.change_log.append(
                            f"For inventory number **{inventory_num}**, user changed **{col}** from `{original_val}` to `{edited_val}`."
                        )
            
            # Update processed_book_data with edited values for label generation
            st.session_state.processed_book_data = edited_data.to_dict(orient='records')


        # --- Spine Label ID Selection ---
        st.subheader("Spine Label Identifier")
        spine_label_id = st.radio(
            "Select the identifier for the spine label (Label 3):",
            ('A', 'B', 'C', 'D'),
            index=1, # Default to 'B'
            key="spine_id_radio"
        )

        # Update processed_book_data with the selected spine_label_id
        for entry in st.session_state.processed_book_data:
            entry['spine_label_id'] = spine_label_id

        # --- Generate PDF ---
        st.subheader("Generate Labels")
        if st.button("Generate PDF Labels"):
            if not st.session_state.processed_book_data:
                st.warning("Please upload a CSV file first and ensure data is processed.")
            else:
                with st.spinner("Generating PDF... This may take a moment."):
                    pdf_bytes = generate_pdf_sheet(st.session_state.processed_book_data)
                
                st.success("PDF generated successfully!")
                
                st.download_button(
                    label="Download Labels PDF",
                    data=pdf_bytes,
                    file_name="book_labels.pdf",
                    mime="application/pdf"
                )
                
                # --- Printing Instructions ---
                st.subheader("Important Printing Instructions for Avery 5160 Labels")
                st.markdown(
                    """
                    To ensure your labels print correctly on **Avery 5160** sheets, please follow these steps in Adobe Acrobat Reader (Windows 11):

                    1.  Open the downloaded `book_labels.pdf` file in Adobe Acrobat Reader.
                    2.  Go to `File` > `Print` (or press `Ctrl+P`).
                    3.  In the Print dialog box, look for **"Page Sizing & Handling"** or **"Page Scaling"**.
                    4.  Select the option **"Actual Size"** or ensure **"Custom Scale: 100%"** is chosen.
                        **DO NOT** select "Fit", "Shrink Oversized Pages", or "Fit to Page".
                    5.  Verify that the paper size is set to "Letter" (8.5 x 11 inches).
                    6.  Proceed with printing.

                    For more details, you can refer to the [Avery 5160 product page](https://www.avery.com/products/labels/5160).
                    """
                )

                # --- Information Note on Changes ---
                if st.session_state.changes_made and st.session_state.change_log:
                    st.subheader("Data Alterations Note")
                    st.warning("The following changes were made to the data **for label generation only**:")
                    for change in st.session_state.change_log:
                        st.markdown(f"- {change}")
                    st.markdown(
                        """
                        **Please ensure you update your Atriuum database to match these changes if you wish them to be permanent.**
                        """
                    )
                elif st.session_state.changes_made and not st.session_state.change_log:
                    st.info("Changes were made to the data, but no specific field-level alterations were detected by the change tracker (e.g., row additions/deletions).")
                else:
                    st.info("No data alterations were made in the web UI for this session.")

    except Exception as e:
        st.error(f"An error occurred during CSV processing or app rendering: {e}")
        st.exception(e)
else:
    st.info("Please upload a CSV file to begin.")
