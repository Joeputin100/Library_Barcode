import streamlit as st
import pandas as pd
import io
from label_generator import generate_pdf_sheet # Assuming label_generator.py is in the same directory

import random

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
        
        import re
import requests
from lxml import etree

def extract_oldest_year(copyright_str, pub_date_str):
    years = []
    # Regex to find 4-digit numbers starting with 17, 18, 19, or 20
    year_pattern = re.compile(r'\b(1[7-9]\d{2}|20\d{2})\b')

    if copyright_str:
        found_years = year_pattern.findall(str(copyright_str))
        years.extend([int(y) for y in found_years])

    if pub_date_str:
        found_years = year_pattern.findall(str(pub_date_str))
        years.extend([int(y) for y in found_years])

    if years:
        return str(min(years))
    return ''

def get_book_metadata(title, author):
    """
    Queries the Library of Congress API for MODS data to extract classification (FIC or DDC), 
    series name, volume number, and additional metadata for a book.

    Args:
        title (str): The book title (e.g., "Death's End").
        author (str): The author's name (e.g., "Cixin Liu").

    Returns:
        dict: Contains:
            - classification: "FIC" (fiction) or DDC number (non-fiction), or "No DDC found".
            - series_name: Series title or "No series found".
            - volume_number: Volume number or "No volume found".
            - publication_year: Publication year or "No year found".
            - isbn: ISBN or "No ISBN found".
            - error: Error message if query fails, else None.
    """
    # Format query parameters
    title = title.replace(" ", "+").strip()
    author = author.replace(" ", "+").strip()
    url = (
        f"http://z3950.loc.gov:7090/voyager?operation=searchRetrieve&version=1.1"
        f"&query=bath.title=\"{title}\" + AND + bath.personalName=\"{author}\""
        f"&recordSchema=mods&maximumRecords=10"
    )

    try:
        # Send request to LC API
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # Raise exception for HTTP errors
        xml = response.content

        # Parse XML response
        root = etree.fromstring(xml)
        namespaces = {
            "mods": "http://www.loc.gov/mods/v3",
            "zs": "http://www.loc.gov/zing/srw/"
        }

        # Initialize result with default values
        best_result = {
            "classification": "No DDC found",
            "series_name": "No series found",
            "volume_number": "No volume found",
            "publication_year": "No year found",
            "isbn": "No ISBN found",
            "error": None
        }

        # Track best record based on data completeness
        best_score = 0  # Score based on fields present (DDC, series, volume)

        # Iterate through records
        for record in root.findall(".//zs:record/zs:recordData/mods:mods", namespaces):
            result = best_result.copy()
            score = 0

            # Extract DDC and determine fiction/non-fiction
            ddc = record.find(".//mods:classification[@authority='ddc']", namespaces)
            ddc_number = ddc.text if ddc is not None else None
            if ddc_number:
                score += 2  # Higher weight for DDC

            is_fiction = False
            genre = record.find(".//mods:genre", namespaces)
            subjects = record.findall(".//mods:subject/mods:topic", namespaces)
            if genre is not None and any(term in genre.text.lower() for term in ["novel", "fiction"]):
                is_fiction = True
            elif any("fiction" in topic.text.lower() for topic in subjects):
                is_fiction = True
            elif ddc_number and (ddc_number.startswith("8") or ddc_number.startswith("89")) and ".5" in ddc_number:
                is_fiction = True  # 800s or 895 (e.g., Chinese fiction) with .5x indicates fiction

            result["classification"] = "FIC" if is_fiction else ddc_number or "No DDC found"

            # Extract series name
            series = record.find(".//mods:titleInfo[@type='series']/mods:title", namespaces)
            if series is not None:
                result["series_name"] = series.text
                score += 1

            # Extract volume number
            volume = record.find(".//mods:titleInfo[@type='series']/mods:partNumber", namespaces)
            if volume is None:
                volume = record.find(".//mods:part/mods:detail[@type='volume']/mods:number", namespaces)
            if volume is not None:
                result["volume_number"] = volume.text
                score += 1

            # Extract publication year
            year = record.find(".//mods:originInfo/mods:dateIssued", namespaces)
            if year is not None:
                result["publication_year"] = year.text
                score += 1

            # Extract ISBN
            isbn = record.find(".//mods:identifier[@type='isbn']", namespaces)
            if isbn is not None:
                result["isbn"] = isbn.text
                score += 1

            # Update best result if this record has more data
            if score > best_score:
                best_score = score
                best_result = result

        return best_result

    except requests.RequestException as e:
        return {
            "classification": "No DDC found",
            "series_name": "No series found",
            "volume_number": "No volume found",
            "publication_year": "No year found",
            "isbn": "No ISBN found",
            "error": f"Error querying API: {str(e)}"
        }
    except etree.XMLSyntaxError:
        return {
            "classification": "No DDC found",
            "series_name": "No series found",
            "volume_number": "No volume found",
            "publication_year": "No year found",
            "isbn": "No ISBN found",
            "error": "Error parsing XML response"
        }
    except Exception as e:
        return {
            "classification": "No DDC found",
            "series_name": "No series found",
            "volume_number": "No volume found",
            "publication_year": "No year found",
            "isbn": "No ISBN found",
            "error": f"Unexpected error: {str(e)}"
        }

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
            if book_entry['Title'] and book_entry['Author\'s Name'] and \
               (not book_entry['Series Title'] or not book_entry['Series Volume'] or not book_entry['Call Number']):
                
                st.write(f"Querying LC API for: {book_entry['Title']} by {book_entry['Author\'s Name']}") # Debugging
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
                        inventory_num = st.session_state.original_df.loc[idx, 'Line Number'] if 'Line Number' in st.session_state.original_df.columns else \
                                        st.session_state.original_df.loc[idx, 'Holdings Barcode'] if 'Holdings Barcode' in st.session_state.original_df.columns else 'N/A'
                        
                        st.session_state.change_log.append(
                            f"For inventory number **{inventory_num}**, user changed **{col}** from \`{original_val}\` to \`{edited_val}\`."
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
