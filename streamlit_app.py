import streamlit as st
import hashlib
import os
import pandas as pd
import io
import logging
from datetime import datetime
import pytz
import re

from caching import load_cache, save_cache
from data_cleaning import (
    clean_title,
    capitalize_title_mla,
    clean_author,
    clean_call_number,
    clean_series_number,
    extract_year,
    SUGGESTION_FLAG,
)
from api_calls import get_book_metadata_initial_pass, get_vertex_ai_classification_batch
from pdf_generation import generate_pdf_labels
from csv_importer import import_csv

# --- Logging Setup ---
if 'log_capture_string' not in st.session_state:
    st.session_state.log_capture_string = io.StringIO()

st_logger = logging.getLogger()
st_logger.setLevel(logging.DEBUG)
st_handler = logging.StreamHandler(st.session_state.log_capture_string)
st_handler.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
# Remove existing handlers to prevent duplicate logs
if st_logger.handlers:
    st_logger.handlers = []
st_logger.addHandler(st_handler)
logging.getLogger('api_calls').setLevel(logging.DEBUG)
logging.getLogger('data_cleaning').setLevel(logging.DEBUG)

# --- Page Title ---
st.title("Atriuum Label Generator")
    # Calculate MD5 hash of the current script file
script_path = os.path.abspath(__file__)
with open(script_path, "rb") as f:
script_hash = hashlib.md5(f.read()).hexdigest()
st.caption(f"Script MD5: {script_hash}")

# --- Constants for UI and Data Sourcing ---
SOURCE_COLORS = {
    "Atriuum": "#FFBF00",  # Amber
    "LOC": "#D8BFD8",  # Lilac
    "Google": "#90EE90",  # Green
    "Vertex": "#E42217",  # Vermillion
}

# Columns to display in the editable table (label-relevant fields)
LABEL_DISPLAY_COLUMNS = [
    'Title',
    'Author',
    'Call Number',
    'Series Info',
    'Series Number',
    'Copyright Year',
    'Holdings Barcode',
]

# --- CSS for colored cells ---
CSS_STYLES = (
    "<style>\n"
    "    .st-emotion-cache-1r4qj8z { /* Target for data editor cells */\n"
    "        background-color: var(--background-color);\n"
    "    }\n"
    "    .st-emotion-cache-1r4qj8z[data-source=\"Atriuum\"] { background-color: #FFBF00; }\n"
    "    .st-emotion-cache-1r4qj8z[data-source=\"LOC\"] { background-color: #D8BFD8; }\n"
    "    .st-emotion-cache-1r4qj8z[data-source=\"Google\"] { background-color: #90EE90; }\n"
    "    .st-emotion-cache-1r4qj8z[data-source=\"Vertex\"] { background-color: #E42217; }\n"
    "    @keyframes flash {\n"
    "        0% { opacity: 1; }\n"
    "        50% { opacity: 0.5; }\n"
    "        100% { opacity: 1; }\n"
    "    }\n"
    "    .flash-button {\n"
    "        animation: flash 1s infinite;\n"
    "    }\n"
    "</style>\n"
)
st.markdown(CSS_STYLES, unsafe_allow_html=True)


# --- Helper Functions for UI State Management ---
def next_step():
    if st.session_state.current_step == "upload_csv":
        st.session_state.current_step = "process_data"
    elif st.session_state.current_step == "process_data":
        st.session_state.current_step = "review_edits"
    elif st.session_state.current_step == "review_edits":
        st.session_state.current_step = "generate_pdf"

def set_step(step):
    st.session_state.current_step = step

def update_processed_df_from_editor():
    edited_data = st.session_state.data_editor['edited_rows']
    for idx, changes in edited_data.items():
        for col, new_value in changes.items():
            st.session_state.processed_df.loc[idx, col] = new_value
            # If a manual edit is made, the source becomes Atriuum (user input)
            st.session_state.processed_df.loc[idx, '_source_data'][col] = "Atriuum"
    st.session_state.edited_rows = {} # Clear edited rows after applying



# --- Main App ---
def _process_single_row(
    row,
    i,
    total_rows,
    loc_cache,
    progress_placeholders,
    update_progress,
    unclassified_books_for_vertex_ai,
    LABEL_DISPLAY_COLUMNS,
    SUGGESTION_FLAG,
):
    st_logger.info(f"Processing row {i + 1}/{total_rows}: {row.get('Title', '')}")
    # Initialize source tracking for this row
    row_sources = {col: "Atriuum" for col in LABEL_DISPLAY_COLUMNS}

    title = row.get('Title', '').strip()
    author = row.get("Author", '').strip()
    is_blank_row = not title and not author

    problematic_books = [
        (
            "The Genius Prince's Guide to Raising a Nation Out Of Debt (Hey, How About Treason?), Vol. 5",
            "Toba, Toru",
        ),
        ("The old man and the sea", "Hemingway, Ernest"),
        ("Jack & Jill (Alex Cross)", "Patterson, James"),
    ]
    is_problematic_row = (title, author) in problematic_books

    st_logger.debug(f"--- Processing row {i + 1}/{total_rows} ---")
    st_logger.debug(f"Original row data: {row.to_dict()}")

    update_progress("Extracting data", f"Processing row {i + 1}/{total_rows}")

    st_logger.debug(f"Calling get_book_metadata_initial_pass for title='{title}', author='{author}'")
    lc_meta, google_cached, loc_cached = get_book_metadata_initial_pass(
        title, author, loc_cache, is_blank=is_blank_row, is_problematic=is_problematic_row
    )
    st_logger.debug(
        f"lc_meta after initial pass: {lc_meta}, Google Cached: {google_cached}, LOC Cached: {loc_cached}"
    )

    if google_cached:
        row_sources['Series Info'] = "Google (Cached)"
        row_sources['Series Number'] = "Google (Cached)"
        row_sources['Copyright Year'] = "Google (Cached)"
        update_progress("Enriching with Google Books", "Done")
    else:
        if lc_meta.get('series_name'):
            row_sources['Series Info'] = "Google"
        if lc_meta.get('volume_number'):
            row_sources['Series Number'] = "Google"
        if lc_meta.get('publication_year'):
            row_sources['Copyright Year'] = "Google"
        update_progress("Enriching with Google Books", "Done") # Even if no data, mark as done

    if loc_cached:
        row_sources['Call Number'] = "LOC (Cached)"
        update_progress("Enriching with Library of Congress", "Done")
    else:
        if lc_meta.get('classification'):
            row_sources['Call Number'] = "LOC"
        update_progress("Enriching with Library of Congress", "Done") # Even if no data, mark as done

    if not lc_meta.get('volume_number'):
        st_logger.debug(f"lc_meta volume_number empty, calling clean_series_number for title='{title}'")
        lc_meta['volume_number'] = clean_series_number(title)
        st_logger.debug(f"lc_meta volume_number after clean_series_number: {lc_meta['volume_number']}")

    if not lc_meta.get('volume_number') and any(
        c.lower() in ['manga', 'comic'] for c in lc_meta.get('genres', [])
    ):
        st_logger.debug(f"Attempting to extract trailing number for manga/comic: {title}")
        trailing_num_match = re.search(r'(\d+)', title)
        if trailing_num_match:
            lc_meta['volume_number'] = trailing_num_match.group(1)
            st_logger.debug(f"Extracted trailing number: {lc_meta['volume_number']}")

    original_holding_barcode = row.get('Holdings Barcode', '').strip()
    raw_original_call_number = row.get('Call Number', '').strip()
    st_logger.debug(
        f"Calling clean_call_number for raw_original_call_number='{raw_original_call_number}'"
    )
    cleaned_original_call_number = clean_call_number(
        raw_original_call_number, [], [], title="", is_original_data=True
    )
    st_logger.debug(f"cleaned_original_call_number: {cleaned_original_call_number}")

    original_series_name = row.get('Series Title', '').strip()
    st_logger.debug(
        f"Calling clean_series_number for original_series_number='{row.get('Series Number', '').strip()}'"
    )
    original_series_number = clean_series_number(row.get('Series Number', '').strip())
    st_logger.debug(f"original_series_number: {original_series_number}")

    original_copyright_year = extract_year(row.get('Copyright', '').strip())
    st_logger.debug(f"original_copyright_year: {original_copyright_year}")

    original_publication_date_year = extract_year(
        row.get('Publication Date', '').strip()
    )
    st_logger.debug(f"original_publication_date_year: {original_publication_date_year}")

    final_original_year = ""
    if original_copyright_year:
        final_original_year = original_copyright_year
    elif original_publication_date_year:
        final_original_year = original_publication_date_year
    st_logger.debug(f"final_original_year: {final_original_year}")

    api_call_number = lc_meta.get('classification', '')
    st_logger.debug(f"Calling clean_call_number for api_call_number='{api_call_number}'")
    cleaned_call_number = clean_call_number(
        api_call_number, lc_meta.get('genres', []),
        lc_meta.get('google_genres', []),
        title=title,
    )
    st_logger.debug(f"cleaned_call_number from API: {cleaned_call_number}")

    mashed_series_name = (lc_meta.get('series_name') or '').strip()
    mashed_volume_number = (lc_meta.get('volume_number') or '').strip()
    mashed_publication_year = (lc_meta.get('publication_year') or '').strip()
    st_logger.debug(
        f"mashed_series_name: {mashed_series_name}, mashed_volume_number: {mashed_volume_number}, mashed_publication_year: {mashed_publication_year}"
    )

    current_call_number = cleaned_original_call_number
    if not current_call_number and cleaned_call_number:
        current_call_number = SUGGESTION_FLAG + cleaned_call_number
        row_sources['Call Number'] = "LOC"  # Assuming LOC is primary source for classification

    current_series_name = original_series_name
    if not current_series_name and mashed_series_name:
        current_series_name = SUGGESTION_FLAG + mashed_series_name
        row_sources['Series Info'] = "Google"  # Assuming Google is primary source for series

    current_series_number = original_series_number
    if not current_series_number and mashed_volume_number:
        current_series_number = SUGGESTION_FLAG + mashed_volume_number
        row_sources['Series Number'] = "Google"  # Assuming Google is primary source for volume

    current_publication_year = final_original_year
    if not current_publication_year and mashed_publication_year:
        current_publication_year = SUGGESTION_FLAG + mashed_publication_year
        row_sources['Copyright Year'] = "Google"  # Assuming Google is primary source for year

    if not current_call_number or current_call_number == "UNKNOWN":
        st_logger.debug(f"Book {title} unclassified, adding to Vertex AI queue.")
        unclassified_books_for_vertex_ai.append(
            {
                'title': title,
                'author': author,
                'row_index': i,
                'lc_meta': lc_meta,
                'row_sources': row_sources,  # Pass sources to Vertex AI step
            }
        )

    result = {
        'Title': capitalize_title_mla(clean_title(title)),
        'Author': clean_author(author),
        'Holdings Barcode': original_holding_barcode,
        'Call Number': current_call_number,
        'Series Info': current_series_name,
        'Series Number': current_series_number,
        'Copyright Year': current_publication_year,
        '_source_data': row_sources,  # Store source data for later
    }
    st_logger.debug(f"Results for row {i}: {result}")
    return result, lc_meta


def main():
    st_logger.info("Streamlit app main function started.")
    st_logger.debug("Streamlit app main function started.")

    if "current_step" not in st.session_state:
        st.session_state.current_step = "upload_csv"
    if "processed_df" not in st.session_state:
        st.session_state.processed_df = pd.DataFrame()
    if "edited_rows" not in st.session_state:
        st.session_state.edited_rows = {}
    if "source_data" not in st.session_state:
        st.session_state.source_data = {}
    if "uploaded_file_hash" not in st.session_state:
        st.session_state.uploaded_file_hash = None
    if "raw_df" not in st.session_state: 
        st.session_state.raw_df = pd.DataFrame() 
    if "processing_done" not in st.session_state:
        st.session_state.processing_done = False

    # Determine the current step based on state
    if "current_step" not in st.session_state:
        st.session_state.current_step = "upload_csv"

    if st.session_state.current_step == "upload_csv":
        if "raw_df" in st.session_state and not st.session_state.raw_df.empty:
            st.session_state.current_step = "process_data"
    elif st.session_state.current_step == "process_data":
        if st.session_state.processing_done and not st.session_state.processed_df.empty:
            st.session_state.current_step = "review_edits"
    # No change needed for review_edits or generate_pdf, as they are terminal states for progression

    # --- Step 1: Upload CSV ---
    if st.session_state.current_step == "upload_csv":
        with st.container():
            st.header("Step 1: Upload CSV Export")
            uploaded_file = st.file_uploader("Upload your Atriuum CSV Export", type="csv", key="csv_uploader")

            if uploaded_file:
                st.session_state.raw_df = import_csv(uploaded_file)
                st.write("### Raw Imported Data (Non-Editable)")
                st.dataframe(st.session_state.raw_df, use_container_width=True, hide_index=True)
                if st.button("Proceed to Data Processing", on_click=next_step):
                    pass  # Handled by on_click

    # --- Step 2: Process Data ---
    if st.session_state.current_step == "process_data":
        with st.container():
            st.header("Step 2: Process Data")
            st.info("Click 'Process Data' to enrich and clean your book entries. This may take some time.")

            st.markdown('<div class="flash-button">', unsafe_allow_html=True)
            if st.button("Process Data", key="process_data_button"):
                st.markdown('</div>', unsafe_allow_html=True)  # Close the div after the button
                loc_cache = load_cache()
                st.session_state.source_data = {}  # Reset source data for new processing

                with st.expander("Processing Report", expanded=True):
                    st.write("Processing rows...")
                    steps = [
                        "Extracting data",
                        "Enriching with Google Books",
                        "Enriching with Library of Congress",
                        "Classifying with Vertex AI",
                        "Cleaning data",
                    ]
                    progress_placeholders = {step: st.progress(0, text=step) for step in steps}

                    def update_progress(step, status):
                        if status == "Pending":
                            progress_placeholders[step].progress(0, text=f"{step}: Pending")
                        elif status == "In progress...":
                            progress_placeholders[step].progress(50, text=f"{step}: In progress...")
                        elif status == "Done":
                            progress_placeholders[step].progress(100, text=f"{step}: Done")
                        else:
                            progress_placeholders[step].progress(0, text=f"{step}: {status}")
                        st.rerun()


                    for step in steps:
                        update_progress(step, "Pending")

                    results = []
                    unclassified_books_for_vertex_ai = []

                    total_rows = len(st.session_state.raw_df)
                    for i, row in st.session_state.raw_df.iterrows():
                        result, lc_meta = _process_single_row(
                            row,
                            i,
                            total_rows,
                            loc_cache,
                            progress_placeholders,
                            update_progress,
                            unclassified_books_for_vertex_ai,
                            LABEL_DISPLAY_COLUMNS,
                            SUGGESTION_FLAG,
                        )
                        results.append(result)
                        # Update progress for the overall "Extracting data" step
                        progress_placeholders["Extracting data"].progress(
                            int(((i + 1) / total_rows) * 100),
                            text=f"Extracting data: Processed row {i+1}/{total_rows}",
                        )

                    update_progress("Extracting data", "Done")

                    if unclassified_books_for_vertex_ai:
                        update_progress("Classifying with Vertex AI", "In progress...")
                        BATCH_SIZE = 5
                        batches = [
                            unclassified_books_for_vertex_ai[j : j + BATCH_SIZE]
                            for j in range(0, len(unclassified_books_for_vertex_ai), BATCH_SIZE)
                        ]

                        for batch_idx, batch in enumerate(batches):
                            st_logger.debug(f"Processing Vertex AI batch: {batch}")
                            batch_classifications, vertex_cached = get_vertex_ai_classification_batch(
                                batch, st.secrets["vertex_ai"], loc_cache
                            )
                            st_logger.debug(
                                f"Received Vertex AI batch classifications: {batch_classifications}, Cached: {vertex_cached}"
                            )

                            if not isinstance(batch_classifications, list):
                                st_logger.warning(
                                    f"Vertex AI batch classifications not a list: {batch_classifications}"
                                )
                                continue

                            for book_data, vertex_ai_results in zip(batch, batch_classifications):
                                st_logger.debug(f"Processing Vertex AI results for book: {book_data}")
                                row_index = book_data['row_index']
                                lc_meta = book_data['lc_meta']
                                title = book_data['title']
                                row_sources = book_data['row_sources']  # Retrieve sources

                                for k, v in vertex_ai_results.items():
                                    if v == "Unknown":
                                        vertex_ai_results[k] = ""
                                st_logger.debug(f"Vertex AI results after 'Unknown' cleanup: {vertex_ai_results}")

                                if vertex_ai_results.get('classification'):
                                    st_logger.debug(
                                        f"Updating lc_meta classification with: {vertex_ai_results['classification']}"
                                    )
                                    lc_meta['classification'] = vertex_ai_results['classification']
                                    if (
                                        'google_genres' not in lc_meta
                                        or not isinstance(lc_meta['google_genres'], list)
                                    ):
                                        st_logger.debug("google_genres not found or not a list, initializing.")
                                        lc_meta['google_genres'] = []
                                    lc_meta['google_genres'].append(vertex_ai_results['classification'])
                                    st_logger.debug(
                                        f"lc_meta google_genres after update: {lc_meta['google_genres']}"
                                    )

                                if vertex_ai_results.get('series_title'):
                                    st_logger.debug(
                                        f"Updating lc_meta series_name with: {vertex_ai_results['series_title']}"
                                    )
                                    lc_meta['series_name'] = vertex_ai_results['series_title']
                                if vertex_ai_results.get('volume_number'):
                                    st_logger.debug(
                                        f"Updating lc_meta volume_number with: {vertex_ai_results['volume_number']}"
                                    )
                                    lc_meta['volume_number'] = vertex_ai_results['volume_number']
                                if vertex_ai_results.get('copyright_year'):
                                    st_logger.debug(
                                        f"Updating lc_meta publication_year with: {vertex_ai_results['copyright_year']}"
                                    )
                                    lc_meta['publication_year'] = vertex_ai_results['copyright_year']

                                st_logger.debug(
                                    f"Calling clean_call_number after Vertex AI for classification='{lc_meta.get('classification', '')}'"
                                )
                                final_call_number_after_vertex_ai = clean_call_number(
                                    lc_meta.get('classification', ''),
                                    lc_meta.get('genres', []),
                                    lc_meta.get('google_genres', []),
                                    title=title,
                                )
                                st_logger.debug(
                                    f"final_call_number_after_vertex_ai: {final_call_number_after_vertex_ai}"
                                )

                                # Only update if current value is empty or from a less authoritative source
                                if (
                                    final_call_number_after_vertex_ai
                                    and not results[row_index]['Call Number'].replace(SUGGESTION_FLAG, '')
                                ):
                                    st_logger.debug(
                                        f"Updating Call Number for row {row_index} with Vertex AI suggestion."
                                    )
                                    results[row_index]['Call Number'] = (
                                        SUGGESTION_FLAG + final_call_number_after_vertex_ai
                                    )
                                    results[row_index]['_source_data']['Call Number'] = (
                                        "Vertex (Cached)" if vertex_cached else "Vertex"
                                    )

                                if lc_meta.get('series_name') and not results[row_index]['Series Info'].replace(
                                    SUGGESTION_FLAG, ''
                                ):
                                    st_logger.debug(
                                        f"Updating Series Info for row {row_index} with Vertex AI suggestion."
                                    )
                                    results[row_index]['Series Info'] = (
                                        SUGGESTION_FLAG + lc_meta.get('series_name')
                                    )
                                    results[row_index]['_source_data']['Series Info'] = (
                                        "Vertex (Cached)" if vertex_cached else "Vertex"
                                    )

                                if lc_meta.get('volume_number') and not results[row_index]['Series Number'].replace(
                                    SUGGESTION_FLAG, ''
                                ):
                                    st_logger.debug(
                                        f"Updating Series Number for row {row_index} with Vertex AI suggestion."
                                    )
                                    results[row_index]['Series Number'] = (
                                        SUGGESTION_FLAG + str(lc_meta.get('volume_number'))
                                    )
                                    results[row_index]['_source_data']['Series Number'] = (
                                        "Vertex (Cached)" if vertex_cached else "Vertex"
                                    )

                                if lc_meta.get('publication_year') and not results[row_index][
                                    'Copyright Year'
                                ].replace(SUGGESTION_FLAG, ''):
                                    st_logger.debug(
                                        f"Updating Copyright Year for row {row_index} with Vertex AI suggestion."
                                    )
                                    results[row_index]['Copyright Year'] = (
                                        SUGGESTION_FLAG + str(lc_meta.get('publication_year'))
                                    )
                                    results[row_index]['_source_data']['Copyright Year'] = (
                                        "Vertex (Cached)" if vertex_cached else "Vertex"
                                    )

                            # Update progress for Vertex AI step
                            progress_placeholders["Classifying with Vertex AI"].progress(
                                int(((batch_idx + 1) / len(batches)) * 100),
                                text=f"Classifying with Vertex AI: Processed batch {batch_idx+1}/{len(batches)}",
                            )

                        update_progress("Classifying with Vertex AI", "Done")

                    update_progress("Cleaning data", "In progress...")
                    # Final pass to clean up any remaining issues or apply final formatting
                    for i, row_data in enumerate(results):
                        # Remove monkey emojis for final display in editable table
                        for col in LABEL_DISPLAY_COLUMNS:
                            if isinstance(row_data.get(col), str) and row_data[col].startswith(
                                SUGGESTION_FLAG
                            ):
                                results[i][col] = row_data[col].lstrip(SUGGESTION_FLAG)

                        # Ensure all label display columns are present, even if empty
                        for col in LABEL_DISPLAY_COLUMNS:
                            if col not in results[i]:
                                results[i][col] = ""

                    update_progress("Cleaning data", "Done")

                    save_cache(loc_cache)

                    st.write("Processing complete!")
                    st.session_state.processing_done = True
                    st.session_state.current_step = "review_edits"

                    st.session_state.processed_df = pd.DataFrame(results)
                    st.session_state.processed_df['_source_data'] = [
                        r['_source_data'] for r in results
                    ]  # Store source data separately
                    

                st.write("### Review and Edit Processed Data")
                st.info(
                    "Values marked with 🐒 are suggestions from external APIs. You can edit any field. Click 'Save Manual Edits' when done."
                )

                # Display editable table with monkey emojis
                st.data_editor(
                    st.session_state.processed_df[LABEL_DISPLAY_COLUMNS],
                    key="data_editor",
                    on_change=update_processed_df_from_editor,
                    use_container_width=True,
                    hide_index=True,
                )

    # --- Step 3: Review and Confirm Data ---
    if st.session_state.current_step == "review_edits":
        with st.container():
            st.header("Step 3: Review and Confirm Data")
            st.info("Review the final data. Colors indicate the source of the data for each field.")

            # Legend for colors
            st.markdown("### Data Source Legend:")
            cols = st.columns(len(SOURCE_COLORS))
            for i, (source, color) in enumerate(SOURCE_COLORS.items()):
                cols[i].markdown(
                    f"<span style='background-color:{color}; padding: 5px; border-radius: 3px;'>{source}</span>",
                    unsafe_allow_html=True,
                )
            st.markdown("\n")  # Add a newline for spacing

            # Function to apply styling based on source_data
            def highlight_source(row):
                style = ['' for _ in row.index]
                for col_name in LABEL_DISPLAY_COLUMNS:
                    source = row['_source_data'].get(col_name, "Atriuum")  # Default to Atriuum
                    color = SOURCE_COLORS.get(source, "transparent")
                    style[row.index.get_loc(col_name)] = f'background-color: {color}'
                return style

            # Display non-editable table with colored backgrounds
            st.dataframe(
                st.session_state.processed_df[LABEL_DISPLAY_COLUMNS].style.apply(highlight_source, axis=1),
                use_container_width=True,
                hide_index=True,
            )

            if st.button("Proceed to PDF Generation", key="proceed_to_pdf_button", on_click=next_step):
                pass  # Handled by on_click

    # --- Step 4: Generate PDF Labels ---
    if st.session_state.current_step == "generate_pdf":
        with st.container():
            st.header("Step 4: Generate PDF Labels")
            st.info("Click 'Generate PDF' to create your barcode labels.")

            if st.button("Generate PDF", key="generate_pdf_button"):
                try:
                    # Prepare data for PDF generation: remove _source_data column
                    pdf_data_df = st.session_state.processed_df.drop(columns=['_source_data'], errors='ignore')
                    pdf_output = generate_pdf_labels(pdf_data_df)
                    st.session_state.pdf_data = pdf_output
                    st.success("PDF generated successfully!")
                except Exception as e:
                    st.error(f"An error occurred while generating the PDF: {e}")
                    st.session_state.pdf_data = None

            if 'pdf_data' in st.session_state and st.session_state.pdf_data:
                st.download_button(
                    label="Download PDF Labels",
                    data=st.session_state.pdf_data,
                    file_name="book_labels.pdf",
                    mime="application/pdf",
                    key="pdf-download",
                )

    with st.expander("Debug Log"):
        st.download_button(
            label="Download Full Debug Log",
            data=st.session_state.log_capture_string.getvalue(),
            file_name="debug_log.txt",
            mime="text/plain",
        )


if __name__ == "__main__":
    main()