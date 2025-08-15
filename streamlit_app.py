import streamlit as st
import pandas as pd
import io
import logging
from datetime import datetime
import pytz
import re

from caching import load_cache, save_cache
from data_cleaning import (
    clean_title, capitalize_title_mla, clean_author, clean_call_number,
    clean_series_number, extract_year, SUGGESTION_FLAG
)
from api_calls import get_book_metadata_initial_pass, get_vertex_ai_classification_batch
from pdf_generation import generate_pdf_labels
from csv_importer import import_csv

# --- Logging Setup ---
log_capture_string = io.StringIO()
st_logger = logging.getLogger()
st_logger.setLevel(logging.DEBUG)
st_handler = logging.StreamHandler(log_capture_string)
st_handler.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
st_logger.addHandler(st_handler)

# --- Page Title ---
st.title("Atriuum Label Generator")
st.caption(f"Last updated: {datetime.now(pytz.timezone('US/Pacific')).strftime('%Y-%m-%d %H:%M:%S %Z%z')}")

# --- Main App ---


def main():
    st_logger.debug("Streamlit app main function started.")

    uploaded_file = st.file_uploader("Upload your Atriuum CSV Export", type="csv")

    if uploaded_file:
        if 'processed_df' not in st.session_state:
            st.session_state.processed_df = import_csv(uploaded_file)
            st.session_state.edited_rows = {}

        def on_edit():
            st.session_state.edited_rows.update(st.session_state.data_editor['edited_rows'])

        st.data_editor(
            st.session_state.processed_df,
            key="data_editor",
            on_change=on_edit,
            use_container_width=True,
            hide_index=True
        )

        if st.button("Process Data"):
            loc_cache = load_cache()

            with st.expander("Processing Report", expanded=True):
                st.write("Processing rows...")
                steps = ["Extracting data", "Enriching with Google Books", "Enriching with Library of Congress", "Classifying with Vertex AI", "Cleaning data"]
                progress_placeholders = {step: st.empty() for step in steps}

                def update_progress(step, status):
                    progress_placeholders[step].write(f"- {step}: {status}")

                for step in steps:
                    update_progress(step, "Pending")

                results = [{} for _ in range(len(st.session_state.processed_df))]
                unclassified_books_for_vertex_ai = []

                for i, row in st.session_state.processed_df.iterrows():
                    if i in st.session_state.edited_rows:
                        results[i] = row.to_dict()
                        continue

                    title = row.get('Title', '').strip()
                    author = row.get("Author", '').strip()
                    is_blank_row = not title and not author

                    problematic_books = [
                        ("The Genius Prince's Guide to Raising a Nation Out of Debt (Hey, How About Treason?), Vol. 5",
                         "Toba, Toru"),
                        ("The old man and the sea", "Hemingway, Ernest"),
                        ("Jack & Jill (Alex Cross)", "Patterson, James"),
                    ]
                    is_problematic_row = (title, author) in problematic_books

                    update_progress("Extracting data", f"Processing row {i+1}/{len(st.session_state.processed_df)}")
                    lc_meta = get_book_metadata_initial_pass(
                        title, author, loc_cache, is_blank=is_blank_row, is_problematic=is_problematic_row
                    )

                    if not lc_meta.get('volume_number'):
                        lc_meta['volume_number'] = clean_series_number(title)

                    if (not lc_meta.get('volume_number') and
                            any(c.lower() in ['manga', 'comic'] for c in lc_meta.get('genres', []))):
                        trailing_num_match = re.search(r'(\d+)$', title)
                        if trailing_num_match:
                            lc_meta['volume_number'] = trailing_num_match.group(1)

                    original_holding_barcode = row.get('Holdings Barcode', '').strip()
                    raw_original_call_number = row.get('Call Number', '').strip()
                    cleaned_original_call_number = clean_call_number(
                        raw_original_call_number, [], [], title="", is_original_data=True
                    )
                    original_series_name = row.get('Series Title', '').strip()
                    original_series_number = clean_series_number(row.get('Series Number', '').strip())
                    original_copyright_year = extract_year(row.get('Copyright', '').strip())
                    original_publication_date_year = extract_year(row.get('Publication Date', '').strip())

                    final_original_year = ""
                    if original_copyright_year:
                        final_original_year = original_copyright_year
                    elif original_publication_date_year:
                        final_original_year = original_publication_date_year

                    api_call_number = lc_meta.get('classification', '')
                    cleaned_call_number = clean_call_number(
                        api_call_number, lc_meta.get('genres', []), lc_meta.get('google_genres', []), title=title
                    )
                    mashed_series_name = (lc_meta.get('series_name') or '').strip()
                    mashed_volume_number = (lc_meta.get('volume_number') or '').strip()
                    mashed_publication_year = (lc_meta.get('publication_year') or '').strip()

                    current_call_number = cleaned_original_call_number if cleaned_original_call_number else (
                        SUGGESTION_FLAG + cleaned_call_number if cleaned_call_number else ""
                    )
                    current_series_name = original_series_name if original_series_name else (
                        SUGGESTION_FLAG + mashed_series_name if mashed_series_name else ''
                    )
                    current_series_number = original_series_number if original_series_number else (
                        SUGGESTION_FLAG + mashed_volume_number if mashed_volume_number else ''
                    )
                    current_publication_year = final_original_year if final_original_year else (
                        SUGGESTION_FLAG + mashed_publication_year if mashed_publication_year else ''
                    )

                    if not current_call_number or current_call_number == "UNKNOWN":
                        unclassified_books_for_vertex_ai.append({
                            'title': title,
                            'author': author,
                            'row_index': i,
                            'lc_meta': lc_meta
                        })

                    results[i] = {
                        'Title': capitalize_title_mla(clean_title(title)),
                        'Author': clean_author(author),
                        'Holdings Barcode': original_holding_barcode,
                        'Call Number': current_call_number,
                        'Series Info': current_series_name,
                        'Series Number': current_series_number,
                        'Copyright Year': current_publication_year,
                    }

                update_progress("Extracting data", "Done")

                if unclassified_books_for_vertex_ai:
                    update_progress("Classifying with Vertex AI", "In progress...")
                    BATCH_SIZE = 5
                    batches = [unclassified_books_for_vertex_ai[j:j + BATCH_SIZE]
                               for j in range(0, len(unclassified_books_for_vertex_ai), BATCH_SIZE)]

                    for batch in batches:
                        batch_classifications = get_vertex_ai_classification_batch(
                            batch, st.secrets["vertex_ai"], loc_cache
                        )

                        if not isinstance(batch_classifications, list):
                            continue

                        for book_data, vertex_ai_results in zip(batch, batch_classifications):
                            row_index = book_data['row_index']
                            lc_meta = book_data['lc_meta']
                            title = book_data['title']

                            for k, v in vertex_ai_results.items():
                                if v == "Unknown":
                                    vertex_ai_results[k] = ""

                            if vertex_ai_results.get('classification'):
                                lc_meta['classification'] = vertex_ai_results['classification']
                                if 'google_genres' not in lc_meta or not isinstance(lc_meta['google_genres'], list):
                                    lc_meta['google_genres'] = []
                                lc_meta['google_genres'].append(vertex_ai_results['classification'])

                            if vertex_ai_results.get('series_title'):
                                lc_meta['series_name'] = vertex_ai_results['series_title']
                            if vertex_ai_results.get('volume_number'):
                                lc_meta['volume_number'] = vertex_ai_results['volume_number']
                            if vertex_ai_results.get('copyright_year'):
                                lc_meta['publication_year'] = vertex_ai_results['copyright_year']

                            final_call_number_after_vertex_ai = clean_call_number(
                                lc_meta.get('classification', ''),
                                lc_meta.get('genres', []),
                                lc_meta.get('google_genres', []),
                                title=title
                            )

                            if (final_call_number_after_vertex_ai and
                                    not results[row_index]['Call Number'].replace(SUGGESTION_FLAG, '')):
                                results[row_index]['Call Number'] = SUGGESTION_FLAG + final_call_number_after_vertex_ai

                            if (lc_meta.get('series_name') and
                                    not results[row_index]['Series Info'].replace(SUGGESTION_FLAG, '')):
                                results[row_index]['Series Info'] = SUGGESTION_FLAG + lc_meta.get('series_name')

                            if (lc_meta.get('volume_number') and
                                    not results[row_index]['Series Number'].replace(SUGGESTION_FLAG, '')):
                                results[row_index]['Series Number'] = SUGGESTION_FLAG + str(lc_meta.get('volume_number'))

                            if (lc_meta.get('publication_year') and
                                    not results[row_index]['Copyright Year'].replace(SUGGESTION_FLAG, '')):
                                results[row_index]['Copyright Year'] = SUGGESTION_FLAG + str(
                                    lc_meta.get('publication_year')
                                )
                    update_progress("Classifying with Vertex AI", "Done")

                update_progress("Cleaning data", "In progress...")
                for i, row_data in enumerate(results):
                    final_series_name = row_data['Series Info']
                    final_series_number = row_data['Series Number']

                    results[i]['Series Info'] = final_series_name
                    results[i]['Series Number'] = final_series_number
                update_progress("Cleaning data", "Done")

                save_cache(loc_cache)

                st.write("Processing complete!")

                results_df = pd.DataFrame(results)
                st.session_state.processed_df = results_df

        if st.button("Save Manual Edits"):
            st.session_state.processed_df = st.session_state.data_editor
            st.success("Manual edits saved!")

        st.subheader("Generate PDF Labels")
        if st.button("Generate PDF"):
            try:
                pdf_output = generate_pdf_labels(st.session_state.processed_df)
                st.session_state.pdf_data = pdf_output
            except Exception as e:
                st.error(f"An error occurred while generating the PDF: {e}")
                st.session_state.pdf_data = None

        if 'pdf_data' in st.session_state and st.session_state.pdf_data:
            st.download_button(
                label="Download PDF Labels",
                data=st.session_state.pdf_data,
                file_name="book_labels.pdf",
                mime="application/pdf",
                key="pdf-download"
            )

    with st.expander("Debug Log"):
        st.download_button(
            label="Download Full Debug Log",
            data=log_capture_string.getvalue(),
            file_name="debug_log.txt",
            mime="text/plain"
        )


if __name__ == "__main__":
    main()
