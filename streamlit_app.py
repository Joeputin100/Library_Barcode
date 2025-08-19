import streamlit as st
import hashlib
import os
import pandas as pd
import io
import logging
from datetime import datetime
from google.cloud import bigquery
from marc_exporter import convert_df_to_marc, write_marc_file
from pdf_generation import generate_pdf_labels
from pdf_generation import generate_pdf_labels
from pdf_generation import generate_pdf_labels
from pdf_generation import generate_pdf_labels
from pdf_generation import generate_pdf_labels
from pdf_generation import generate_pdf_labels
from pdf_generation import generate_pdf_labels
from pdf_generation import generate_pdf_labels

# --- Logging Setup ---
if "log_capture_string" not in st.session_state:
    st.session_state.log_capture_string = io.StringIO()

st_logger = logging.getLogger()
st_logger.setLevel(logging.DEBUG)
st_handler = logging.StreamHandler(st.session_state.log_capture_string)
st_handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
# Remove existing handlers to prevent duplicate logs
if st_logger.handlers:
    st_logger.handlers = []
st_logger.addHandler(st_handler)

# --- Page Title ---
st.title("Atriuum Label Generator")
# Calculate MD5 hash of the current script file
script_path = os.path.abspath(__file__)
with open(script_path, "rb") as f:
    script_hash = hashlib.md5(f.read()).hexdigest()
    st.caption(f"Script MD5: {script_hash}")


# --- BigQuery Client ---
@st.cache_resource
def get_bigquery_client():
    # This will use the default credentials from the environment.
    # Make sure you have authenticated with `gcloud auth application-default login`
    return bigquery.Client()


client = get_bigquery_client()
TABLE_ID = "barcode.marc_records"  # Replace with your project and dataset if needed


# --- Helper Functions ---
def load_data_from_bigquery():
    query = f"SELECT * FROM `{TABLE_ID}`"
    try:
        df = client.query(query).to_dataframe()
        return df
    except Exception as e:
        st.error(f"Error loading data from BigQuery: {e}")
        return pd.DataFrame()


def update_row_in_bigquery(row):
    # This function needs to be implemented to update a single row in BigQuery.
    # It will be called after a user edits a row in the Streamlit app.
    pass


# --- Main App ---
def main():
    st_logger.info("Streamlit app main function started.")
    st_logger.info(
        f"Script MD5: {script_hash}, Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )

    if "current_step" not in st.session_state:
        st.session_state.current_step = "view_data"
    if "processed_df" not in st.session_state:
        st.session_state.processed_df = pd.DataFrame()

    if st.session_state.current_step == "view_data":
        st.header("MARC Records from BigQuery")

        if st.button("Reload Data from BigQuery"):
            st.session_state.processed_df = load_data_from_bigquery()

        if st.session_state.processed_df.empty:
            st.session_state.processed_df = load_data_from_bigquery()

        if not st.session_state.processed_df.empty:
            st.dataframe(
                st.session_state.processed_df, use_container_width=True, hide_index=True
            )

            if st.button("Proceed to Edit Data"):
                st.session_state.current_step = "edit_data"
                st.rerun()
        else:
            st.warning("No data loaded from BigQuery.")

    elif st.session_state.current_step == "edit_data":
        st.header("Edit Data")

        edited_df = st.data_editor(
            st.session_state.processed_df,
            key="data_editor",
            use_container_width=True,
            hide_index=True,
        )

        if st.button("Save Changes"):
            # Here you would implement the logic to save the changes back to BigQuery.
            # For now, we will just show the edited dataframe.
            st.session_state.processed_df = edited_df
            st.success("Changes saved in session state.")
            st.session_state.current_step = "generate_labels"
            st.rerun()

    elif st.session_state.current_step == "generate_labels":
        st.header("Generate Labels and Export")

        if st.button("Generate PDF Labels"):
            pdf_output = generate_pdf_labels(st.session_state.processed_df, "Library")
            st.download_button(
                label="Download PDF Labels",
                data=pdf_output,
                file_name="book_labels.pdf",
                mime="application/pdf",
            )

        if st.button("Generate MARC Export"):
            marc_records = convert_df_to_marc(st.session_state.processed_df)
            marc_file_path = "export.mrc"
            write_marc_file(marc_records, marc_file_path)
            with open(marc_file_path, "rb") as fp:
                st.download_button(
                    label="Download MARC Export",
                    data=fp,
                    file_name="export.mrc",
                    mime="application/marc",
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
