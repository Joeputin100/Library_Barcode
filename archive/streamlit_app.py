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
from pymarc import MARCReader
from external_enricher import enrich_data

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
TABLE_ID = (
    "barcode.enriched_marc_records"  # Replace with your project and dataset if needed
)


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

def load_marc_file(uploaded_file):
    records = []
    try:
        # pymarc expects a file-like object in binary mode
        reader = MARCReader(uploaded_file.read(), to_unicode=True, force_utf8=True)
        for record in reader:
            # Extract relevant fields from MARC record
            # This is a simplified example, you'll need to expand this
            # based on the MARC fields you want to extract.
            title = record.title() if record.title() else None
            author = record.author() if record.author() else None
            isbn = record['020']['a'] if '020' in record and 'a' in record['020'] else None
            barcode = record['952']['p'] if '952'] in record and 'p' in record['952'] else None # Example for a local barcode field
            records.append({
                "title": title,
                "author": author,
                "isbn": isbn,
                "barcode": barcode,
                # Add more fields as needed
            })
        return pd.DataFrame(records)
    except Exception as e:
        st.error(f"Error processing MARC file: {e}")
        return pd.DataFrame()

def load_csv_file(uploaded_file):
    try:
        # pandas can directly read from an uploaded file object
        df = pd.read_csv(uploaded_file)
        return df
    except Exception as e:
        st.error(f"Error processing CSV file: {e}")
        return pd.DataFrame()


# --- Main App ---
def main():
    st_logger.info("Streamlit app main function started.")
    st_logger.info(
        f"Script MD5: {script_hash}, Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )

    if "current_step" not in st.session_state:
        st.session_state.current_step = "select_data_source"
    if "processed_df" not in st.session_state:
        st.session_state.processed_df = pd.DataFrame()

    if st.session_state.current_step == "select_data_source":
        st.header("Select Data Source")
        source_option = st.radio(
            "Choose your data source:",
            ("Load from BigQuery", "Upload MARC File", "Upload CSV File", "Select Existing MARC Database")
        )

        if source_option == "Load from BigQuery":
            if st.button("Load Data from BigQuery"):
                st.session_state.processed_df = load_data_from_bigquery()
                if not st.session_state.processed_df.empty:
                    st.session_state.current_step = "view_data"
                    st.rerun()
                else:
                    st.warning("No data loaded from BigQuery.")
        elif source_option == "Upload MARC File":
            uploaded_file = st.file_uploader("Upload a MARC file (.mrc, .marc)", type=["mrc", "marc"])
            if uploaded_file is not None:
                st.session_state.processed_df = load_marc_file(uploaded_file)
                if not st.session_state.processed_df.empty:
                    st.session_state.current_step = "view_data"
                    st.rerun()
                else:
                    st.error("Failed to load data from MARC file.")
        elif source_option == "Upload CSV File":
            uploaded_file = st.file_uploader("Upload a CSV file (.csv)", type=["csv"])
            if uploaded_file is not None:
                st.session_state.processed_df = load_csv_file(uploaded_file)
                if not st.session_state.processed_df.empty:
                    st.session_state.current_step = "view_data"
                    st.rerun()
                else:
                    st.error("Failed to load data from CSV file.")
        elif source_option == "Select Existing MARC Database":
            st.warning("This feature is not yet implemented. Please upload a file or load from BigQuery.")
            # Placeholder for future implementation
            # marc_files = list_marc_files_in_directory("data/")
            # if marc_files:
            #     selected_file = st.selectbox("Select a MARC file:", marc_files)
            #     if st.button("Load Selected MARC File"):
            #         st.session_state.processed_df = load_marc_file(selected_file)
            #         st.session_state.current_step = "view_data"
            #         st.rerun()
            # else:
            #     st.info("No MARC files found in the 'data/' directory.")

    elif st.session_state.current_step == "view_data":
        st.header("View Data") # Changed header to be more generic

        if not st.session_state.processed_df.empty:
            st.dataframe(
                st.session_state.processed_df,
                use_container_width=True,
                hide_index=True,
            )

            if st.button("Proceed to Edit Data"):
                st.session_state.current_step = "edit_data"
                st.rerun()
        else:
            st.warning("No data loaded. Please select a data source.")

    

    elif st.session_state.current_step == "edit_data":
        st.header("Edit Data")

        edited_df = st.data_editor(
            st.session_state.processed_df,
            key="data_editor",
            use_container_width=True,
            hide_index=True,
        )

        st.markdown("---") # Add a separator for clarity
        confirm_save = st.checkbox("Confirm changes before saving to database.")

        if st.button("Save Changes") and confirm_save:
            # Get the indices of the edited rows
            edited_row_indices = st.session_state["data_editor"]["edited_rows"].keys()

            if edited_row_indices:
                # Create a DataFrame with only the edited rows
                changed_rows_df = edited_df.loc[list(edited_row_indices)]

                # Here you would implement the logic to save the changes back to BigQuery.
                # For now, we will just show the edited dataframe.
                st.session_state.processed_df = edited_df # Update the session state with the full edited df
                st.success("Changes saved in session state.")

                # Generate MARC Export for changed rows
                marc_records = convert_df_to_marc(changed_rows_df)
                marc_file_path = "export_changed_rows.mrc"
                write_marc_file(marc_records, marc_file_path)
                with open(marc_file_path, "rb") as fp:
                    st.download_button(
                        label="Download MARC Export for Changed Rows",
                        data=fp,
                        file_name="export_changed_rows.mrc",
                        mime="application/marc",
                    )
                st.session_state.current_step = "generate_labels" # Move to generate labels step
                st.rerun()
            else:
                st.info("No changes detected to save.")
                st.session_state.current_step = "generate_labels" # Move to generate labels step
                st.rerun()

        elif st.button("Save Changes") and not confirm_save:
            st.warning("Please confirm changes by checking the box before saving.")

    elif st.session_state.current_step == "generate_labels":
        st.header("Generate Labels and Export")

        if st.button("Generate PDF Labels"):
            pdf_output = generate_pdf_labels(
                st.session_state.processed_df, "Library"
            )
            st.download_button(
                label="Download PDF Labels",
                data=pdf_output,
                file_name="book_labels.pdf",
                mime="application/pdf",
            )

        # The "Generate MARC Export" button for all data is now handled by the "Save Changes" button for changed rows.
        # if st.button("Generate MARC Export"):
        #     marc_records = convert_df_to_marc(st.session_state.processed_df)
        #     marc_file_path = "export.mrc"
        #     write_marc_file(marc_records, marc_file_path)
        #     with open(marc_file_path, "rb") as fp:
        #         st.download_button(
        #             label="Download MARC Export",
        #             data=fp,
        #             file_name="export.mrc",
        #             mime="application/marc",
        #         )

    with st.expander("Debug Log"):
        st.download_button(
            label="Download Full Debug Log",
            data=st.session_state.log_capture_string.getvalue(),
            file_name="debug_log.txt",
            mime="text/plain",
        )


if __name__ == "__main__":
    main()
