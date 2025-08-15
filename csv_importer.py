import pandas as pd
import streamlit as st
import hashlib


def import_csv(uploaded_file):
    uploaded_file_hash = hashlib.md5(uploaded_file.getvalue()).hexdigest()
    if 'processed_df' not in st.session_state or st.session_state.uploaded_file_hash is None or st.session_state.uploaded_file_hash != uploaded_file_hash:
        df = pd.read_csv(uploaded_file, encoding='latin1', dtype=str).fillna('')
        df.rename(columns={"Author's Name": "Author"}, inplace=True)
        st.session_state.processed_df = df
        st.session_state.uploaded_file_hash = uploaded_file_hash
        st.session_state.original_df = df.copy()
        st.session_state.pdf_data = None
    return st.session_state.processed_df
