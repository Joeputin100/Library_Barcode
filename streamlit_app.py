import streamlit as st
# import pandas as pd
# from label_generator import generate_pdf_sheet
# import re
# import requests
# from lxml import etree
# import time
# import json
# import os

# --- Page Configuration ---
st.set_page_config(
    page_title="Barcode & QR Code Label Generator",
    page_icon="✨",
    layout="wide",
)

st.title("Test - Step 1")

# --- Constants & Cache ---
# SUGGESTION_FLAG = "🐒"
# CACHE_FILE = "loc_cache.json"

# # --- Caching Functions ---
# def load_cache():
#     if os.path.exists(CACHE_FILE):
#         with open(CACHE_FILE, 'r') as f:
#             return json.load(f)
#     return {}

# def save_cache(cache):
#     with open(CACHE_FILE, 'w') as f:
#         json.dump(cache, f, indent=4)

# # --- Instruction Display Function ---
# def show_instructions():
#     with st.expander("How to Generate the CSV File from Atriuum on Android"):
#         st.markdown("""1. Open Atriuum, login to your library, and tap on "Reports".""")
#         st.image("images/image4.jpg") # D
#         st.markdown("""2. Select 'Shelf List' from the report options.""")
#         st.markdown("""3. Configure the report as follows: On the left side of the window, Change Data type to “Holdings Barcode.” Change Qualifier to “is greater than or equal to.” Enter Search Term {The first Holding Number in the range}. Tap Add New.""")
#         st.markdown("""5. Change Data type to “Holdings Barcode.” Change Qualifier to “is less than or equal to.” Enter Search Term {The last Holding Number in the range}. Tap Add New.""")
#         st.image("images/image3.jpg") # C
#         st.markdown("""8.  the red top bar, tap “Columns”.  Change Possible Columns to “Holdings Barcode”.  Tap ➡️. Do the same for “Call Number”, “Author’s name”, “Publication Date”, “Copyright”, “Series Volume”, “Series Title”, and “Title”.  If you tap on “Selected Columns”, you should see all 7 fields.  Tap “Generate Report”.""")
#         st.image("images/image5.jpg") # E
#         st.image("images/image1.jpg") # A
#         st.markdown("""9. Tap “Export Report as CSV”.""")
#         st.image("images/image7.jpg") # G
#         st.markdown("""10. Tap “Download Exported Report”.  Save as a file name with a .CSV extension.""")
#         st.markdown("""11. Locate the file in your device's 'Download' folder.""")
