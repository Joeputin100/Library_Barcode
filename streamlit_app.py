import streamlit as st
import pandas as pd
from label_generator import generate_pdf_sheet
import re
import requests
from lxml import etree
import time
import json
import os

# --- Page Configuration ---
st.set_page_config(
    page_title="Barcode & QR Code Label Generator",
    page_icon="✨",
    layout="wide",
)

# --- Constants & Cache ---
SUGGESTION_FLAG = "🐒"
CACHE_FILE = "loc_cache.json"

# --- Caching Functions ---
def load_cache():
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, 'r') as f:
            return json.load(f)
    return {}

def save_cache(cache):
    with open(CACHE_FILE, 'w') as f:
        json.dump(cache, f, indent=4)

# --- Instruction Display Function ---
def show_instructions():
    # Custom CSS to change expander background color
    st.markdown(r"""
    <style>
    .st-expander, .st-expander:hover {
        background-color: #FFC0CB !important; /* Pink */
    }
    </style>
    """, unsafe_allow_html=True)

    with st.expander("How to Generate the CSV File from Atriuum on Android"):
        st.markdown("""1. Open Atriuum, login to your library, and tap on "Reports".""")