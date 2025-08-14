import streamlit as st
import pandas as pd
import re
import requests
from lxml import etree
import time
import json
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import xml.etree.ElementTree as ET
import logging
from datetime import datetime
import io
import hashlib
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
import barcode
from barcode.writer import ImageWriter
import qrcode
from reportlab.lib.utils import ImageReader
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from reportlab.platypus.flowables import KeepInFrame

# --- Logging Setup ---
log_capture_string = io.StringIO()
st_logger = logging.getLogger()
st_logger.setLevel(logging.DEBUG)
st_handler = logging.StreamHandler(log_capture_string)
st_handler.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
st_logger.addHandler(st_handler)
from bs4 import BeautifulSoup
import vertexai
import pytz
from vertexai.generative_models import GenerativeModel

# --- Page Title ---
st.title("Atriuum Label Generator")
st.caption(f"Last updated: {datetime.now(pytz.timezone('US/Pacific')).strftime('%Y-%m-%d %H:%M:%S %Z%z')}")

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

def clean_title(title):
    """Cleans title by moving leading articles to the end."""
    if not isinstance(title, str):
        return ""
    articles = ['The ', 'A ', 'An ']
    for article in articles:
        if title.startswith(article):
            return title[len(article):] + ", " + title[:len(article)-1]
    return title

def capitalize_title_mla(title):
    """Capitalizes a title according to MLA standards."""
    if not isinstance(title, str) or not title:
        return ""
    
    words = title.lower().split()
    # List of articles, prepositions, and conjunctions that should not be capitalized unless they are the first or last word.
    minor_words = ['a', 'an', 'the', 'and', 'but', 'or', 'for', 'nor', 'on', 'at', 'to', 'from', 'by', 'in', 'of', 'off', 'out', 'up', 'so', 'yet']
    
    capitalized_words = []
    for i, word in enumerate(words):
        if i == 0 or i == len(words) - 1 or word not in minor_words:
            capitalized_words.append(word.capitalize())
        else:
            capitalized_words.append(word)
            
    return " ".join(capitalized_words)

def clean_author(author):
    """Cleans author name to Last, First Middle."""
    if not isinstance(author, str):
        return ""
    parts = author.split(',')
    if len(parts) == 2:
        return f"{parts[0].strip()}, {parts[1].strip()}"
    return author

# --- Helper Functions ---
def get_book_metadata_google_books(title, author, cache):
    """Fetches book metadata from the Google Books API."""
    safe_title = re.sub(r'[^a-zA-Z0-9\s\.:]', '', title)
    safe_author = re.sub(r'[^a-zA-Z0-9\s,]', '', author)
    cache_key = f"google_{safe_title}|{safe_author}".lower()
    if cache_key in cache:
        st.write(f"DEBUG: Google Books cache hit for '{title}' by '{author}'.")
        return cache[cache_key]

    metadata = {'google_genres': [], 'classification': '', 'series_name': '', 'volume_number': '', 'publication_year': '', 'error': None}
    try:
        query = f'intitle:"{safe_title}"+inauthor:"{safe_author}"'
        url = f"https://www.googleapis.com/books/v1/volumes?q={query}&maxResults=1"
        st_logger.debug(f"Google Books query for '{title}' by '{author}': {url}")
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        data = response.json()
        st_logger.debug(f"Google Books raw response for '{title}' by '{author}': {response.text}")

        if "items" in data and data["items"]:
            item = data["items"][0]
            volume_info = item.get("volumeInfo", {})
            st_logger.debug(f"Google Books volume_info for '{title}': {volume_info}")
            st_logger.info(f"Google Books genres for '{title}': {volume_info.get('categories', [])}")

            if "categories" in volume_info:
                metadata['google_genres'].extend(volume_info["categories"])
            
            if "description" in volume_info:
                description = volume_info["description"]
                match = re.search(r'Subject: (.*?)(?:\n|$)', description, re.IGNORECASE)
                if match:
                    subjects = [s.strip() for s in match.group(1).split(',')]
                    metadata['google_genres'].extend(subjects)
                    st_logger.info(f"Google Books subjects for '{title}': {subjects}")

            if 'publishedDate' in volume_info:
                metadata['publication_year'] = extract_year(volume_info['publishedDate'])

            if 'seriesInfo' in volume_info:
                series_info = volume_info['seriesInfo']
                if 'bookDisplayNumber' in series_info:
                    metadata['volume_number'] = series_info['bookDisplayNumber']
                if 'series' in series_info and series_info['series']:
                    if 'title' in series_info['series'][0]:
                        metadata['series_name'] = series_info['series'][0]['title']

        cache[cache_key] = metadata
        return metadata

    except requests.exceptions.RequestException as e:
        metadata['error'] = f"Google Books API request failed: {e}"
    except Exception as e:
        metadata['error'] = f"An unexpected error occurred with Google Books API: {e}"
    return metadata

def get_vertex_ai_classification_batch(batch_books, vertex_ai_credentials):
    """Uses a Generative AI model on Vertex AI to classify a batch of books' genres."""
    temp_creds_path = "temp_creds.json"
    retry_delays = [10, 20, 30] # Increased delays for Vertex AI retries
    
    try:
        credentials_dict = dict(vertex_ai_credentials)
        credentials_json = json.dumps(credentials_dict)
        
        with open(temp_creds_path, "w") as f:
            f.write(credentials_json)

        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = temp_creds_path
        
        vertexai.init(project=credentials_dict["project_id"], location="us-central1")
        model = GenerativeModel("gemini-2.5-flash")
        
        # Construct a single prompt for the batch
        batch_prompts = []
        for book in batch_books:
            batch_prompts.append(f"Title: {book['title']}, Author: {book['author']}")
        
        full_prompt = (
            "For each book in the following list, provide its primary genre or Dewey Decimal classification, series title, volume number, and copyright year. "
            "If it's fiction, classify as 'FIC'. If non-fiction, provide a general Dewey Decimal category like '300' for Social Sciences, '500' for Science, etc. "
            "Provide the output as a JSON array of objects, where each object has 'title', 'author', 'classification', 'series_title', 'volume_number', and 'copyright_year' fields. "
            "If you cannot determine a value for a field, use an empty string ''.\n\n" +
            "Books:\n" + "\n".join(batch_prompts)
        )
        st_logger.debug(f"Vertex AI full prompt:\n```\n{full_prompt}\n```")
        
        for i in range(len(retry_delays) + 1):
            try:
                response = model.generate_content(full_prompt)
                # Attempt to parse JSON response
                response_text = response.text.strip()
                st_logger.debug(f"Vertex AI raw response:\n```\n{response_text}\n```")
                # Clean up markdown code block if present
                if response_text.startswith("```json") and response_text.endswith("```"):
                    response_text = response_text[7:-3].strip()
                
                classifications = json.loads(response_text)
                
                return classifications
            except Exception as e:
                if i < len(retry_delays):
                    st_logger.warning(f"Vertex AI batch call failed: {e}. Retrying in {retry_delays[i]} seconds...")
                    time.sleep(retry_delays[i])
                else:
                    st_logger.error(f"Vertex AI batch call failed after multiple retries: {e}")
                    return [] # Return empty list on failure
    finally:
        if os.path.exists(temp_creds_path):
            os.remove(temp_creds_path)

LCC_TO_DDC_MAP = {
    'AC': '080-089', 'AE': '030-039', 'AG': '030-039', 'AI': '050-059', 'AM': '060-069',
    'AS': '060-069', 'AY': '031-032', 'B': '100-109', 'BC': '160-169', 'BD': '110-119',
    'BF': '150-159', 'BH': '111, 701', 'BJ': '170-179', 'BL': '200-299', 'BM': '200-299',
    'BP': '200-299', 'BQ': '200-299', 'BR': '200-299', 'BS': '200-299', 'BT': '200-299',
    'BV': '200-299', 'BX': '200-299', 'CB': '909', 'CC': '930-939', 'CD': '091, 930',
    'CE': '529', 'CJ': '737', 'CN': '411, 930', 'CR': '929.6', 'CS': '929.1-929.5',
    'CT': '920-929', 'D': '909', 'DA': '940-999', 'DB': '940-999', 'DC': '940-999',
    'DD': '940-999', 'DE': '940-999', 'DF': '940-999', 'DG': '940-999', 'DH': '940-999',
    'DJ': '940-999', 'DK': '940-999', 'DL': '940-999', 'DP': '940-999', 'DQ': '940-999',
    'DR': '940-999', 'DS': '940-999', 'DT': '940-999', 'DU': '940-999', 'DX': '940-999',
    'E': '970-979', 'F': '973-999', 'G': '910-919', 'GB': '910.02', 'GC': '551.46',
    'GE': '333.7, 577', 'GF': '304.2, 301-309', 'GN': '301-309', 'GR': '398',
    'GT': '390-399', 'GV': '790-799', 'H': '300-309', 'HA': '310-319', 'HB': '330-339',
    'HC': '330-339', 'HD': '330-339', 'HE': '380-389', 'HF': '330-339', 'HG': '330-339',
    'HH': '330-339', 'HJ': '330-339', 'HM': '301-309', 'HN': '301-309', 'HQ': '301-309',
    'HS': '301-309', 'HT': '301-309', 'HV': '301-309', 'HX': '335', 'J': '320',
    'JA': '320.01', 'JC': '320.1-320.5', 'JF': '320-329', 'JJ': '320-329', 'JK': '320-329',
    'JL': '320-329', 'JN': '320-329', 'JQ': '320-329', 'JS': '320-329', 'JV': '320-329',
    'JX': '341-349', 'K': '340', 'KBM': '340-349', 'KBP': '340-349', 'KBR': '340-349',
    'KBS': '340-349', 'KBT': '340-349', 'KBU': '340-349', 'L': '370', 'LA': '370-375',
    'LB': '370-375', 'LC': '371-379', 'LD': '378', 'LE': '378', 'LF': '378', 'LG': '378',
    'M': '780', 'ML': '780.9', 'MT': '781-789', 'N': '700-709', 'NA': '720-729',
    'NB': '730-779', 'NC': '730-779', 'ND': '730-779', 'NE': '730-779', 'NK': '730-779',
    'NX': '730-779', 'P': '400-409', 'PA': '880-889', 'PB': '410-419', 'PC': '440-469',
    'PD': '430-439', 'PE': '420-429', 'PF': '430-439', 'PG': '891.7', 'PH': '494',
    'PJ': '892', 'PK': '891', 'PL': '895', 'PM': '497, 499', 'PN': '800-809',
    'PQ': '840-849', 'PR': '820-829', 'PS': '810-819', 'PT': '830-839', 'PZ': 'FIC',
    'Q': '500-509', 'QA': '510-519', 'QB': '520-599', 'QC': '520-599', 'QD': '520-599',
    'QE': '520-599', 'QH': '520-599', 'QK': '520-599', 'QL': '520-599', 'QM': '520-599',
    'QP': '520-599', 'QR': '520-599', 'R': '610', 'RA': '610-619', 'RB': '610-619',
    'RC': '610-619', 'RD': '610-619', 'RE': '610-619', 'RF': '610-619', 'RG': '610-619',
    'RJ': '610-619', 'RK': '610-619', 'RL': '610-619', 'RM': '610-619', 'RS': '610-619',
    'RT': '610-619', 'RV': '610-619', 'RX': '610-619', 'RZ': '610-619', 'S': '630',
    'SB': '630-639', 'SD': '630-639', 'SF': '630-639', 'SH': '630-639', 'SK': '630-639',
    'T': '600-609', 'TA': '620-699', 'TC': '620-699', 'TD': '620-699', 'TE': '620-699',
    'TF': '620-699', 'TG': '620-699', 'TH': '620-699', 'TJ': '620-699', 'TK': '620-699',
    'TL': '620-699', 'TN': '620-699', 'TP': '620-699', 'TR': '620-699', 'TS': '620-699',
    'TT': '620-699', 'TX': '620-699', 'U': '355', 'UA': '355-359', 'UB': '355-359',
    'UC': '355-359', 'UD': '355-359', 'UE': '355-359', 'UF': '355-359', 'UG': '355-359',
    'UH': '355-359', 'V': '359', 'VM': '623', 'Z': '010-029'
}

def lcc_to_ddc(lcc):
    """Converts an LCC call number to a DDC range or 'FIC'."""
    if not isinstance(lcc, str) or not lcc:
        return ""

    if lcc == "FIC":
        return "FIC"

    # Check for fiction-heavy classes first
    if lcc.startswith(('PZ', 'PQ', 'PR', 'PS', 'PT')):
        return "FIC"

    # General mapping
    for prefix, ddc_range in LCC_TO_DDC_MAP.items():
        if lcc.startswith(prefix):
            return ddc_range.split('-')[0].strip()
            
    return "" # Return empty if no match found

def clean_call_number(call_num_str, genres, google_genres=None, title="", is_original_data=False):
    st_logger.debug(f"clean_call_number input: call_num_str='{call_num_str}', genres={genres}, google_genres={google_genres}, title='{title}', is_original_data={is_original_data}")
    if google_genres is None:
        google_genres = []
        
    if not isinstance(call_num_str, str):
        st_logger.debug(f"clean_call_number returning empty string for non-string input: {call_num_str}")
        return "" # Default for non-string input

    cleaned = call_num_str.strip()
    # Only remove suggestion flag if it's not original data
    if not is_original_data:
        cleaned = cleaned.lstrip(SUGGESTION_FLAG)

    # Prioritize Google Books categories and other genre lists for FIC
    fiction_keywords_all = ["fiction", "fantasy", "science fiction", "thriller", "mystery", "romance", "horror", "novel", "stories", "a novel", "young adult fiction", "historical fiction", "literary fiction"]
    if any(g.lower() in fiction_keywords_all for g in google_genres) or \
       any(genre.lower() in fiction_keywords_all for genre in genres) or \
       any(keyword in title.lower() for keyword in fiction_keywords_all):
        st_logger.debug(f"clean_call_number returning FIC based on genre/title keywords: {cleaned}")
        return "FIC"

    # Check for LCC
    ddc_from_lcc = lcc_to_ddc(cleaned)
    if ddc_from_lcc:
        st_logger.debug(f"clean_call_number returning from LCC: {ddc_from_lcc}")
        return ddc_from_lcc

    # Strip common non-numeric characters from Dewey-like numbers
    cleaned = re.sub(r'[^a-zA-Z0-9\s\.:]', '', cleaned).strip()

    # If the cleaned string is a known non-numeric genre from Vertex AI, map to FIC
    # This catches cases where Vertex AI directly returns a genre name
    if cleaned.lower() in ["fantasy", "science fiction", "thriller", "mystery", "romance", "horror", "novel", "fiction", "young adult fiction", "historical fiction", "literary fiction"]:
        st_logger.debug(f"clean_call_number returning FIC based on cleaned string match: {cleaned}")
        return "FIC"

    # Check for explicit "FIC" or valid Dewey Decimal from any source
    if cleaned.upper().startswith("FIC"):
        st_logger.debug(f"clean_call_number returning FIC based on explicit FIC: {cleaned}")
        return "FIC"
    
    # Strict check for Dewey Decimal Number format (3 digits, optional decimal and more digits)
    if re.match(r'^\d{3}(\.\d+)?$', cleaned):
        st_logger.debug(f"clean_call_number returning Dewey Decimal: {cleaned}")
        return cleaned
    
    # If it's a number but not 3 digits, still return it (e.g., from LOC 050 field like "PS3515.E37"), keep it as is
    # This allows for LC call numbers to pass through if they are numeric-like
    if re.match(r'^[A-Z]{1,3}\d+(\.\d+)?$', cleaned) or re.match(r'^\d+(\.\d+)?$', cleaned):
        st_logger.debug(f"clean_call_number returning LC-like or numeric: {cleaned}")
        return cleaned

    # If none of the above conditions are met, it's an invalid format for a call number
    st_logger.debug(f"clean_call_number returning empty string for invalid format: {cleaned}")
    return ""


def get_book_metadata_initial_pass(title, author, cache, is_blank=False, is_problematic=False):
    st_logger.debug(f"Entering get_book_metadata_initial_pass for: {title}")
    safe_title = re.sub(r'[^a-zA-Z0-9\s\.:]', '', title)
    safe_author = re.sub(r'[^a-zA-Z0-9\s,]', '', author)

    metadata = {'classification': '', 'series_name': '', 'volume_number': '', 'publication_year': '', 'genres': [], 'google_genres': [], 'error': None}

    google_meta = get_book_metadata_google_books(title, author, cache)
    metadata.update(google_meta)

    loc_cache_key = f"loc_{safe_title}|{safe_author}".lower()
    if loc_cache_key in cache and 'series_name' in cache[loc_cache_key] and 'publication_year' in cache[loc_cache_key]:
        st_logger.debug(f"LOC cache hit for '{title}' by '{author}'.")
        cached_loc_meta = cache[loc_cache_key]
        metadata.update(cached_loc_meta)
    else:
        base_url = "http://lx2.loc.gov:210/LCDB"
        query = f'bath.title="{safe_title}" and bath.author="{safe_author}"'
        params = {"version": "1.1", "operation": "searchRetrieve", "query": query, "maximumRecords": "1", "recordSchema": "marcxml"}
        st_logger.debug(f"LOC query for '{title}' by '{author}': {base_url}?{requests.compat.urlencode(params)}")
        
        retry_delays = [5, 15, 30]
        for i in range(len(retry_delays) + 1):
            try:
                response = requests.get(base_url, params=params, timeout=20)
                response.raise_for_status()
                st_logger.debug(f"LOC raw response for '{title}' by '{author}':\n```xml\n{response.content.decode('utf-8')}\n```")
                root = etree.fromstring(response.content)
                ns_diag = {'diag': 'http://www.loc.gov/zing/srw/diagnostic/'}
                error_message = root.find('.//diag:message', ns_diag)
                if error_message is not None:
                    metadata['error'] = f"LOC API Error: {error_message.text}"
                else:
                    ns_marc = {'marc': 'http://www.loc.gov/MARC21/slim'}
                    classification_node = root.find('.//marc:datafield[@tag="082"]/marc:subfield[@code="a"]', ns_marc)
                    if classification_node is not None: metadata['classification'] = classification_node.text.strip()
                    series_node = root.find('.//marc:datafield[@tag="490"]/marc:subfield[@code="a"]', ns_marc)
                    if series_node is not None: metadata['series_name'] = series_node.text.strip().rstrip(' ;')
                    volume_node = root.find('.//marc:datafield[@tag="490"]/marc:subfield[@code="v"]', ns_marc)
                    if volume_node is not None: metadata['volume_number'] = volume_node.text.strip()
                    pub_year_node = root.find('.//marc:datafield[@tag="264"]/marc:subfield[@code="c"]', ns_marc)
                    if pub_year_node is None:
                        pub_year_node = root.find('.//marc:datafield[@tag="260"]/marc:subfield[@code="c"]', ns_marc)
                    if pub_year_node is not None and pub_year_node.text:
                        years = re.findall(r'(1[7-9]\d{2}|20\d{2})', pub_year_node.text)
                        if years: metadata['publication_year'] = str(min([int(y) for y in years]))
                    genre_nodes = root.findall('.//marc:datafield[@tag="655"]/marc:subfield[@code="a"]', ns_marc)
                    if genre_nodes:
                        metadata['genres'] = [g.text.strip().rstrip('.') for g in genre_nodes]
                        st_logger.info(f"LOC genres for '{title}': {metadata['genres']}")

                    # Only cache successful LOC lookups
                    if not metadata['error']:
                        cache[loc_cache_key] = metadata
                break # Exit retry loop on success
            except requests.exceptions.RequestException as e:
                if i < len(retry_delays):
                    st_logger.warning(f"LOC API call failed for {title}. Retrying in {retry_delays[i]}s...")
                    time.sleep(retry_delays[i])
                    continue
                metadata['error'] = f"LOC API request failed after retries: {e}"
                st_logger.error(f"LOC failed for {title}, returning what we have.")
            except Exception as e:
                metadata['error'] = f"An unexpected error occurred with LOC API: {e}"
                st_logger.error(f"Unexpected LOC error for {title}, returning what we have.")
                break

    return metadata

def clean_series_number(series_num_str):
    """Cleans and converts series number strings to digits.
    Removes brackets, periods, descriptive words, and converts written numbers to digits.
    Handles 'of X' phrases.
    """
    if not isinstance(series_num_str, str):
        return ""

    cleaned = series_num_str.strip().lower()

    # Remove 'of X' phrases (e.g., 'book 3 of 6' -> 'book 3')
    cleaned = re.sub(r'\s*of\s*\d+', '', cleaned)

    # Remove brackets, periods, and common descriptive words
    cleaned = re.sub(r'[\[\]\.]', '', cleaned) # Remove brackets and periods
    cleaned = re.sub(r'\b(book|bk|bk\.|volume|vol|part|pt|v|no|number)\b', '', cleaned) # Remove descriptive words
    cleaned = cleaned.strip()

    # Convert written numbers to digits
    word_to_num = {
        'one': '1', 'two': '2', 'three': '3', 'four': '4', 'five': '5',
        'six': '6', 'seven': '7', 'eight': '8', 'nine': '9', 'ten': '10',
        'eleven': '11', 'twelve': '12', 'thirteen': '13', 'fourteen': '14', 'fifteen': '15',
        'sixteen': '16', 'seventeen': '17', 'eighteen': '18', 'nineteen': '19', 'twenty': '20'
    }
    for word, digit in word_to_num.items():
        cleaned = cleaned.replace(word, digit)

    # Extract the first sequence of digits found
    match = re.search(r'\d+', cleaned)
    if match:
        return match.group(0)
    return ""

def generate_pdf_labels(df):
    st_logger.info("--- generate_pdf_labels function called ---")
    st_logger.debug(f"Generating PDF for {len(df)} rows.")
    if len(df) > 0:
        st_logger.debug(f"First row of data:\n{df.head(1).to_string()}")
    
    book_data_list = df.to_dict('records')
    pdf_data = generate_pdf_sheet(book_data_list)
    
    st_logger.debug(f"Generated PDF size: {len(pdf_data)} bytes.")
    return pdf_data

# Avery 5160 label dimensions (approximate, for layout)
LABEL_WIDTH = 2.625 * inch
LABEL_HEIGHT = 1.0 * inch
LABELS_PER_SHEET_WIDTH = 3
LABELS_PER_SHEET_HEIGHT = 10
PAGE_WIDTH, PAGE_HEIGHT = letter # 8.5 x 11 inches

# Margins for Avery 5160 (standard, adjust if needed)
LEFT_MARGIN = 0.1875 * inch
TOP_MARGIN = 0.5 * inch
HORIZONTAL_SPACING = 0.125 * inch # Space between labels horizontally
VERTICAL_SPACING = 0.0 * inch # Space between labels vertically (they touch)

# Debugging grid spacing
GRID_SPACING = 0.1 * inch

def pad_inventory_number(inventory_num):
    """Pads the inventory number with leading zeros to 6 digits."""
    return str(inventory_num).zfill(6)

def generate_barcode(inventory_num):
    """Generates a Code 128 barcode image for the given inventory number."""
    padded_num = pad_inventory_number(inventory_num)
    EAN = barcode.get('code128', padded_num, writer=ImageWriter())
    # Save to a BytesIO object to avoid writing to disk
    buffer = io.BytesIO()
    EAN.write(buffer)
    buffer.seek(0)
    return ImageReader(buffer)

def generate_qrcode(inventory_num):
    """Generates a QR code image for the given inventory number."""
    padded_num = pad_inventory_number(inventory_num)
    qr = qrcode.QRCode(
        version=1,
        error_correction=qrcode.constants.ERROR_CORRECT_L,
        box_size=10,
        border=4,
    )
    qr.add_data(padded_num)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    # Save to a BytesIO object
    buffer = io.BytesIO()
    img.save(buffer, format="PNG")
    buffer.seek(0)
    return ImageReader(buffer)

def _fit_text_to_box(c, text_lines, font_name, max_width, max_height, initial_font_size=10, min_font_size=5, alignment=TA_LEFT):
    """
    Finds the largest font size that allows all text_lines to fit within max_width and max_height.
    Returns the optimal font size and the calculated height of the text block.
    """
    styles = getSampleStyleSheet()
    style = styles['Normal']
    style.fontName = font_name
    style.alignment = alignment
    
    optimal_font_size = min_font_size
    text_block_height = 0

    for font_size in range(initial_font_size, min_font_size - 1, -1):
        style.fontSize = font_size
        current_text_height = 0
        for line in text_lines:
            p = Paragraph(line, style)
            # Calculate wrapped height for each paragraph
            width, height = p.wrapOn(c, max_width, max_height) # c is needed for wrapOn
            current_text_height += height + (0.05 * inch) # Add line spacing

        if current_text_height <= max_height:
            optimal_font_size = font_size
            text_block_height = current_text_height
            break
    else: # If loop completes without breaking, no font size fits, use min_font_size
        optimal_font_size = min_font_size
        style.fontSize = min_font_size
        text_block_height = 0
        for line in text_lines:
            p = Paragraph(line, style)
            width, height = p.wrapOn(c, max_width, max_height)
            text_block_height += height + (0.05 * inch)

    return optimal_font_size, text_block_height

def create_label(c, x, y, book_data, label_type):
    """
    Renders content for a single label on the PDF canvas.
    c: reportlab canvas object
    x, y: bottom-left coordinates of the label
    book_data: dictionary containing book information
    label_type: 1, 2, 3, or 4
    """
    title = book_data.get('Title', '')
    authors = book_data.get("Author", '')
    publication_year = book_data.get('Copyright Year', '')
    series_name = book_data.get('Series Info', '')
    series_number = book_data.get('Series Number', '')
    dewey_number = book_data.get('Call Number', '')
    inventory_number = pad_inventory_number(book_data.get('Holdings Barcode', ''))

    # Truncate title and series_name for Label 1 if they are too long
    if label_type == 1 or label_type == 2:
        if len(title) > 26:
            title = title[:23] + '...'
        if series_name and len(series_name) > 26:
            series_name = series_name[:23] + '...'

    if label_type == 1:
        # Label 1: title, authors, publication year, series, Dewey, inventory number
        text_lines = [
            f"{title} - {authors} - {publication_year}",
        ]
        if series_name:
            text_lines.append(f"{series_name} {series_number}" if series_number else series_name)
        text_lines.append(f"<b>{inventory_number}</b> - <b>{dewey_number}</b>") # Bold inventory and Dewey numbers

        # Dynamic font sizing for Label 1
        max_text_width = LABEL_WIDTH - 10 # 5 units margin on each side
        max_text_height = LABEL_HEIGHT - 10 # 5 units margin on top/bottom
        
        styles = getSampleStyleSheet()
        style = styles['Normal']
        style.fontName = 'Courier'
        style.leading = style.fontSize * 1.1 # Adjusted line spacing

        current_y = y + LABEL_HEIGHT - 5 # Start near top of label

        # Adjust initial Y position for non-series books
        if not series_name:
            current_y -= (1.5 * GRID_SPACING) # Move all lines down 1.5 blue box heights

        for line_idx, line_text in enumerate(text_lines):
            # For the third line (index 2) in series books, move down by 1 blue box height
            if series_name and line_idx == 2:
                current_y -= (1 * GRID_SPACING)

            # Dynamically adjust font size to fit within max_text_width without wrapping
            optimal_font_size_line = 18 # Start with a large font size
            # Use c.stringWidth for precise width calculation without word wrap
            while c.stringWidth(line_text, 'Courier', optimal_font_size_line) > max_text_width and optimal_font_size_line > 5:
                optimal_font_size_line -= 0.5

            style.fontSize = optimal_font_size_line
            # For the last line (inventory and dewey), use bold font
            if line_idx == len(text_lines) - 1:
                style.fontName = 'Courier-Bold'
            else:
                style.fontName = 'Courier'
            
            p = Paragraph(line_text, style)
            # Use a very large height to prevent Paragraph from wrapping vertically,
            # as we've already ensured horizontal fit with optimal_font_size_line
            width, height = p.wrapOn(c, max_text_width, LABEL_HEIGHT * 2) 
            current_y -= height # Move down for current line
            p.drawOn(c, x + 5, current_y) # Draw from top down
            current_y -= (0.02 * inch) # Reduced line spacing

    elif label_type == 2:
        # Label 2: title, author, series, inventory number, QR code (half label)
        qr_code_size = LABEL_HEIGHT - GRID_SPACING # Crop by half a blue box on all sides
        qr_code_x = x + LABEL_WIDTH - qr_code_size # Flush with the right side of the label
        qr_code_y = y + (LABEL_HEIGHT - qr_code_size) / 2 # Vertically centered

        qr_image = generate_qrcode(inventory_number)
        c.drawImage(qr_image, qr_code_x, qr_code_y, width=qr_code_size, height=qr_code_size)
        

        text_lines = [
            title,
            authors.split(',')[0] if authors else '',
        ]
        if series_name:
            text_lines.append(f"{series_name} #{series_number}" if series_number else series_name)
        text_lines.append(inventory_number)

        # Calculate text area width (left of QR code) and height
        max_text_width = LABEL_WIDTH - qr_code_size - 10 # 10 units for margins
        max_text_height = LABEL_HEIGHT - 10 # 5 units margin on top/bottom

        styles = getSampleStyleSheet()
        style = styles['Normal']
        style.fontName = 'Courier'
        style.leading = 1.5 # Increased line spacing

        # Calculate vertical start position to center text block
        # This will be adjusted per line for independent sizing
        # Calculate total text height for vertical centering
        total_text_height = 0
        for line in text_lines:
            p = Paragraph(line, style)
            width, height = p.wrapOn(c, max_text_width, max_text_height) # Use max_text_height to allow wrapping
            total_text_height += height + (0.05 * inch) # Add line spacing

        # Adjust initial Y position to vertically center the entire text block
        current_y = y + (LABEL_HEIGHT - total_text_height) / 2 + total_text_height # Start from the top of the text block

        for line_idx, line_text in enumerate(text_lines):
            # Apply specific offsets
            line_offset_y = 0
            if line_idx == 0: # Row 1 (index 0)
                line_offset_y = 3 * GRID_SPACING # Move up
            elif line_idx == 1: # Row 2 (index 1)
                line_offset_y = 2.5 * GRID_SPACING # Move up
            elif line_idx == 3: # Row 4 (index 3)
                line_offset_y = -1.25 * GRID_SPACING # Move down

            # Dynamically adjust font size to fit within max_text_width without wrapping
            optimal_font_size_line = 18 # Start with a large font size
            while c.stringWidth(line_text, 'Courier', optimal_font_size_line) > max_text_width and optimal_font_size_line > 5:
                optimal_font_size_line -= 0.5

            style.fontSize = optimal_font_size_line
            p = Paragraph(line_text, style)
            width, height = p.wrapOn(c, max_text_width, max_text_height) # Allow wrapping within max_text_height
            current_y -= height # Move down for current line
            p.drawOn(c, x + 5, current_y + line_offset_y) # Draw from top down, apply offset
            current_y -= (0.05 * inch) # Increased line spacing

    elif label_type == 3:
        # Label 3 (Spine Label): Dewey, author (3 letters), year, inventory number (centered)
        c.setFont('Courier-Bold', 10)
        lines = [
            dewey_number,
            authors[:3].upper() if authors else '',
            str(publication_year),
            inventory_number
        ]
        
        # Calculate vertical position for centered text
        line_height = 12 # Approximate line height for font size 10
        total_text_height = len(lines) * line_height
        # Corrected vertical centering: y + (LABEL_HEIGHT - total_text_height) / 2
        start_y = y + (LABEL_HEIGHT - total_text_height) / 2 + total_text_height - (line_height * 0.8) # Adjust for baseline

        # Add giant spine label ID
        b_text = book_data.get('spine_label_id', 'B') # Use selected spine label ID
        # Calculate font size to make 'B' flush with label width
        # Start with a large font size and decrease until it fits
        b_font_size = 100 # Arbitrary large starting size
        while c.stringWidth(b_text, 'Helvetica-Bold', b_font_size) > LABEL_WIDTH and b_font_size > 10:
            b_font_size -= 1
        b_font_size *= 0.9 # Reduce size by 10%

        c.setFont('Helvetica-Bold', b_font_size)
        b_text_width = c.stringWidth(b_text, 'Helvetica-Bold', b_font_size)
        # Align 'B' flush with the right side of the label
        b_x = x + LABEL_WIDTH - b_text_width
        # Position 'B' vertically to be roughly centered, considering its height
        # This might need fine-tuning based on visual inspection
        b_y = y + (LABEL_HEIGHT - b_font_size * 0.8) / 2 + (0.5 * GRID_SPACING) # Approximate vertical centering, moved up by 0.5 blue box heights

        c.drawString(b_x, b_y, b_text)

        # Original text lines for Label 3
        c.setFont('Courier-Bold', 10)
        lines = [
            dewey_number,
            authors[:3].upper() if authors else '',
            str(publication_year),
            inventory_number
        ]
        
        # Calculate vertical position for centered text
        line_height = 12 # Approximate line height for font size 10
        total_text_height = len(lines) * line_height
        # Corrected vertical centering: y + (LABEL_HEIGHT - total_text_height) / 2
        start_y = y + (LABEL_HEIGHT - total_text_height) / 2 + total_text_height - (line_height * 0.8) # Adjust for baseline

        for i, line in enumerate(lines):
            text_width = c.stringWidth(line, 'Courier-Bold', 10)
            c.drawString(x + (LABEL_WIDTH - text_width) / 2, start_y - (i * line_height), line)

    elif label_type == 4:
        # Label 4: title, author, series, inventory number (text + barcode - 75% label)
        barcode_height = 7 * GRID_SPACING # Set barcode height to 7 blue box heights
        barcode_width = barcode_height * ((LABEL_WIDTH * 0.8 - 4 * GRID_SPACING) / (LABEL_HEIGHT * 0.6)) # Maintain aspect ratio
        barcode_x = x + LABEL_WIDTH - barcode_width # Flush with the right side of the label
        barcode_y = y # Flush with the bottom of the label

        barcode_image = generate_barcode(inventory_number)
        c.drawImage(barcode_image, barcode_x, barcode_y, width=barcode_width, height=barcode_height)
        

        # Text above barcode (Title, Author)
        text_above_barcode_lines = [
            f"{title} by {authors.split(',')[0] if authors else ''}", # Combined title and author
        ]
        # Define max_text_above_width before its usage
        max_text_above_width = LABEL_WIDTH - 10 # 10 units for margins
        max_text_above_height = (y + LABEL_HEIGHT) - (barcode_y + barcode_height) - 5 # Space from top of label to top of barcode

        optimal_font_size_above, text_block_height_above = _fit_text_to_box(c, text_above_barcode_lines, 'Courier', max_text_above_width, max_text_above_height, initial_font_size=10, alignment=TA_CENTER)
        
        styles = getSampleStyleSheet()
        style_above = styles['Normal']
        style_above.fontName = 'Courier'
        style_above.fontSize = optimal_font_size_above
        style_above.leading = optimal_font_size_above * 1.2 # Line spacing
        style_above.alignment = TA_CENTER # Center text horizontally

        current_y_above = y + LABEL_HEIGHT - 5 # Start near top of label
        for line in text_above_barcode_lines:
            p = Paragraph(line, style_above)
            width, height = p.wrapOn(c, max_text_above_width, max_text_above_height) # Wrap text within the box
            p.drawOn(c, x + 5, current_y_above - height) # Draw from top down
            current_y_above -= (height + (0.05 * inch)) # Move down for current line

        # Text to the left of barcode (Series)
        text_left_barcode_lines = []
        if series_name:
            text_left_barcode_lines.append(f"Vol. {series_number}" if series_number else series_name)

        max_text_left_width = barcode_x - (x + 5) - 5 # Space to the left of barcode
        max_text_left_height = barcode_height # Height of barcode area

        # Calculate optimal font size for the series number, reducing for longer numbers
        optimal_font_size_left = 10 # Initial font size
        if series_number and len(series_number) > 1:
            optimal_font_size_left -= (len(series_number) - 1) # Reduce by 1pt for each char after the first
        optimal_font_size_left = max(optimal_font_size_left, 5) # Ensure minimum font size

        optimal_font_size_left, text_block_height_left = _fit_text_to_box(c, text_left_barcode_lines, 'Courier', max_text_left_width, max_text_left_height, initial_font_size=optimal_font_size_left, alignment=TA_LEFT)

        style_left = styles['Normal']
        style_left.fontName = 'Courier'
        style_left.fontSize = optimal_font_size_left
        style_left.leading = optimal_font_size_left * 1.2 # Line spacing
        style_left.alignment = TA_LEFT

        # Position and rotate text to the left of the barcode
        if text_left_barcode_lines:
            c.saveState()
            # Translate to the bottom-left of the text area, then rotate
            text_origin_x = x + 5 # Start 5 units from left edge
            # Move down by 1 GRID_SPACING for every character after the first in series_number
            vertical_offset = 0
            if series_number and len(series_number) > 1:
                vertical_offset = (len(series_number) - 1) * GRID_SPACING
            text_origin_y = y + (LABEL_HEIGHT - text_block_height_left) / 2 + text_block_height_left - (0.1 * inch) - (3.5 * GRID_SPACING)
            
            c.translate(text_origin_x, text_origin_y)
            c.rotate(90) # Rotate 90 degrees counter-clockwise
            
            # Draw text after rotation and translation
            current_rotated_y = 0 # Relative to new origin
            for line in text_left_barcode_lines:
                p = Paragraph(line, style_left)
                # For rotated text, width and height parameters are swapped
                width, height = p.wrapOn(c, max_text_left_width, max_text_left_height) 
                current_rotated_y -= height # Move down for current line
                p.drawOn(c, 0, current_rotated_y) # Draw from new origin
                current_rotated_y -= (0.05 * inch) # Add line spacing
            c.restoreState()

def generate_pdf_sheet(book_data_list):
    """Generates a PDF with multiple sheets of Avery 5160 labels."""
    buffer = io.BytesIO()
    c = canvas.Canvas(buffer, pagesize=letter)

    # Limit to first 30 labels for debugging
    book_data_list_debug = book_data_list

    label_count = 0
    for book_data in book_data_list_debug:
        for label_type in range(1, 5): # Generate 4 labels for each book
            row = (label_count // LABELS_PER_SHEET_WIDTH) % LABELS_PER_SHEET_HEIGHT
            col = label_count % LABELS_PER_SHEET_WIDTH

            x_pos = LEFT_MARGIN + col * (LABEL_WIDTH + HORIZONTAL_SPACING)
            y_pos = PAGE_HEIGHT - TOP_MARGIN - (row + 1) * (LABEL_HEIGHT + VERTICAL_SPACING)

            

            # Draw dotted lines for label edges (perimeter of each label cell) with rounded corners
            c.setDash(1, 2) # Dotted line: 1 unit on, 2 units off
            c.setStrokeColorRGB(0, 0, 0) # Black color
            c.setLineWidth(0.5) # Line thickness
            c.roundRect(x_pos, y_pos, LABEL_WIDTH, LABEL_HEIGHT, 5) # 5 units for corner radius

            # Draw solid lines for buffer spaces (between labels, not overlapping dotted lines)
            c.setDash() # Solid line
            c.setStrokeColorRGB(0, 0, 0) # Black color
            c.setLineWidth(0.5) # Line thickness
            
            # Vertical solid lines in the horizontal spacing area
            if col < LABELS_PER_SHEET_WIDTH - 1:
                # Draw a single solid line in the middle of the horizontal spacing
                c.line(x_pos + LABEL_WIDTH + HORIZONTAL_SPACING / 2, y_pos, x_pos + LABEL_WIDTH + HORIZONTAL_SPACING / 2, y_pos + LABEL_HEIGHT)

            # Horizontal solid lines in the vertical spacing area
            if row < LABELS_PER_SHEET_HEIGHT - 1:
                # Draw a single solid line in the middle of the vertical spacing
                c.line(x_pos, y_pos - VERTICAL_SPACING / 2, x_pos + LABEL_WIDTH, y_pos - VERTICAL_SPACING / 2)

            create_label(c, x_pos, y_pos, book_data, label_type)
            label_count += 1

            if label_count % (LABELS_PER_SHEET_WIDTH * LABELS_PER_SHEET_HEIGHT) == 0:
                c.showPage() # Start a new page after a full sheet
                c.setFont('Courier', 8) # Reset font for new page

    c.save()
    buffer.seek(0)
    return buffer.getvalue()

def extract_year(date_string):
    """Extracts the first 4-digit number from a string, assuming it's a year."""
    if isinstance(date_string, str):
        # Regex to find a 4-digit year, ignoring surrounding brackets, c, or ©
        match = re.search(r'[\(\)\[©c]?(\d{4})[\)\]]?', date_string)
        if match:
            return match.group(1)
    return ""

def main():
    st_logger.debug("Streamlit app main function started.")
    try:
        with open(__file__, 'rb') as f:
            file_hash = hashlib.md5(f.read()).hexdigest()
            st_logger.info(f"Running streamlit_app.py version: {file_hash}")
    except Exception as e:
        st_logger.warning(f"Could not calculate file hash: {e}")

    uploaded_file = st.file_uploader("Upload your Atriuum CSV Export", type="csv")

    if uploaded_file:
        st_logger.debug("Uploaded file detected.")
        if 'processed_df' not in st.session_state or st.session_state.uploaded_file_hash != hashlib.md5(uploaded_file.getvalue()).hexdigest():
            st_logger.debug("New or updated CSV file. Starting processing.")
            st_logger.debug("Attempting to read CSV file.")
            df = pd.read_csv(uploaded_file, encoding='latin1', dtype=str).fillna('')
            st_logger.debug(f"CSV file read successfully. {len(df)} rows found.")
            df.rename(columns={"Author's Name": "Author"}, inplace=True)
            if 'edited' not in df.columns:
                df['edited'] = False
            st.session_state.processed_df = df
            st.session_state.uploaded_file_hash = hashlib.md5(uploaded_file.getvalue()).hexdigest()
            st.session_state.original_df = df.copy()
            st.session_state.pdf_data = None

            loc_cache = load_cache()
            
            st.write("Processing rows...")
            progress_bar = st.progress(0)

            # Get Vertex AI credentials once on the main thread
            vertex_ai_credentials = None
            try:
                vertex_ai_credentials = st.secrets["vertex_ai"]
            except Exception as e:
                st.error(f"Error loading Vertex AI credentials from st.secrets: {e}")
                st.info("Please ensure you have configured your Vertex AI credentials in Streamlit secrets. See README for instructions.")
                return # Stop execution if credentials are not available

            results = [{} for _ in range(len(st.session_state.processed_df))]
            unclassified_books_for_vertex_ai = [] # To collect books for batch Vertex AI processing

            # First pass: Process with Google Books and LOC APIs
            st_logger.debug("Starting first pass: Processing with Google Books and LOC APIs.")
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = {}
                for i, row in st.session_state.processed_df.iterrows():
                    st_logger.debug(f"Processing row {i}: Title='{row.get('Title', '')}', Author='{row.get('Author', '')}'")
                    if row['edited']:
                        st_logger.debug(f"Row {i} already edited, skipping API calls.")
                        results[i] = row.to_dict()
                        progress_bar.progress((i + 1) / len(st.session_state.processed_df))
                        continue

                    title = row.get('Title', '').strip()
                    author = row.get("Author", '').strip()
                    is_blank_row = not title and not author
                    
                    problematic_books = [
                        ("The Genius Prince's Guide to Raising a Nation Out of Debt (Hey, How About Treason?), Vol. 5", "Toba, Toru"),
                        ("The old man and the sea", "Hemingway, Ernest"),
                        ("Jack & Jill (Alex Cross)", "Patterson, James"),
                    ]
                    is_problematic_row = (title, author) in problematic_books

                    future = executor.submit(get_book_metadata_initial_pass, title, author, loc_cache, is_blank=is_blank_row, is_problematic=is_problematic_row)
                    futures[future] = i

                for i, future in enumerate(as_completed(futures)):
                    row_index = futures[future]
                    lc_meta = future.result()
                    title = st.session_state.processed_df.iloc[row_index].get('Title', '').strip()

                    # Series number extraction from title, to be done even if cache is hit
                    if not lc_meta.get('volume_number'):
                        lc_meta['volume_number'] = clean_series_number(title)

                    if not lc_meta.get('volume_number') and any(c.lower() in ['manga', 'comic'] for c in lc_meta.get('genres', [])):
                        trailing_num_match = re.search(r'(\d+)

        # Display editable DataFrame
        edited_df = st.data_editor(st.session_state.processed_df, use_container_width=True, hide_index=True)

        st.info("Values marked with 🐒 are suggestions from external APIs. The monkey emoji will not appear on printed labels, but the suggested values will be used.")

        if st.button("Apply Manual Classifications and Update Cache", key="apply_changes"):
            updated_count = 0
            current_cache = load_cache()
            for index, row in edited_df.iterrows():
                original_row = st.session_state.original_df.loc[index]
                if not row.equals(original_row):
                    st.session_state.processed_df.loc[index, 'edited'] = True
                
                title = original_row['Title'].strip()
                author = original_row['Author'].strip()
                manual_key = f"{re.sub(r'[^a-zA-Z0-9\s\.:]', '', title)}|{re.sub(r'[^a-zA-Z0-9\s,]', '', author)}".lower()

                # Update cache only if the cleaned call number has changed
                if row['Call Number'] != original_row['Call Number']:
                    # Remove monkey emoji before saving to cache
                    current_cache[manual_key] = row['Call Number'].replace(SUGGESTION_FLAG, '')
                    updated_count += 1
            save_cache(current_cache)
            st.session_state.processed_df = edited_df.copy() # Update the displayed DataFrame
            st.success(f"Updated {updated_count} manual classifications in cache and applied changes!")
            st.rerun()

        # PDF Generation Section
        st.subheader("Generate PDF Labels")
        if st.button("Generate PDF", key="generate_pdf"):
            st_logger.debug("'Generate PDF' button clicked.")
            try:
                pdf_output = generate_pdf_labels(edited_df)
                st.session_state.pdf_data = pdf_output
                st_logger.debug(f"PDF generated successfully. Size: {len(pdf_output)} bytes.")
            except Exception as e:
                st_logger.error(f"Error generating PDF: {e}", exc_info=True)
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
            st_logger.debug("Download button rendered.")

    with st.expander("Debug Log"):
        st.download_button(
            label="Download Full Debug Log",
            data=log_capture_string.getvalue(),
            file_name="debug_log.txt",
            mime="text/plain"
        )

if __name__ == "__main__":
    main()
, title)
                        if trailing_num_match:
                            lc_meta['volume_number'] = trailing_num_match.group(1)
                    st_logger.debug(f"lc_meta for row {row_index}: {lc_meta}")
                    row = st.session_state.processed_df.iloc[row_index]
                    author = row.get("Author", '').strip()

                    problematic_books = [
                        ("The Genius Prince's Guide to Raising a Nation Out of Debt (Hey, How About Treason?), Vol. 5", "Toba, Toru"),
                        ("The old man and the sea", "Hemingway, Ernest"),
                        ("Jack & Jill (Alex Cross)", "Patterson, James"),
                    ]
                    is_problematic = (title, author) in problematic_books

                    if is_problematic:
                        st_logger.info(f"--- Problematic Book Detected: {title} by {author} ---")
                    
                    # Original Atriuum data
                    original_holding_barcode = row.get('Holdings Barcode', '').strip()
                    raw_original_call_number = row.get('Call Number', '').strip() # Get raw original
                    cleaned_original_call_number = clean_call_number(raw_original_call_number, [], [], title="", is_original_data=True) # Clean original
                    original_series_name = row.get('Series Title', '').strip()
                    original_series_number = clean_series_number(row.get('Series Number', '').strip())
                    original_copyright_year = extract_year(row.get('Copyright', '').strip())
                    original_publication_date_year = extract_year(row.get('Publication Date', '').strip())

                    # Prioritize original copyright/publication year
                    final_original_year = ""
                    if original_copyright_year: final_original_year = original_copyright_year
                    elif original_publication_date_year: final_original_year = original_publication_date_year

                    # Mashed-up data from initial pass
                    api_call_number = lc_meta.get('classification', '')
                    cleaned_call_number = clean_call_number(api_call_number, lc_meta.get('genres', []), lc_meta.get('google_genres', []), title=title)
                    mashed_series_name = (lc_meta.get('series_name') or '').strip()
                    mashed_volume_number = (lc_meta.get('volume_number') or '').strip()
                    mashed_publication_year = (lc_meta.get('publication_year') or '').strip()

                    # Merge logic for initial pass
                    # Use cleaned_original_call_number if valid, else fallback to mashed-up
                    current_call_number = cleaned_original_call_number if cleaned_original_call_number else (SUGGESTION_FLAG + cleaned_call_number if cleaned_call_number else "")
                    current_series_name = original_series_name if original_series_name else (SUGGESTION_FLAG + mashed_series_name if mashed_series_name else '')
                    current_series_number = original_series_number if original_series_number else (SUGGESTION_FLAG + mashed_volume_number if mashed_volume_number else '')
                    current_publication_year = final_original_year if final_original_year else (SUGGESTION_FLAG + mashed_publication_year if mashed_publication_year else '')

                    if is_problematic:
                        st_logger.info(f"Final merged data for '{title}': Call Number: {current_call_number}, Series Info: {current_series_name}, Series Number: {current_series_number}, Copyright Year: {current_publication_year}")

                    # Collect for Vertex AI batch processing if still unclassified
                    if not current_call_number or current_call_number == "UNKNOWN":
                        unclassified_books_for_vertex_ai.append({
                            'title': title,
                            'author': author,
                            'row_index': row_index, # Keep track of original row index
                            'lc_meta': lc_meta # Keep original metadata for later merging
                        })
                    
                    results[row_index] = {
                        'Title': capitalize_title_mla(clean_title(title)),
                        'Author': clean_author(author),
                        'Holdings Barcode': original_holding_barcode,
                        'Call Number': current_call_number,
                        'Series Info': current_series_name,
                        'Series Number': current_series_number,
                        'Copyright Year': current_publication_year,
                        'edited': False
                    }
                    progress_bar.progress((i + 1) / len(st.session_state.processed_df))
            st_logger.debug("First pass complete.")

            # Second pass: Batch process unclassified books with Vertex AI
            if unclassified_books_for_vertex_ai:
                st_logger.debug(f"Starting second pass: Batch processing {len(unclassified_books_for_vertex_ai)} unclassified books with Vertex AI.")
                BATCH_SIZE = 5
                # Group unclassified books into batches
                batches = [unclassified_books_for_vertex_ai[j:j + BATCH_SIZE] for j in range(0, len(unclassified_books_for_vertex_ai), BATCH_SIZE)]
                
                with ThreadPoolExecutor(max_workers=1) as executor: # Use single worker for batch calls to avoid hitting rate limits too fast
                    batch_futures = {executor.submit(get_vertex_ai_classification_batch, batch, vertex_ai_credentials): batch for batch in batches}
                    
                    for future in as_completed(batch_futures):
                        processed_batch = batch_futures[future]
                        batch_classifications = future.result()
                        
                        if not isinstance(batch_classifications, list):
                            st_logger.error(f"Vertex AI returned non-list object: {batch_classifications}")
                            continue

                        for book_data, vertex_ai_results in zip(processed_batch, batch_classifications):
                            title = book_data['title']
                            author = book_data['author']
                            row_index = book_data['row_index']
                            lc_meta = book_data['lc_meta']
                            
                            st_logger.debug(f"--- Processing Vertex AI results for row {row_index}, title: {title} ---")
                            st_logger.debug(f"results[{row_index}] before update: {results[row_index]}")
                            st_logger.debug(f"lc_meta before update: {lc_meta}")

                            
                            st_logger.debug(f"vertex_ai_results: {vertex_ai_results}")

                            # Replace "Unknown" with empty string
                            for k, v in vertex_ai_results.items():
                                if v == "Unknown":
                                    vertex_ai_results[k] = ""

                            # Update the classification in lc_meta for this book
                            if vertex_ai_results.get('classification'):
                                lc_meta['classification'] = vertex_ai_results['classification']
                                if 'google_genres' not in lc_meta or not isinstance(lc_meta['google_genres'], list):
                                    lc_meta['google_genres'] = []
                                lc_meta['google_genres'].append(vertex_ai_results['classification'])
                            
                            if vertex_ai_results.get('series_title'):
                                lc_meta['series_name'] = vertex_ai_results['series_title']
                            if vertex_ai_results.get('volume_number'):
                                lc_meta['volume_number'] = vertex_ai_results['volume_number']
                            if lc_meta.get('copyright_year'):
                                lc_meta['publication_year'] = vertex_ai_results['copyright_year']

                            st_logger.debug(f"lc_meta after update: {lc_meta}")

                            # Re-clean call number with new Vertex AI classification
                            final_call_number_after_vertex_ai = clean_call_number(lc_meta.get('classification', ''), lc_meta.get('genres', []), lc_meta.get('google_genres', []), title=title)
                            
                            # Update the results list, but only if we have new information
                            if final_call_number_after_vertex_ai and not results[row_index]['Call Number'].replace(SUGGESTION_FLAG, ''):
                                results[row_index]['Call Number'] = SUGGESTION_FLAG + final_call_number_after_vertex_ai
                            
                            if lc_meta.get('series_name') and not results[row_index]['Series Info'].replace(SUGGESTION_FLAG, ''):
                                results[row_index]['Series Info'] = SUGGESTION_FLAG + lc_meta.get('series_name')

                            if lc_meta.get('volume_number') and not results[row_index]['Series Number'].replace(SUGGESTION_FLAG, ''):
                                results[row_index]['Series Number'] = SUGGESTION_FLAG + str(lc_meta.get('volume_number'))

                            if lc_meta.get('publication_year') and not results[row_index]['Copyright Year'].replace(SUGGESTION_FLAG, ''):
                                results[row_index]['Copyright Year'] = SUGGESTION_FLAG + str(lc_meta.get('publication_year'))
                            st_logger.debug(f"results[{row_index}] after update: {results[row_index]}")
                st_logger.debug("Second pass (Vertex AI) complete.")

            # Final pass to populate Series Info and ensure all fields are non-blank
            for i, row_data in enumerate(results):
                # Re-combine series name and volume number for display after all processing
                final_series_name = row_data['Series Info'] # This was populated with mashed_series_name
                final_series_number = row_data['Series Number']

                results[i]['Series Info'] = final_series_name
                results[i]['Series Number'] = final_series_number

            save_cache(loc_cache)
            
            st.write("Processing complete!")
            
            # Create a DataFrame from results
            results_df = pd.DataFrame(results)
            st.session_state.processed_df = results_df

        # Display editable DataFrame
        edited_df = st.data_editor(st.session_state.processed_df, use_container_width=True, hide_index=True)

        st.info("Values marked with 🐒 are suggestions from external APIs. The monkey emoji will not appear on printed labels, but the suggested values will be used.")

        if st.button("Apply Manual Classifications and Update Cache", key="apply_changes"):
            updated_count = 0
            current_cache = load_cache()
            for index, row in edited_df.iterrows():
                original_row = st.session_state.original_df.loc[index]
                if not row.equals(original_row):
                    st.session_state.processed_df.loc[index, 'edited'] = True
                
                title = original_row['Title'].strip()
                author = original_row['Author'].strip()
                manual_key = f"{re.sub(r'[^a-zA-Z0-9\s\.:]', '', title)}|{re.sub(r'[^a-zA-Z0-9\s,]', '', author)}".lower()

                # Update cache only if the cleaned call number has changed
                if row['Call Number'] != original_row['Call Number']:
                    # Remove monkey emoji before saving to cache
                    current_cache[manual_key] = row['Call Number'].replace(SUGGESTION_FLAG, '')
                    updated_count += 1
            save_cache(current_cache)
            st.session_state.processed_df = edited_df.copy() # Update the displayed DataFrame
            st.success(f"Updated {updated_count} manual classifications in cache and applied changes!")
            st.rerun()

        # PDF Generation Section
        st.subheader("Generate PDF Labels")
        if st.button("Generate PDF", key="generate_pdf"):
            st_logger.debug("'Generate PDF' button clicked.")
            try:
                pdf_output = generate_pdf_labels(edited_df)
                st.session_state.pdf_data = pdf_output
                st_logger.debug(f"PDF generated successfully. Size: {len(pdf_output)} bytes.")
            except Exception as e:
                st_logger.error(f"Error generating PDF: {e}", exc_info=True)
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
            st_logger.debug("Download button rendered.")

    with st.expander("Debug Log"):
        st.download_button(
            label="Download Full Debug Log",
            data=log_capture_string.getvalue(),
            file_name="debug_log.txt",
            mime="text/plain"
        )

if __name__ == "__main__":
    main()
