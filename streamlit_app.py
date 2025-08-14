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
    st_logger.debug(f"Generating PDF for {len(df)} rows.")
    if len(df) > 0:
        st_logger.debug(f"First row of data:\n{df.head(1).to_string()}")
    buffer = io.BytesIO()
    c = canvas.Canvas(buffer, pagesize=letter)
    width, height = letter

    # Label dimensions (example, adjust as needed)
    label_width = 2.5 * inch
    label_height = 1 * inch
    x_margin = 0.5 * inch
    y_margin = 0.5 * inch
    x_spacing = 0.25 * inch
    y_spacing = 0.25 * inch

    labels_per_row = int((width - 2 * x_margin + x_spacing) / (label_width + x_spacing))
    labels_per_col = int((height - 2 * y_margin + y_spacing) / (label_height + y_spacing))

    for i, row in df.iterrows():
        col_num = i % labels_per_row
        row_num = (i // labels_per_row) % labels_per_col
        
        if i > 0 and col_num == 0 and row_num == 0:
            c.showPage()

        x = x_margin + col_num * (label_width + x_spacing)
        y = height - y_margin - (row_num + 1) * (label_height + y_spacing)

        # Draw border for label (for debugging layout)
        # c.rect(x, y, label_width, label_height)

        # Extract and clean data for label
        holding_barcode = str(row['Holdings Barcode']).replace(SUGGESTION_FLAG, '')
        call_number = str(row['Call Number']).replace(SUGGESTION_FLAG, '')
        title = str(row['Title'])
        author = str(row['Author'])
        series_info = str(row['Series Info']).replace(SUGGESTION_FLAG, '')
        copyright_year = str(row['Copyright Year']).replace(SUGGESTION_FLAG, '')

        # Set font and size
        c.setFont('Helvetica', 10)

        # Draw Holding Number
        c.drawString(x + 0.1 * inch, y + label_height - 0.2 * inch, holding_barcode)

        # Draw Call Number
        c.drawString(x + 0.1 * inch, y + label_height - 0.35 * inch, call_number)

        # Draw Title (truncate if too long)
        max_title_width = label_width - 0.2 * inch
        if c.stringWidth(title, 'Helvetica', 10) > max_title_width:
            while c.stringWidth(title + '...', 'Helvetica', 10) > max_title_width:
                title = title[:-1]
            title += '...'
        c.drawString(x + 0.1 * inch, y + label_height - 0.5 * inch, title)

        # Draw Author
        c.drawString(x + 0.1 * inch, y + label_height - 0.65 * inch, author)

        # Draw Series Info and Copyright Year
        bottom_text = []
        if series_info: bottom_text.append(series_info)
        if copyright_year: bottom_text.append(copyright_year)
        
        if bottom_text:
            c.drawString(x + 0.1 * inch, y + label_height - 0.8 * inch, ', '.join(bottom_text))

    c.save()
    pdf_data = buffer.getvalue()
    st_logger.debug(f"Generated PDF size: {len(pdf_data)} bytes.")
    return pdf_data

def extract_year(date_string):
    """Extracts the first 4-digit number from a string, assuming it's a year."""
    if isinstance(date_string, str):
        # Regex to find a 4-digit year, ignoring surrounding brackets, c, or ©
        match = re.search(r'[\(\)\[©c]?(?:\d{4})[\)\]]?', date_string)
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
        if 'processed_df' not in st.session_state or st.session_state.uploaded_file_hash != hashlib.md5(uploaded_file.getvalue()).hexdigest():
            df = pd.read_csv(uploaded_file, encoding='latin1', dtype=str).fillna('')
            df.rename(columns={"Author's Name": "Author"}, inplace=True)
            if 'edited' not in df.columns:
                df['edited'] = False
            st.session_state.processed_df = df
            st.session_state.uploaded_file_hash = hashlib.md5(uploaded_file.getvalue()).hexdigest()
            st.session_state.original_df = df.copy()

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
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = {}
                for i, row in st.session_state.processed_df.iterrows():
                    if row['edited']:
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
                        trailing_num_match = re.search(r'(\d+)$', title)
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

            # Second pass: Batch process unclassified books with Vertex AI
            if unclassified_books_for_vertex_ai:
                st.write("Attempting Vertex AI batch classification for remaining books...")
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

        if st.button("Apply Manual Classifications and Update Cache"):
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
        if st.button("Generate PDF"):
            st_logger.debug("'Generate PDF' button clicked.")
            pdf_output = generate_pdf_labels(edited_df)
            st_logger.debug(f"PDF output type: {type(pdf_output)}, size: {len(pdf_output)} bytes.")
            st.download_button(
                label="Download PDF Labels",
                data=pdf_output,
                file_name="book_labels.pdf",
                mime="application/pdf"
            )
            st_logger.debug("Download button created.")

    with st.expander("Debug Log"):
        st.download_button(
            label="Download Full Debug Log",
            data=log_capture_string.getvalue(),
            file_name="debug_log.txt",
            mime="text/plain"
        )

if __name__ == "__main__":
    main()
