import re
import requests
import json
import os
import time
import logging
from lxml import etree # For LOC API parsing

# --- Logging Setup ---
# This logger will be used by the functions extracted from Streamlit app
# It will print to stdout, which can be captured if needed.
ext_logger = logging.getLogger(__name__)
ext_logger.setLevel(logging.DEBUG)
ext_handler = logging.StreamHandler()
ext_handler.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
ext_logger.addHandler(ext_handler)

# --- Constants & Cache (simplified for external use) ---
SUGGESTION_FLAG = "🐒" # Keep this for consistency if needed for cleaning
# CACHE_FILE is handled externally by loc_enricher.py

# --- Helper Functions (from streamlit_app.py) ---
def clean_title(title):
    """Cleans title by moving leading articles to the end."""
    if not isinstance(title, str):
        return ""
    articles = ['The ', 'A ', 'An ']
    for article in articles:
        if title.startswith(article):
            return title[len(article):] + ", " + title[:len(article)-1]
    return title

def clean_author(author):
    """Cleans author name to Last, First Middle."""
    if not isinstance(author, str):
        return ""
    parts = author.split(',')
    if len(parts) == 2:
        return f"{parts[0].strip()}, {parts[1].strip()}"
    return author

def extract_year(date_string):
    """Extracts the first 4-digit number from a string, assuming it's a year."""
    if isinstance(date_string, str):
        # Regex to find a 4-digit year, ignoring surrounding brackets, c, or ©
        match = re.search(r'[\(\)\[©c]?(\d{4})[\)\]]?', date_string)
        if match:
            return match.group(1)
    return ""

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
    ext_logger.debug(f"clean_call_number input: call_num_str='{call_num_str}', genres={genres}, google_genres={google_genres}, title='{title}', is_original_data={is_original_data}")
    if google_genres is None:
        google_genres = []
        
    if not isinstance(call_num_str, str):
        ext_logger.debug(f"clean_call_number returning empty string for non-string input: {call_num_str}")
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
        ext_logger.debug(f"clean_call_number returning FIC based on genre/title keywords: {cleaned}")
        return "FIC"

    # Check for LCC
    ddc_from_lcc = lcc_to_ddc(cleaned)
    if ddc_from_lcc:
        ext_logger.debug(f"clean_call_number returning from LCC: {ddc_from_lcc}")
        return ddc_from_lcc

    # Strip common non-numeric characters from Dewey-like numbers
    cleaned = re.sub(r'[^a-zA-Z0-9\s\.:]', '', cleaned).strip()

    # If the cleaned string is a known non-numeric genre from Vertex AI, map to FIC
    # This catches cases where Vertex AI directly returns a genre name
    if cleaned.lower() in ["fantasy", "science fiction", "thriller", "mystery", "romance", "horror", "novel", "fiction", "young adult fiction", "historical fiction", "literary fiction"]:
        ext_logger.debug(f"clean_call_number returning FIC based on cleaned string match: {cleaned}")
        return "FIC"

    # Check for explicit "FIC" or valid Dewey Decimal from any source
    if cleaned.upper().startswith("FIC"):
        ext_logger.debug(f"clean_call_number returning FIC based on explicit FIC: {cleaned}")
        return "FIC"
    
    # Strict check for Dewey Decimal Number format (3 digits, optional decimal and more digits)
    if re.match(r'^\d{3}(\.\d+)?$', cleaned):
        ext_logger.debug(f"clean_call_number returning Dewey Decimal: {cleaned}")
        return cleaned
    
    # If it's a number but not 3 digits, still return it (e.g., from LOC 050 field like "PS3515.E37"), keep it as is
    # This allows for LC call numbers to pass through if they are numeric-like
    if re.match(r'^[A-Z]{1,3}\d+(\.\d+)?$', cleaned) or re.match(r'^\d+(\.\d+)?$', cleaned):
        ext_logger.debug(f"clean_call_number returning LC-like or numeric: {cleaned}")
        return cleaned

    # If none of the above conditions are met, it's an invalid format for a call number
    ext_logger.debug(f"clean_call_number returning empty string for invalid format: {cleaned}")
    return ""


def get_book_metadata_google_books(title, author, cache):
    """Fetches book metadata from the Google Books API."""
    safe_title = re.sub(r'[^a-zA-Z0-9\s\.:]', '', title)
    safe_author = re.sub(r'[^a-zA-Z0-9\s,]', '', author)
    cache_key = f"google_{safe_title}|{safe_author}".lower()
    if cache_key in cache:
        ext_logger.debug(f"Google Books cache hit for '{title}' by '{author}'.")
        return cache[cache_key]

    metadata = {'google_genres': [], 'classification': '', 'series_name': '', 'volume_number': '', 'publication_year': '', 'error': None}
    try:
        query = f'intitle:"{safe_title}"+inauthor:"{safe_author}"'
        url = f"https://www.googleapis.com/books/v1/volumes?q={query}&maxResults=1"
        ext_logger.debug(f"Google Books query for '{title}' by '{author}': {url}")
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        data = response.json()
        ext_logger.debug(f"Google Books raw response for '{title}' by '{author}': {response.text}")

        if "items" in data and data["items"]:
            item = data["items"][0]
            volume_info = item.get("volumeInfo", {})
            ext_logger.debug(f"Google Books volume_info for '{title}': {volume_info}")
            ext_logger.info(f"Google Books genres for '{title}': {volume_info.get('categories', [])}")

            if "categories" in volume_info:
                metadata['google_genres'].extend(volume_info["categories"])
            
            if "description" in volume_info:
                description = volume_info["description"]
                match = re.search(r'Subject: (.*?)(?:\n|$)', description, re.IGNORECASE)
                if match:
                    subjects = [s.strip() for s in match.group(1).split(',')]
                    metadata['google_genres'].extend(subjects)
                    ext_logger.info(f"Google Books subjects for '{title}': {subjects}")

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
        
        import vertexai # Import here to avoid global import issues if not configured
        from vertexai.generative_models import GenerativeModel

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
        ext_logger.debug(f"Vertex AI full prompt:\n```\n{full_prompt}\n```")
        
        for i in range(len(retry_delays) + 1):
            try:
                response = model.generate_content(full_prompt)
                # Attempt to parse JSON response
                response_text = response.text.strip()
                ext_logger.debug(f"Vertex AI raw response:\n```\n{response_text}\n```")
                # Clean up markdown code block if present
                if response_text.startswith("```json") and response_text.endswith("```"):
                    response_text = response_text[7:-3].strip()
                
                classifications = json.loads(response_text)
                
                return classifications
            except Exception as e:
                if i < len(retry_delays):
                    ext_logger.warning(f"Vertex AI batch call failed: {e}. Retrying in {retry_delays[i]} seconds...")
                    time.sleep(retry_delays[i])
                else:
                    ext_logger.error(f"Vertex AI batch call failed after multiple retries: {e}")
                    return [] # Return empty list on failure
    finally:
        if os.path.exists(temp_creds_path):
            os.remove(temp_creds_path)

# Main execution block (for testing purposes)
if __name__ == '__main__':
    # Example usage (replace with actual data and credentials)
    sample_books = [
        {'title': 'The Hitchhiker\'s Guide to the Galaxy', 'author': 'Douglas Adams'},
        {'title': 'Pride and Prejudice', 'author': 'Jane Austen'},
    ]
    # You would need to provide actual Vertex AI credentials here
    # vertex_ai_creds = {"project_id": "your-project-id", ...}
    # classifications = get_vertex_ai_classification_batch(sample_books, vertex_ai_creds)
    # print(json.dumps(classifications, indent=4))

    # Example for Google Books
    # cache = {}
    # google_meta = get_book_metadata_google_books("The Lord of the Rings", "J.R.R. Tolkien", cache)
    # print(json.dumps(google_meta, indent=4))
