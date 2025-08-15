import pandas as pd
import json
import os
import re
import requests
import time
from lxml import etree
import logging
from io import StringIO
from pymarc import Record
from pymarc.marcxml import parse_xml_to_array

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


# --- Helper Functions (copied from streamlit_app.py for standalone testing) ---
# Updated keyword lists
fiction_keywords = [
    r'\bfiction\b',
    r'\bnovel\b',
    r'\bnovels\b',
    r'\bstory\b',
    r'\bstories\b',
    r'\bshort stories\b',
    r'\bgraphic novel\b',
    r'\bnarrative\b',
    r'\bdrama\b',
    r'\bromance\b',
    r'\bmystery\b',
    r'\bthriller\b',
    r'\bfi\b',
    r'\bfantasy\b',
    r'\bhistorical fiction\b',
    r'\bliterary fiction\b',
    r'\badventure\b',
    r'\bcoming[- ]of[- ]age\b',
    r'\bfairy tale\b',
    r'\bfable\b',
    r'\bsatire\b',
    r'\bparody\b',
    r'\ballegory\b',
    r'\bmanga\b'
]

nonfiction_exclusions = [
    r'\bnon[- ]?fiction\b',
    r'\bbiography\b',
    r'\bautobiography\b',
    r'\bmemoir\b',
    r'\bself[- ]?help\b',
    r'\bguide\b',
    r'\bmanual\b',
    r'\btextbook\b',
    r'\bhow[- ]to\b',
    r'\btrue story\b',
    r'\bdocumentary\b',
    r'\bhistory\b',
    r'\bscience\b',
    r'\bphilosophy\b',
    r'\bpsychology\b',
    r'\bpolitics\b',
    r'\beconomics\b'
]


def is_likely_fiction(text: str) -> bool:
    """Check if text matches fiction patterns and not nonfiction exclusions."""
    low = text.lower()
    return (
        any(re.search(p, low) for p in fiction_keywords)
        and not any(re.search(p, low) for p in nonfiction_exclusions)
    )


def parse_raw_response(xml_string: str) -> str:
    """
    Parse raw LOC API XML and return 'FIC' if it looks like fiction.
    Scans genreForm, subject, description, summary, title, and notes.
    """
    print(f"[DEBUG] parse_raw_response: Input XML string (first 200 chars): {xml_string[:200]}")
    try:
        root = etree.fromstring(xml_string)
        print(f"[DEBUG] parse_raw_response: Type of root: {type(root)}")
        print(f"[DEBUG] parse_raw_response: Root tag: {root.tag}")
    except etree.XMLSyntaxError as e:
        print(f"[ERROR] parse_raw_response: XML Syntax Error: {e}")
        return "" # Return empty string on XML parsing error
    except Exception as e:
        print(f"[ERROR] parse_raw_response: Unexpected error during XML parsing: {e}")
        return ""

    pieces = []
    # MARCXML tags to search for textual content
    # 655: Genre/Form, 650: Subject, 520: Summary, 245: Title, 500: General Note
    tags_to_search = ['655', '650', '520', '245', '500']
    ns_marc = {'marc': 'http://www.loc.gov/MARC21/slim'}

    for tag in tags_to_search:
        # Find all subfields within the datafield
        for datafield in root.findall(f'.//marc:datafield[@tag="{tag}"]', ns_marc):
            for subfield in datafield.findall('marc:subfield', ns_marc):
                if subfield.text:
                    pieces.append(subfield.text)

    combined_text = ' '.join(pieces)
    print(f"[DEBUG] parse_raw_response: Combined text for fiction detection: {combined_text[:500]}")
    if is_likely_fiction(combined_text):
        return "FIC"
    return "" # Return empty string if not fiction


def is_fiction_ddn(ddn: str) -> bool:
    """
    Determines if a Dewey Decimal Number (DDN) represents fiction,
    based on an expanded list that includes literature, poetry, drama, and fiction.

    Args:
        ddn (str): The DDN string, e.g., '813', '822.33', '873.01'

    Returns:
        bool: True if the DDN is classified as fiction, False otherwise.
    """
    # Regex pattern for accepted fiction-related DDNs
    fiction_pattern = re.compile(
        r'^(('
        r'810(\.\d+)?|'  # American literature
        r'811(\.\d+)?|'  # American poetry
        r'812(\.\d+)?|'  # American drama
        r'813(\.\d+)?|'  # American fiction
        r'822(\.\d+)?|'  # English drama
        r'823(\.\d+)?|'  # English fiction
        r'833(\.\d+)?|'  # German fiction
        r'843(\.\d+)?|'  # French fiction
        r'853(\.\d+)?|'  # Italian fiction
        r'862(\.\d+)?|'  # Spanish drama
        r'863(\.\d+)?|'  # Spanish fiction
        r'872(\.\d+)?|'  # Latin drama
        r'873(\.\d+)?|'  # Latin epic poetry and fiction
        r'883(\.\d+)?'   # Classical Greek epic poetry and fiction
        r'))$'
    )

    return bool(fiction_pattern.match(ddn.strip()))


# Configure module-level logger
logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)


def is_fiction(call_number: str) -> bool:
    """
    Return True if the LC call number should be classified as fiction,
    with an explicit exclusion for PE (grammar, usage, spelling).
    """
    # Normalize and extract the two-letter prefix
    prefix_match = re.match(r'^([A-Z]{1,2})', call_number.upper())
    prefix = prefix_match.group(1) if prefix_match else ''

    # 1. PZ is always fiction
    if prefix == 'PZ':
        return True

    # 2. PE is always non-fiction (grammar & spelling refs, dictionaries, etc.)
    if prefix == 'PE':
        return False

    # 3. PR, PS, PQ cover English, American, and Romance literatures
    if prefix in {'PR', 'PS', 'PQ'}:
        return True

    # 4. All other P-subclasses – defer to your broader non-fiction logic
    return False


def approximate_dewey_from_050(call_no_050: str) -> str:
    """
    Approximate a Dewey Decimal number from an LC 050 call number,
    or return 'FIC' if the item is likely fiction.

    Parameters:
      call_no_050 (str): The 050 field’s call number (e.g. "PS3552 .O752 A6 2020" or "QA76.73.P98").

    Returns:
      str: A three-digit Dewey class (as string) or "FIC" for likely fiction.
           Returns None if it cannot parse the call number.
    """
    # 1. Strip whitespace and grab the initial one or two letters
    call_no_050 = call_no_050.strip().upper()
    match = re.match(r'^([A-Z]{1,2})', call_no_050)
    if not match:
        return None

    prefix = match.group(1)

    # 2. Fiction detection: use the new is_fiction function for P-classes
    if is_fiction(call_no_050):
        return "FIC"

    # 3. Map the first letter of non-P classes to an approximate Dewey number
    letter = prefix[0]
    dewey_map = {
        'A': '000',  # General works
        'B': '100',  # Philosophy & psychology
        'C': '900',  # History & geography (auxiliary)
        'D': '900',  # History
        'E': '900',  # History of Americas
        'F': '970',  # U.S. local history
        'G': '910',  # Geography, recreation
        'H': '300',  # Social sciences
        'J': '340',  # Law
        'K': '350',  # Law of the Americas
        'L': '400',  # Education & linguistics
        'M': '780',  # Music
        'N': '780',  # Fine arts
        'Q': '500',  # Science
        'R': '600',  # Technology & medicine
        'S': '650',  # Technology
        'T': '630',  # Agriculture & related technologies
        'U': '355',  # Military science
        'V': '359',  # Naval sciences
        'W': '629',  # Aircraft & aerospace
        'X': '016',  # Bibliography
        'Y': '686',  # Printing & related activities
        'Z': '020',  # Library & information sciences
    }

    return dewey_map.get(letter)


# --- Google Books Category to Dewey Mapping ---
# This is a simplified mapping and can be expanded/refined.
google_category_to_dewey_map = {
    "Computers": "000",
    "Philosophy": "100",
    "Psychology": "150",
    "Religion": "200",
    "Social Science": "300",
    "Language Arts & Disciplines": "400",
    "Science": "500",
    "Mathematics": "510",
    "Technology": "600",
    "Medical": "610",
    "Engineering": "620",
    "Agriculture": "630",
    "Home Economics": "640",
    "Management": "650",
    "Manufacturing": "670",
    "Arts": "700",
    "Music": "780",
    "Sports & Recreation": "790",
    "Literature": "800",
    "History": "900",
    "Biography & Autobiography": "920",
    "Geography": "910",
    "Travel": "910",
    # More specific mappings can be added as needed
    "Fiction": "FIC", # Handled separately, but good to have here for completeness
    "Juvenile Fiction": "FIC",
    "Young Adult Fiction": "FIC",
    "Literary Criticism": "800",
    "Education": "370",
    "Political Science": "320",
    "Economics": "330",
    "Law": "340",
    "Family & Relationships": "306",
    "Health & Fitness": "613",
    "Cooking": "641",
    "Gardening": "635",
    "Crafts & Hobbies": "745",
    "Performing Arts": "792",
    "Poetry": "808",
    "Drama": "808",
    "Humor": "817",
    "True Crime": "364",
    "Nature": "570",
    "Animals": "590",
    "Antiques & Collectibles": "745",
    "Architecture": "720",
    "Design": "740",
    "Photography": "770",
    "Games": "793",
    "Comics & Graphic Novels": "741",
    "Self-Help": "158",
    "Business & Economics": "330",
    "Computers / Programming": "005",
    "Computers / Networking": "004",
    "Computers / Hardware": "004",
    "Computers / Software": "005",
    "Computers / Web": "006",
    "Computers / Databases": "005",
    "Computers / Security": "005",
    "Computers / Mobile": "004",
    "Computers / Operating Systems": "005",
    "Computers / Graphics": "006",
    "Computers / Artificial Intelligence": "006",
    "Computers / Data Science": "006",
    "Computers / Machine Learning": "006",
    "Computers / Big Data": "006",
    "Computers / Cloud Computing": "004",
    "Computers / Virtual Reality": "006",
    "Computers / Internet": "004",
    "Computers / Social Media": "302",
    "Computers / Digital Media": "006",
    "Computers / Games": "794",
    "Computers / Robotics": "629",
    "Computers / General": "004",
    "Computers / Hardware / General": "004",
    "Computers / Networking / General": "004",
    "Computers / Programming / General": "005",
    "Computers / Software / General": "005",
    "Computers / Web / General": "006",
    "Computers / Databases / General": "005",
    "Computers / Security / General": "005",
    "Computers / Mobile / General": "004",
    "Computers / Operating Systems / General": "005",
    "Computers / Graphics / General": "006",
    "Computers / Artificial Intelligence / General": "006",
    "Computers / Data Science / General": "006",
    "Computers / Machine Learning / General": "006",
    "Computers / Big Data / General": "006",
    "Computers / Cloud Computing / General": "004",
    "Computers / Virtual Reality / General": "006",
    "Computers / Internet / General": "004",
    "Computers / Social Media / General": "302",
    "Computers / Digital Media / General": "006",
    "Computers / Games / General": "794",
    "Computers / Robotics / General": "629",
}


def estimate_dewey_from_google_categories(categories: list, main_category: str) -> str | None:
    """
    Estimates a Dewey Decimal Number (DDN) based on Google Books categories.
    Prioritizes main_category, then iterates through categories.
    """
    # Prioritize main_category if it provides a direct mapping
    if main_category and main_category in google_category_to_dewey_map:
        return google_category_to_dewey_map[main_category]

    # Iterate through all categories for a match
    for category in categories:
        # Check for exact match
        if category in google_category_to_dewey_map:
            return google_category_to_dewey_map[category]

        # Check for partial matches (e.g., "History / Ancient" -> "History")
        for key, ddc in google_category_to_dewey_map.items():
            if key in category:
                return ddc

    return None # No suitable Dewey estimation found


def clean_call_number(call_num_str, genres, raw_response_xml, call_no_050, main_category):
    print(f"[DEBUG] clean_call_number: Initial call_num_str='{call_num_str}', call_no_050='{call_no_050}', main_category='{main_category}'")
    if not isinstance(call_num_str, str):
        call_num_str = "" # Ensure it's a string for strip() and lstrip()

    cleaned = call_num_str.strip().lstrip(SUGGESTION_FLAG)
    print(f"[DEBUG] clean_call_number: After stripping suggestion flag and whitespace, cleaned='{cleaned}'")
    cleaned = cleaned.replace('/', '')
    print(f"[DEBUG] clean_call_number: After replacing '/', cleaned='{cleaned}'")

    # New genre check using raw_response_xml (prioritized)
    if raw_response_xml and raw_response_xml.strip().startswith("<?xml"):
        fiction_from_raw = parse_raw_response(raw_response_xml)
        print(f"[DEBUG] clean_call_number: parse_raw_response returned '{fiction_from_raw}'")
        if fiction_from_raw == "FIC":
            print(f"[DEBUG] clean_call_number: Detected FIC from raw_response_xml. Returning 'FIC'")
            return "FIC"

    # Primary fiction checks (existing)
    if cleaned.upper().startswith("[FIC]") or cleaned.upper().startswith("FIC"):
        print(f"[DEBUG] clean_call_number: Detected FIC from prefix. Returning 'FIC'")
        return "FIC"
    if is_fiction_ddn(cleaned):
        print(f"[DEBUG] clean_call_number: Detected FIC from is_fiction_ddn. Returning 'FIC'")
        return "FIC"

    # If call_num_str is still blank, try 050 fallback
    if not cleaned and call_no_050:
        print(f"[DEBUG] clean_call_number: Call number is blank, attempting 050 fallback with '{call_no_050}'")
        approx_dewey = approximate_dewey_from_050(call_no_050)
        print(f"[DEBUG] clean_call_number: approximate_dewey_from_050 returned '{approx_dewey}'")
        if approx_dewey:
            print(f"[DEBUG] clean_call_number: Using 050 fallback. Returning '{SUGGESTION_FLAG}{approx_dewey}'")
            return f"{SUGGESTION_FLAG}{approx_dewey}" # Add suggestion flag for 050 fallback

    # If still no classification, try Google Books categories fallback
    if not cleaned:
        print(f"[DEBUG] clean_call_number: Call number still blank, attempting Google Books categories fallback.")
        estimated_dewey = estimate_dewey_from_google_categories(genres, main_category) # genres are Google Books categories
        print(f"[DEBUG] clean_call_number: estimate_dewey_from_google_categories returned '{estimated_dewey}'")
        if estimated_dewey and estimated_dewey != "FIC": # Don't overwrite FIC if already determined
            print(f"[DEBUG] clean_call_number: Using Google Books categories fallback. Returning '{SUGGESTION_FLAG}{estimated_dewey}'")
            return f"{SUGGESTION_FLAG}{estimated_dewey}" # Add suggestion flag for Google Books fallback

    match = re.match(r'^(\d+(\.\d+)?)\\', cleaned) # Corrected regex for DDN matching
    if match:
        print(f"[DEBUG] clean_call_number: Detected DDN from regex. Returning '{match.group(1)}'")
        return match.group(1)
    print(f"[DEBUG] clean_call_number: No specific pattern matched. Returning cleaned='{cleaned}'")
    return cleaned


def extract_series_info(xml_string: str) -> dict:
    """
    Extracts series title and volume from MARCXML.
    Prioritizes 490 fields, then falls back to 830 fields, and finally checks 020 subfield q.
    """
    series_data = {"series": [], "fallback": []}
    try:
        root = etree.fromstring(xml_string.encode('utf-8'))
        ns_marc = {'marc': 'http://www.loc.gov/MARC21/slim'}

        # Extract from 490 fields (Series Statement)
        for field_490 in root.findall('.//marc:datafield[@tag="490"]', ns_marc):
            title_node = field_490.find('marc:subfield[@code="a"]', ns_marc)
            volume_node = field_490.find('marc:subfield[@code="v"]', ns_marc)
            title = title_node.text.strip() if title_node is not None else ""
            volume = volume_node.text.strip() if volume_node is not None else ""
            if title or volume:
                series_data["series"].append({"title": title, "volume": volume})

        # Extract from 830 fields (Series Added Entry - Uniform Title) as fallback
        if not series_data["series"]:
            for field_830 in root.findall('.//marc:datafield[@tag="830"]', ns_marc):
                title_node = field_830.find('marc:subfield[@code="a"]', ns_marc)
                volume_node = field_830.find('marc:subfield[@code="v"]', ns_marc)
                title = title_node.text.strip() if title_node is not None else ""
                volume = volume_node.text.strip() if volume_node is not None else ""
                if title or volume:
                    series_data["fallback"].append({"title": title, "volume": volume})

        # Extract from 440 fields (Series Statement/Added Entry - Title)
        # 440 is obsolete but might still be present in older records
        if not series_data["series"] and not series_data["fallback"]:
            for field_440 in root.findall('.//marc:datafield[@tag="440"]', ns_marc):
                title_node = field_440.find('marc:subfield[@code="a"]', ns_marc)
                volume_node = field_440.find('marc:subfield[@code="v"]', ns_marc)
                title = title_node.text.strip() if title_node is not None else ""
                volume = volume_node.text.strip() if volume_node is not None else ""
                if title or volume:
                    series_data["series"].append({"title": title, "volume": volume})

        # Extract volume from 020 subfield q as a last resort for volume number
        if not series_data["series"] and not series_data["fallback"]:
            for field_020 in root.findall('.//marc:datafield[@tag="020"]', ns_marc):
                subfield_q_node = field_020.find('marc:subfield[@code="q"]', ns_marc)
                if subfield_q_node is not None and subfield_q_node.text:
                    volume_match = re.search(r'[vV]\.?\s*(\d+)', subfield_q_node.text)
                    if volume_match:
                        # If a volume is found in 020$q, add it to the series data.
                        # We don't have a series title from here, so it will be empty.
                        series_data["series"].append({"title": "", "volume": volume_match.group(1)})
                        break # Assuming only one relevant volume per 020$q is needed

    except etree.XMLSyntaxError as e:
        logger.error(f"XML parsing error in extract_series_info: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred in extract_series_info: {e}")

    return series_data


def get_book_metadata(title, author, cache):
    # Sanitize title and author for API query
    # Allow alphanumeric, spaces, and common punctuation like . : '\n'
    safe_title = re.sub(r'[^a-zA-Z0-9\s\.:\\]', '', title)
    safe_author = re.sub(r'[^a-zA-Z0-9\s,]', '', author)

    # Debugging statements using print for standalone script
    print(f"\n--- Processing: Title='{title}', Author='{author}' ---")
    print(f"[DEBUG] get_book_metadata: Sanitized Title='{safe_title}', Sanitized Author='{safe_author}'")

    cache_key = f"{safe_title}|{safe_author}".lower()
    if cache_key in cache:
        print(f"[DEBUG] get_book_metadata: Cache Hit for: {cache_key}")
        return cache[cache_key]
    print(f"[DEBUG] get_book_metadata: Cache Miss for: {cache_key}")

    base_url = "http://lx2.loc.gov:210/LCDB"
    query = f'bath.title="{safe_title}" and bath.author="{safe_author}"'
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
    params = {"version": "1.1", "operation": "searchRetrieve", "query": query, "maximumRecords": "1", "recordSchema": "marcxml"}
    metadata = {'classification': "", 'call_no_050': "", 'series_name': "", 'volume_number': "", 'publication_year': "", 'genres': [], 'error': None, 'query': query, "raw_response": ''}

    print(f"[DEBUG] get_book_metadata: API Query: {query}")

    MAX_RETRIES = 3
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(base_url, params=params, timeout=30, headers=headers)
            response.raise_for_status()
            metadata['raw_response'] = response.content.decode('utf-8')
            print(f"[DEBUG] get_book_metadata: Raw API Response (first 500 chars): {metadata['raw_response'][:500]}")
            root = etree.fromstring(response.content)
            ns_diag = {'diag': 'http://www.loc.gov/zing/srw/diagnostic/'}
            error_message = root.find('.//diag:message', ns_diag)
            if error_message is not None:
                metadata['error'] = f"API Error: {error_message.text}"
                print(f"[DEBUG] get_book_metadata: API Error Message: {error_message.text}")
            else:
                ns_marc = {'marc': 'http://www.loc.gov/MARC21/slim'}

                # Extract 082 classification
                classification_node = root.find('.//marc:datafield[@tag="082"]/marc:subfield[@code="a"]', ns_marc)
                if classification_node is not None: metadata['classification'] = classification_node.text.strip()
                print(f"[DEBUG] get_book_metadata: Extracted 082 Classification: {metadata['classification']}")

                # Extract 050 classification for fallback
                call_no_050_node = root.find('.//marc:datafield[@tag="050"]/marc:subfield[@code="a"]', ns_marc)
                metadata['call_no_050'] = call_no_050_node.text.strip() if call_no_050_node is not None else ""
                print(f"[DEBUG] get_book_metadata: Extracted 050 Call Number: {metadata['call_no_050']}")

                # Extract series info using the new function
                series_info = extract_series_info(metadata['raw_response'])
                print(f"[DEBUG] get_book_metadata: Extracted Series Info: {series_info}")
                if series_info['series']:
                    # Prioritize the first series found
                    metadata['series_name'] = series_info['series'][0]['title']
                    metadata['volume_number'] = series_info['series'][0]['volume']
                    print(f"[DEBUG] get_book_metadata: Using primary series: {metadata['series_name']} (Volume: {metadata['volume_number']})")
                elif series_info['fallback']:
                    # Use fallback if no standard series found
                    metadata['series_name'] = "" # No specific series name from fallback
                    metadata['volume_number'] = series_info['fallback'][0]['volume']
                    print(f"[DEBUG] get_book_metadata: Using fallback series volume: {metadata['volume_number']}")

                pub_year_node = root.find('.//marc:datafield[@tag="264"]/marc:subfield[@code="c"]', ns_marc) or root.find('.//marc:datafield[@tag="260"]/marc:subfield[@code="c"]', ns_marc)
                if pub_year_node is not None and pub_year_node.text:
                    years = re.findall(r'(1[7-9]\d{2}|20\d{2})', pub_year_node.text)
                    if years: metadata['publication_year'] = str(min([int(y) for y in years]))
                genre_nodes = root.findall('.//marc:datafield[@tag="655"]/marc:subfield[@code="a"]', ns_marc)
                metadata['genres'] = [node.text.strip() for node in genre_nodes if node.text]
                print(f"[DEBUG] get_book_metadata: Extracted Metadata: {metadata}")
            time.sleep(1) # Add a 1-second delay after a successful API call
            break # Break out of retry loop on success
        except requests.exceptions.RequestException as e:
            metadata['error'] = f"API request failed: {e}"
            print(f"[DEBUG] get_book_metadata: Request Exception: {e}")
            if attempt < MAX_RETRIES - 1:
                print(f"[INFO] Retrying in 5 seconds (Attempt {attempt + 2}/{MAX_RETRIES})...")
                time.sleep(5)
            else:
                print(f"[ERROR] Max retries reached for API request.")
        except etree.XMLSyntaxError as e:
            metadata['error'] = f"XML parsing error: {e}"
            print(f"[DEBUG] get_book_metadata: XML Parsing Error: {e}")
            break # No point in retrying if XML is malformed
        except Exception as e:
            metadata['error'] = f"An unexpected error occurred: {e}"
            print(f"[DEBUG] get_book_metadata: Unexpected Error: {e}")
            break # No point in retrying for unexpected errors

    # Always call Google Books API to supplement LOC data
    print(f"[INFO] Supplementing with Google Books API for {title} by {author}")
    google_metadata = get_book_metadata_google_books(title, author)
    if google_metadata:
        # Supplement, but don't overwrite existing data unless it's empty
        if not metadata.get('classification'):
            metadata['classification'] = google_metadata.get('classification', '')
        if not metadata.get('series_name'):
            metadata['series_name'] = google_metadata.get('series_name', '')
        if not metadata.get('volume_number'):
            metadata['volume_number'] = google_metadata.get('volume_number', '')
        if not metadata.get('publication_year'):
            metadata['publication_year'] = google_metadata.get('publication_year', '')

        # Combine genres, preventing duplicates
        existing_genres = set(metadata.get('genres', []))
        google_genres = google_metadata.get('genres', [])
        for genre in google_genres:
            existing_genres.add(genre)
        metadata['genres'] = list(existing_genres)

        if not metadata.get('main_category'):
            metadata['main_category'] = google_metadata.get('main_category', '')

        # If there was an error from LoC, but not from Google, clear the error
        if metadata.get('error') and not google_metadata.get('error'):
            metadata['error'] = None

        # Always store the raw google response
        metadata['google_raw_response'] = google_metadata.get('raw_response', '')

    cache[cache_key] = metadata
    return metadata


def get_book_metadata_google_books(title, author, isbn=None):
    print(f"\n--- Processing with Google Books API: Title='{title}', Author='{author}', ISBN='{isbn}' ---")
    base_url = "https://www.googleapis.com/books/v1/volumes"
    query_params = {}
    if isbn:
        query_params['q'] = f"isbn:{isbn}"
    elif title and author:
        query_params['q'] = f"intitle:{title}+inauthor:{author}"
    elif title:
        query_params['q'] = f"intitle:{title}"
    else:
        return {'classification': "", 'series_name': "", 'volume_number': "", 'publication_year': "", 'genres': [], 'error': "No sufficient query parameters for Google Books API", 'query': "", "raw_response": ''}

    query_params['maxResults'] = 1
    query_params['fields'] = "items(volumeInfo(title,authors,publishedDate,description,industryIdentifiers,categories,mainCategory,seriesInfo))"

    metadata = {'classification': "", 'series_name': "", 'volume_number': "", 'publication_year': "", 'genres': [], 'error': None, 'query': query_params.get('q', ''), "raw_response": '', 'main_category': '', 'google_series_name': '', 'google_volume_number': ''}

    try:
        response = requests.get(base_url, params=query_params, timeout=30)
        response.raise_for_status()
        data = response.json()
        metadata['raw_response'] = json.dumps(data, indent=4) # Store raw JSON response

        if 'items' in data and len(data['items']) > 0:
            volume_info = data['items'][0].get('volumeInfo', {})

            metadata['title'] = volume_info.get('title', '')
            metadata['authors'] = volume_info.get('authors', [])
            metadata['published_date'] = volume_info.get('publishedDate', '')
            metadata['description'] = volume_info.get('description', '')
            metadata['genres'] = volume_info.get('categories', [])
            metadata['main_category'] = volume_info.get('mainCategory', '')

            # Extract Subject from description
            if metadata['description']:
                subject_match = re.search(r'Subject:?\s*(.*)', metadata['description'])
                if subject_match:
                    subjects = [s.strip() for s in subject_match.group(1).split(',')]
                    metadata['genres'].extend(subjects)

            # Extract Subject from description
            if metadata['description']:
                subject_match = re.search(r'Subject:?\s*(.*)', metadata['description'])
                if subject_match:
                    subjects = [s.strip() for s in subject_match.group(1).split(',')]
                    metadata['genres'].extend(subjects)

            # Extract Subject from description
            if metadata['description']:
                subject_match = re.search(r'Subject:?\s*(.*)', metadata['description'])
                if subject_match:
                    subjects = [s.strip() for s in subject_match.group(1).split(',')]
                    metadata['genres'].extend(subjects)

            # Extract Subject from description
            if metadata['description']:
                subject_match = re.search(r'Subject:?\s*(.*)', metadata['description'])
                if subject_match:
                    subjects = [s.strip() for s in subject_match.group(1).split(',')]
                    metadata['genres'].extend(subjects)

            # Extract Subject from description
            if metadata['description']:
                subject_match = re.search(r'Subject:?\s*(.*)', metadata['description'])
                if subject_match:
                    subjects = [s.strip() for s in subject_match.group(1).split(',')]
                    metadata['genres'].extend(subjects)

            # Extract Subject from description
            if metadata['description']:
                subject_match = re.search(r'Subject:?\s*(.*)', metadata['description'])
                if subject_match:
                    subjects = [s.strip() for s in subject_match.group(1).split(',')]
                    metadata['genres'].extend(subjects)

            # Extract Subject from description
            if metadata['description']:
                subject_match = re.search(r'Subject:?\s*(.*)', metadata['description'])
                if subject_match:
                    subjects = [s.strip() for s in subject_match.group(1).split(',')]
                    metadata['genres'].extend(subjects)

            # Extract Subject from description
            if metadata['description']:
                subject_match = re.search(r'Subject:?\s*(.*)', metadata['description'])
                if subject_match:
                    subjects = [s.strip() for s in subject_match.group(1).split(',')]
                    metadata['genres'].extend(subjects)

            # Extract Subject from description
            if metadata['description']:
                subject_match = re.search(r'Subject:?\s*(.*)', metadata['description'])
                if subject_match:
                    subjects = [s.strip() for s in subject_match.group(1).split(',')]
                    metadata['genres'].extend(subjects)

            # Extract Subject from description
            if metadata['description']:
                subject_match = re.search(r'Subject:?\s*(.*)', metadata['description'])
                if subject_match:
                    subjects = [s.strip() for s in subject_match.group(1).split(',')]
                    metadata['genres'].extend(subjects)

            # Extract Subject from description
            if metadata['description']:
                subject_match = re.search(r'Subject:?\s*(.*)', metadata['description'])
                if subject_match:
                    subjects = [s.strip() for s in subject_match.group(1).split(',')]
                    metadata['genres'].extend(subjects)

            # Extract Subject from description
            if metadata['description']:
                subject_match = re.search(r'Subject:?\s*(.*)', metadata['description'])
                if subject_match:
                    subjects = [s.strip() for s in subject_match.group(1).split(',')]
                    metadata['genres'].extend(subjects)

            # Extract Subject from description
            if metadata['description']:
                subject_match = re.search(r'Subject:?\s*(.*)', metadata['description'])
                if subject_match:
                    subjects = [s.strip() for s in subject_match.group(1).split(',')]
                    metadata['genres'].extend(subjects)

            # Extract Subject from description
            if metadata['description']:
                subject_match = re.search(r'Subject:?\s*(.*)', metadata['description'])
                if subject_match:
                    subjects = [s.strip() for s in subject_match.group(1).split(',')]
                    metadata['genres'].extend(subjects)

            # Extract Subject from description
            if metadata['description']:
                subject_match = re.search(r'Subject:?\s*(.*)', metadata['description'])
                if subject_match:
                    subjects = [s.strip() for s in subject_match.group(1).split(',')]
                    metadata['genres'].extend(subjects)

            # Extract series info
            series_info = volume_info.get('seriesInfo', {})
            if series_info:
                # Google Books API seriesInfo structure can vary.
                # Common fields are 'bookDisplayNumber', 'seriesTitle'.
                # We'll try to extract a simple series title and a volume number.
                # This might need refinement based on actual API responses.
                metadata['google_series_name'] = series_info.get('seriesTitle', '')
                metadata['google_volume_number'] = series_info.get('bookDisplayNumber', '')

                # If we get series info from Google Books, use it for series_name and volume_number
                if metadata['google_series_name']:
                    metadata['series_name'] = metadata['google_series_name']
                if metadata['google_volume_number']:
                    metadata['volume_number'] = metadata['google_volume_number']

            # Extract publication year
            if metadata['published_date']:
                year_match = re.search(r'\b(1[7-9]\d{2}|20\d{2})\b', metadata['published_date'])
                if year_match:
                    metadata['publication_year'] = year_match.group(1)

            # Determine fiction based on categories or description
            combined_text_for_fiction = f"{metadata['title']} {' '.join(metadata['authors'])} {metadata['description']} {' '.join(metadata['genres'])}".lower()
            if is_likely_fiction(combined_text_for_fiction):
                metadata['classification'] = "FIC" # Use 'FIC' for fiction from Google Books

        else:
            metadata['error'] = "No results found in Google Books API."

    except requests.exceptions.RequestException as e:
        metadata['error'] = f"Google Books API request failed: {e}"
    except Exception as e:
        metadata['error'] = f"An unexpected error occurred with Google Books API: {e}"

    return metadata

# --- Main Test Script Logic ---


def run_test_script(csv_file_path):
    df = pd.read_csv(csv_file_path, encoding='latin1', dtype=str).fillna('')
    loc_cache = load_cache()

    processed_count = 0
    populated_call_numbers = 0
    results = []

    for index, row in df.iterrows():
        title = row.get('Title', '').strip()
        author = row.get("Author's Name", '').strip()

        print(f"\n--- Processing Row {index + 1}: Title='{title}', Author='{author}' ---")

        lc_meta = get_book_metadata(title, author, loc_cache)

        api_call_number = lc_meta.get('classification', '')
        call_no_050 = lc_meta.get('call_no_050', '')
        raw_response = lc_meta.get('raw_response', '')
        genres = lc_meta.get('genres', [])
        main_category = lc_meta.get('main_category', '') # Get main_category from lc_meta

        cleaned_call_number = clean_call_number(api_call_number, genres, raw_response, call_no_050, main_category)

        results.append({
            'Title': title,
            'Author': author,
            'API Call Number': api_call_number,
            '050 Call Number': call_no_050,
            'Cleaned Call Number': cleaned_call_number,
            'Series Name': lc_meta.get('series_name', ''),
            'Volume Number': lc_meta.get('volume_number', ''),
            'Error': lc_meta.get('error', ''),
            'Main Category (Google Books)': main_category # Add main_category to results
        })

        processed_count += 1
        if cleaned_call_number and not cleaned_call_number.startswith(SUGGESTION_FLAG):
            populated_call_numbers += 1
        elif cleaned_call_number.startswith(SUGGESTION_FLAG):
            print(f"[INFO] Call number populated via 050 fallback: {cleaned_call_number}")
            populated_call_numbers += 1
        else:
            print(f"[WARNING] Call number not populated for: {title} by {author}")

    save_cache(loc_cache)

    print("\n--- Processing Complete ---")
    print(f"Total records processed: {processed_count}")
    print(f"Call numbers populated: {populated_call_numbers}")
    if processed_count > 0:
        success_rate = (populated_call_numbers / processed_count) * 100
        print(f"Call number population success rate: {success_rate:.2f}%")
    else:
        print("No records processed.")

    # Optionally, save results to a new CSV
    pd.DataFrame(results).to_csv("test_loc_processing_results.csv", index=False)
    print("Results saved to test_loc_processing_results.csv")


if __name__ == "__main__":
    test_csv_path = "./test2.csv"
    if os.path.exists(test_csv_path):
        run_test_script(test_csv_path)
    else:
        print(f"Error: Test CSV file not found at {test_csv_path}")
