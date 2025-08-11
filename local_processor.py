import pandas as pd
import re
import requests
from lxml import etree
import time
import json
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- Constants & Cache ---
SUGGESTION_FLAG = "🐒"
CACHE_FILE = "loc_cache.json"
MANUAL_CLASSIFICATIONS = {
    "the old man and the sea|hemingway, ernest": "FIC",
    "are we living in the last days? : the second coming of jesus christ and interpreting the book of revelation|killens, chauncey s.": "236",
    "slow bullets|reynolds, alastair": "FIC",
    "nonviolent communication : a language of life|rosenberg, marshall b.": "303.69",
    "the genius prince's guide to raising a nation out of debt (hey how about treason?), vol. 3|toba, toru": "FIC",
    "the genius prince's guide to raising a nation out of debt (hey, how about treason?), vol. 4|toba, toru": "FIC",
    "the genius prince's guide to raising a nation out of debt (hey, how about treason?), vol. 5|toba, toru": "FIC",
    "the genius prince's guide to raising a nation out of debt (hey, how about treason?), vol. 6|toba, toru": "FIC",
    "the genius prince's guide to raising a nation out of debt (hey, how about treason?), vol. 7|toba, toru": "FIC",
    "genius prince's guide to raising a nation out of debt (hey, how about treason?), vol. 8 (light novel)|toba, toru": "FIC",
    "the power of now : a guide to spiritual enlightenment|tolle, eckhart": "204.4",
    "trauma-informed approach to library services|tolley, rebecca": "025.5",
    "the devil's arithmetic|yolen, jane": "FIC",
    "bonji yagkanatu (paperback)|": "FIC"
}

# --- Caching Functions ---
def load_cache():
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, 'r') as f:
            return json.load(f)
    return {}

def save_cache(cache):
    with open(CACHE_FILE, 'w') as f:
        json.dump(cache, f, indent=4)

# --- Helper Functions ---
def get_book_metadata_google_books(title, author, cache):
    """Fetches book metadata from the Google Books API."""
    safe_title = re.sub(r'[^a-zA-Z0-9\s\.:]', '', title)
    safe_author = re.sub(r'[^a-zA-Z0-9\s,]', '', author)
    cache_key = f"google_{safe_title}|{safe_author}".lower()
    if cache_key in cache:
        return cache[cache_key]

    metadata = {'google_genres': [], 'classification': '', 'error': None}
    try:
        query = f'intitle:"{safe_title}"+inauthor:"{safe_author}"'
        url = f"https://www.googleapis.com/books/v1/volumes?q={query}&maxResults=1"
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        data = response.json()

        if "items" in data and data["items"]:
            item = data["items"].get(0)
            volume_info = item.get("volumeInfo", {})

            if "categories" in volume_info:
                metadata['google_genres'].extend(volume_info["categories"])
            
            if "description" in volume_info:
                description = volume_info["description"]
                match = re.search(r'Subject: (.*?)(?:\n|$)', description, re.IGNORECASE)
                if match:
                    subjects = [s.strip() for s in match.group(1).split(',')]
                    metadata['google_genres'].extend(subjects)

        cache[cache_key] = metadata
        return metadata

    except requests.exceptions.RequestException as e:
        metadata['error'] = f"Google Books API request failed: {e}"
    except Exception as e:
        metadata['error'] = f"An unexpected error occurred with Google Books API: {e}"
    return metadata

def clean_call_number(call_num_str, genres, google_genres=None, title=""):
    if google_genres is None:
        google_genres = []
        
    if not isinstance(call_num_str, str):
        return ""
    cleaned = call_num_str.strip().lstrip(SUGGESTION_FLAG)
    cleaned = cleaned.replace('/', '')

    # Prioritize Google Books categories
    if any("fiction" in g.lower() for g in google_genres):
        return "FIC"

    if cleaned.upper().startswith("FIC"):
        return "FIC"
    if re.match(r'^8\\d{2}\\.\\d*$', cleaned):
        return "FIC"
    # Check for fiction genres
    fiction_genres = ["fiction", "novel", "stories"]
    if any(genre.lower() in fiction_genres for genre in genres):
        return "FIC"
    
    # Fallback to title check
    if any(keyword in title.lower() for keyword in ["novel", "stories", "a novel"]):
        return "FIC"
        
    match = re.match(r'^(\d+(\.\d+)?)', cleaned)
    if match:
        return match.group(1)
    return cleaned

def get_book_metadata(title, author, cache, event):
    print(f"**Debug: Entering get_book_metadata for:** {title}")
    safe_title = re.sub(r'[^a-zA-Z0-9\s\.:]', '', title)
    safe_author = re.sub(r'[^a-zA-Z0-9\s,]', '', author)
    manual_key = f"{safe_title}|{safe_author}".lower()

    if manual_key in MANUAL_CLASSIFICATIONS:
        print(f"**Debug: Found manual classification for {title}.**")
        metadata = {
            'classification': MANUAL_CLASSIFICATIONS[manual_key],
            'series_name': "", 
            'volume_number': "", 
            'publication_year': "", 
            'genres': [], 
            'google_genres': [], 
            'error': None
        }
        event.set()
        return metadata

    metadata = {'classification': "", 'series_name': "", 'volume_number': "", 'publication_year': "", 'genres': [], 'google_genres': [], 'error': None}

    google_meta = get_book_metadata_google_books(title, author, cache)
    metadata.update(google_meta)

    if not metadata.get('google_genres'):
        print(f"**Debug: No genres in Google Books for {title}. Querying LOC.**")
        loc_cache_key = f"loc_{safe_title}|{safe_author}".lower()
        if loc_cache_key in cache:
            cached_loc_meta = cache[loc_cache_key]
            metadata.update(cached_loc_meta)
        else:
            base_url = "http://lx2.loc.gov:210/LCDB"
            query = f'bath.title="{safe_title}" and bath.author="{safe_author}"'
            params = {"version": "1.1", "operation": "searchRetrieve", "query": query, "maximumRecords": "1", "recordSchema": "marcxml"}
            
            retry_delays = [5, 15, 30]
            for i in range(len(retry_delays) + 1):
                try:
                    response = requests.get(base_url, params=params, timeout=20)
                    response.raise_for_status()
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
                        pub_year_node = root.find('.//marc:datafield[@tag="264"]/marc:subfield[@code="c"]', ns_marc) or root.find('.//marc:datafield[@tag="260"]/marc:subfield[@code="c"]', ns_marc)
                        if pub_year_node is not None and pub_year_node.text:
                            years = re.findall(r'(1[7-9]\d{2}|20\d{2})', pub_year_node.text)
                            if years: metadata['publication_year'] = str(min([int(y) for y in years]))
                        genre_nodes = root.findall('.//marc:datafield[@tag="655"]/marc:subfield[@code="a"]', ns_marc)
                        if genre_nodes:
                            metadata['genres'] = [g.text.strip().rstrip('.') for g in genre_nodes]
                        
                        cache[loc_cache_key] = metadata
                    break # Exit retry loop on success
                except requests.exceptions.RequestException as e:
                    if i < len(retry_delays):
                        print(f"LOC API call failed for {title}. Retrying in {retry_delays[i]}s...")
                        time.sleep(retry_delays[i])
                        continue
                    metadata['error'] = f"LOC API request failed after retries: {e}"
                    print(f"**Debug: LOC failed for {title}, returning what we have.**")
                except Exception as e:
                    metadata['error'] = f"An unexpected error occurred with LOC API: {e}"
                    print(f"**Debug: Unexpected LOC error for {title}, returning what we have.**")
                    break

    if not metadata.get('classification'):
        print(f"**Debug: No classification for {title}. Performing web search.**")
        try:
            search_results = default_api.google_web_search(query=f"{title} by {author} genre classification")
            if any(keyword in str(search_results).lower() for keyword in ["fiction", "novel", "stories"]):
                metadata['classification'] = "FIC"
                metadata['google_genres'].append("Fiction")
        except Exception as e:
            print(f"**Debug: Web search failed for {title}: {e}**")

    event.set()
    return metadata

def main():
    df = pd.read_csv("test2.csv", encoding='latin1', dtype=str).fillna('')
    loc_cache = load_cache()
    
    print("Title\tAuthor\tAPI Call Number\tCleaned Call Number")

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(get_book_metadata, row.get('Title', '').strip(), row.get("Author's Name", '').strip(), loc_cache, threading.Event()): i for i, row in df.iterrows()}
        
        for future in as_completed(futures):
            i = futures[future]
            lc_meta = future.result()
            row = df.iloc[i]
            title = row.get('Title', '').strip()
            author = row.get("Author's Name", '').strip()
            
            api_call_number = lc_meta.get('classification', '')
            cleaned_call_number = clean_call_number(api_call_number, lc_meta.get('genres', []), lc_meta.get('google_genres', []), title=title)
            
            print(f"{title}\t{author}\t{api_call_number}\t{cleaned_call_number}")

    save_cache(loc_cache)

if __name__ == "__main__":
    main()