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
from io import StringIO
from bs4 import BeautifulSoup
import vertexai
from vertexai.generative_models import GenerativeModel

# --- Page Title ---
st.title("LOC API Processor")

# --- Feature List ---
st.header("Features")
st.markdown(r'''
- [x] CSV file uploading
- [x] Library of Congress API integration
- [x] Data cleaning and processing
- [x] Editable data table for manual classification
- [ ] PDF label generation
''')

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
            item = data["items"][0]
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

def get_vertex_ai_classification(title, author, vertex_ai_credentials):
    """Uses a Generative AI model on Vertex AI to classify a book's genre."""
    # st.write(f"Falling back to Vertex AI for genre classification for {title}...") # Removed st.write
    
    # Create a temporary file to store the credentials
    temp_creds_path = "temp_creds.json"
    try:
        credentials_json = json.dumps(vertex_ai_credentials)
        
        with open(temp_creds_path, "w") as f:
            f.write(credentials_json)

        # Set the environment variable for authentication
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = temp_creds_path
        
        vertexai.init(project=vertex_ai_credentials["project_id"], location="us-central1")
        model = GenerativeModel("gemini-pro")
        
        prompt = (
            f"What is the primary genre of the book titled '{title}' by '{author}'? "
            "Please provide only the genre name, without any additional text or explanation. "
            "For example: 'Science Fiction' or 'Historical Fiction'. If you cannot determine, say 'Unknown'."
        )
        
        response = model.generate_content(prompt)
        
        return response.text.strip()
    
    except Exception as e:
        # st.error(f"Error calling Vertex AI: {e}") # Removed st.error
        print(f"Error calling Vertex AI for {title}: {e}") # Use print for debugging in threads
        return "Unknown"
    finally:
        # Clean up the temporary file
        if os.path.exists(temp_creds_path):
            os.remove(temp_creds_path)


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
    if re.match(r'^8\\d{2}\\.5\\d*$', cleaned):
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

def get_book_metadata(title, author, cache, event, vertex_ai_credentials):
    print(f"**Debug: Entering get_book_metadata for:** {title}")
    safe_title = re.sub(r'[^a-zA-Z0-9\s\.:]', '', title)
    safe_author = re.sub(r'[^a-zA-Z0-9\s,]', '', author)

    metadata = {'classification': '', 'series_name': '', 'volume_number': '', 'publication_year': '', 'genres': [], 'google_genres': [], 'error': None}

    google_meta = get_book_metadata_google_books(title, author, cache)
    metadata.update(google_meta)

    if not metadata.get('google_genres'):
        print(f"**Debug: No genres in Google Books for {title}. Querying LOC.")
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

    # Fallback to Vertex AI if no classification found yet
    if not metadata.get('classification'):
        print(f"**Debug: No classification for {title}. Attempting Vertex AI classification.**")
        vertex_ai_classification_result = get_vertex_ai_classification(title, author, vertex_ai_credentials)
        if vertex_ai_classification_result and vertex_ai_classification_result != "Unknown":
            metadata['classification'] = vertex_ai_classification_result
            metadata['google_genres'].append(vertex_ai_classification_result) # Add to google_genres for consistency

    event.set()
    return metadata

def main():
    uploaded_file = st.file_uploader("Upload your Atriuum CSV Export", type="csv")

    if uploaded_file:
        df = pd.read_csv(uploaded_file, encoding='latin1', dtype=str).fillna('')
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

        results = []

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(get_book_metadata, row.get('Title', '').strip(), row.get("Author's Name", '').strip(), loc_cache, threading.Event(), vertex_ai_credentials): i for i, row in df.iterrows()}
            
            for i, future in enumerate(as_completed(futures)):
                row_index = futures[future]
                lc_meta = future.result()
                row = df.iloc[row_index]
                title = row.get('Title', '').strip()
                author = row.get("Author's Name", '').strip()
                
                api_call_number = lc_meta.get('classification', '')
                cleaned_call_number = clean_call_number(api_call_number, lc_meta.get('genres', []), lc_meta.get('google_genres', []), title=title)
                
                results.append({
                    'Title': title,
                    'Author': author,
                    'API Call Number': api_call_number,
                    'Cleaned Call Number': cleaned_call_number
                })
                progress_bar.progress((i + 1) / len(df))

        save_cache(loc_cache)
        
        st.write("Processing complete!")
        
        # Create a DataFrame from results
        results_df = pd.DataFrame(results)

        st.subheader("Processed Data")
        # Display editable DataFrame
        edited_df = st.data_editor(results_df, use_container_width=True, hide_index=True)

        if st.button("Apply Manual Classifications and Update Cache"):
            updated_count = 0
            current_cache = load_cache()
            for index, row in edited_df.iterrows():
                original_row = results_df.loc[index] # Get original row to form key
                title = original_row['Title'].strip()
                author = original_row['Author'].strip()
                manual_key = f"{re.sub(r'[^a-zA-Z0-9\s\.:]', '', title)}|{re.sub(r'[^a-zA-Z0-9\s,]', '', author)}".lower()

                if row['Cleaned Call Number'] != original_row['Cleaned Call Number']:
                    current_cache[manual_key] = row['Cleaned Call Number']
                    updated_count += 1
            save_cache(current_cache)
            st.success(f"Updated {updated_count} manual classifications in cache!")
            st.rerun()

if __name__ == "__main__":
    main()
