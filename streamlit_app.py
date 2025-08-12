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
import io

# --- Logging Setup ---
log_capture_string = io.StringIO()
st_logger = logging.getLogger()
st_logger.setLevel(logging.DEBUG)
st_handler = logging.StreamHandler(log_capture_string)
st_handler.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
st_logger.addHandler(st_handler)
from bs4 import BeautifulSoup
import vertexai
from vertexai.generative_models import GenerativeModel

# --- Page Title ---
st.title("Atriuum Label Generator")
st.caption("Last updated: 2025-08-11 21:52:02 PDT-0700")

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
        st.write(f"DEBUG: Google Books cache hit for '{title}' by '{author}'.")
        return cache[cache_key]

    metadata = {'google_genres': [], 'classification': '', 'error': None}
    try:
        query = f'intitle:"{safe_title}"+inauthor:"{safe_author}"'
        url = f"https://www.googleapis.com/books/v1/volumes?q={query}&maxResults=1"
        st_logger.debug(f"Google Books query for '{title}' by '{author}': {url}")
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        data = response.json()
        st_logger.debug(f"Google Books raw response for '{title}' by '{author}': {response.text}")

        if "items" in data and data["items"]:
            item = data["items"][0] # Corrected line
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
            "For each book in the following list, provide its primary genre or Dewey Decimal classification. "
            "If it's fiction, classify as 'FIC'. If non-fiction, provide a general Dewey Decimal category like '300' for Social Sciences, '500' for Science, etc. "
            "Provide the output as a JSON array of objects, where each object has 'title', 'author', and 'classification' fields. "
            "If you cannot determine, use 'Unknown'.\n\n" +
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
                
                # Create a dictionary for easy lookup
                classified_results = {}
                for item in classifications:
                    key = f"{item['title']}|{item['author']}".lower()
                    classified_results[key] = item.get('classification', 'Unknown')
                return classified_results
            except Exception as e:
                if i < len(retry_delays):
                    st_logger.warning(f"Vertex AI batch call failed: {e}. Retrying in {retry_delays[i]} seconds...")
                    time.sleep(retry_delays[i])
                else:
                    st_logger.error(f"Vertex AI batch call failed after multiple retries: {e}")
                    return {f"{book['title']}|{book['author']}".lower(): "Unknown" for book in batch_books}
    finally:
        if os.path.exists(temp_creds_path):
            os.remove(temp_creds_path)


def clean_call_number(call_num_str, genres, google_genres=None, title="", is_original_data=False):
    st_logger.debug(f"clean_call_number input: call_num_str='{call_num_str}', genres={genres}, google_genres={google_genres}, title='{title}', is_original_data={is_original_data}")
    if google_genres is None:
        google_genres = []
        
    if not isinstance(call_num_str, str):
        st_logger.debug(f"clean_call_number returning UNKNOWN for non-string input: {call_num_str}")
        return "UNKNOWN" # Default for non-string input

    cleaned = call_num_str.strip()
    # Only remove suggestion flag if it's not original data
    if not is_original_data:
        cleaned = cleaned.lstrip(SUGGESTION_FLAG)
    cleaned = cleaned.replace('/', '')

    # Prioritize Google Books categories and other genre lists for FIC
    fiction_keywords_all = ["fiction", "fantasy", "science fiction", "thriller", "mystery", "romance", "horror", "novel", "stories", "a novel", "young adult fiction", "historical fiction", "literary fiction"]
    if any(g.lower() in fiction_keywords_all for g in google_genres) or \
       any(genre.lower() in fiction_keywords_all for genre in genres) or \
       any(keyword in title.lower() for keyword in fiction_keywords_all):
        st_logger.debug(f"clean_call_number returning FIC based on genre/title keywords: {cleaned}")
        return "FIC"

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
    if re.match(r'^\\d{3}(\\.\\d+)?

def get_book_metadata_initial_pass(title, author, cache, event):
    st_logger.debug(f"Entering get_book_metadata_initial_pass for: {title}")
    safe_title = re.sub(r'[^a-zA-Z0-9\s\.:]', '', title)
    safe_author = re.sub(r'[^a-zA-Z0-9\s,]', '', author)

    metadata = {'classification': '', 'series_name': '', 'volume_number': '', 'publication_year': '', 'genres': [], 'google_genres': [], 'error': None}

    google_meta = get_book_metadata_google_books(title, author, cache)
    metadata.update(google_meta)

    if not metadata.get('google_genres'):
        st_logger.debug(f"No genres in Google Books for {title}. Querying LOC.")
        loc_cache_key = f"loc_{safe_title}|{safe_author}".lower()
        if loc_cache_key in cache:
            st_logger.debug(f"LOC cache hit for '{title}' by '{author}'.")
            cached_loc_meta = cache[loc_cache_key]
            metadata.update(cached_loc_meta)
        else:
            base_url = "http://lx2.loc.gov:210/LCDB"
            query = f'bath.title="{safe_title}" and bath.author="{safe_author}"'
            params = {"version": "1.1", "operation": "searchRetrieve", "query": query, "maximumRecords": "1", "recordSchema": "marcxml"}
            st_logger.debug(f"LOC query for '{title}' by '{author}': {base_url}?{requests.compat.urlencode(params)})")
            
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
                        pub_year_node = root.find('.//marc:datafield[@tag="264"]/marc:subfield[@code="c"]', ns_marc) or root.find('.//marc:datafield[@tag="260"]/marc:subfield[@code="c"]', ns_marc)
                        if pub_year_node is not None and pub_year_node.text:
                            years = re.findall(r'(1[7-9]\d{2}|20\d{2})', pub_year_node.text)
                            if years: metadata['publication_year'] = str(min([int(y) for y in years]))
                        genre_nodes = root.findall('.//marc:datafield[@tag="655"]/marc:subfield[@code="a"]', ns_marc)
                        if genre_nodes:
                            metadata['genres'] = [g.text.strip().rstrip('.') for g in genre_nodes]
                        
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

    event.set()
    return metadata

def generate_pdf_labels(df):
    buffer = StringIO()
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
    return buffer.getvalue()

def extract_year(date_string):
    """Extracts the first 4-digit number from a string, assuming it's a year."""
    if isinstance(date_string, str):
        match = re.search(r'\\b(1[7-9]\d{2}|20\d{2})\\b', date_string)
        if match:
            return match.group(1)
    return ""

def main():
    st_logger.debug("Streamlit app main function started.")
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
        unclassified_books_for_vertex_ai = [] # To collect books for batch Vertex AI processing

        # First pass: Process with Google Books and LOC APIs
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(get_book_metadata_initial_pass, row.get('Title', '').strip(), row.get("Author's Name", '').strip(), loc_cache, threading.Event()): i for i, row in df.iterrows()}
            
            for i, future in enumerate(as_completed(futures)):
                row_index = futures[future]
                lc_meta = future.result()
                row = df.iloc[row_index]
                title = row.get('Title', '').strip()
                author = row.get("Author's Name", '').strip()
                
                # Original Atriuum data
                original_holding_barcode = row.get('Holdings Barcode', '').strip()
                raw_original_call_number = row.get('Call Number', '').strip() # Get raw original
                cleaned_original_call_number = clean_call_number(raw_original_call_number, [], [], title="", is_original_data=True) # Clean original
                original_series_name = row.get('Series Title', '').strip()
                original_series_number = row.get('Series Number', '').strip()
                original_copyright_year = extract_year(row.get('Copyright', '').strip())
                original_publication_date_year = extract_year(row.get('Publication Date', '').strip())

                # Prioritize original copyright/publication year
                final_original_year = ""
                if original_copyright_year: final_original_year = original_copyright_year
                elif original_publication_date_year: final_original_year = original_publication_date_year

                # Mashed-up data from initial pass
                api_call_number = lc_meta.get('classification', '')
                cleaned_call_number = clean_call_number(api_call_number, lc_meta.get('genres', []), lc_meta.get('google_genres', []), title=title)
                mashed_series_name = lc_meta.get('series_name', '').strip()
                mashed_volume_number = lc_meta.get('volume_number', '').strip()
                mashed_publication_year = lc_meta.get('publication_year', '').strip()

                # Merge logic for initial pass
                # Use cleaned_original_call_number if valid, else fallback to mashed-up
                current_call_number = cleaned_original_call_number if cleaned_original_call_number else (SUGGESTION_FLAG + cleaned_call_number if cleaned_call_number else "")
                current_series_name = original_series_name if original_series_name else (SUGGESTION_FLAG + mashed_series_name if mashed_series_name else '')
                current_series_number = original_series_number if original_series_number else (SUGGESTION_FLAG + mashed_volume_number if mashed_volume_number else '')
                current_publication_year = final_original_year if final_original_year else (SUGGESTION_FLAG + mashed_publication_year if mashed_publication_year else '')

                # Collect for Vertex AI batch processing if still unclassified
                if not current_call_number or current_call_number == "UNKNOWN":
                    unclassified_books_for_vertex_ai.append({
                        'title': title,
                        'author': author,
                        'row_index': row_index, # Keep track of original row index
                        'lc_meta': lc_meta # Keep original metadata for later merging
                    })
                
                results.append({
                    'Title': title,
                    'Author': author,
                    'Holdings Barcode': original_holding_barcode,
                    'Call Number': current_call_number,
                    'Series Info': current_series_name, # Will be populated after all processing
                    'Series Number': current_series_number,
                    'Copyright Year': current_publication_year
                })
                progress_bar.progress((i + 1) / len(df))

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
                    
                    for book_data in processed_batch:
                        title = book_data['title']
                        author = book_data['author']
                        row_index = book_data['row_index']
                        lc_meta = book_data['lc_meta']
                        
                        key = f"{title}|{author}".lower()
                        vertex_ai_classification = batch_classifications.get(key, "Unknown")

                        # Update the classification in lc_meta for this book
                        if vertex_ai_classification and vertex_ai_classification != "Unknown":
                            lc_meta['classification'] = vertex_ai_classification
                            # Ensure google_genres is a list before appending
                            if 'google_genres' not in lc_meta or not isinstance(lc_meta['google_genres'], list):
                                lc_meta['google_genres'] = []
                            lc_meta['google_genres'].append(vertex_ai_classification) # Add to google_genres for consistency
                        
                        # Re-clean call number with new Vertex AI classification
                        final_call_number_after_vertex_ai = clean_call_number(lc_meta.get('classification', ''), lc_meta.get('genres', []), lc_meta.get('google_genres', []), title=title)
                        
                        # Update the results list with the new classification
                        results[row_index]['Call Number'] = (SUGGESTION_FLAG + final_call_number_after_vertex_ai if final_call_number_after_vertex_ai else "")

        # Final pass to populate Series Info and ensure all fields are non-blank
        for i, row_data in enumerate(results):
            # Re-combine series name and volume number for display after all processing
            final_series_name = row_data['Series Info'] # This was populated with mashed_series_name
            final_series_number = row_data['Series Number']

            series_info = ""
            if final_series_name and final_series_number:
                series_info = f"{final_series_name}, Vol. {final_series_number}"
            elif final_series_name:
                series_info = final_series_name
            elif final_series_number:
                series_info = f"Vol. {final_series_number}"
            
            results[i]['Series Info'] = series_info

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

                # Update cache only if the cleaned call number has changed
                if row['Call Number'] != original_row['Call Number']:
                    # Remove monkey emoji before saving to cache
                    current_cache[manual_key] = row['Call Number'].replace(SUGGESTION_FLAG, '')
                    updated_count += 1
            save_cache(current_cache)
            st.success(f"Updated {updated_count} manual classifications in cache!")
            st.rerun()

        # PDF Generation Section
        st.subheader("Generate PDF Labels")
        if st.button("Generate PDF"):
            pdf_output = generate_pdf_labels(edited_df)
            st.download_button(
                label="Download PDF Labels",
                data=pdf_output,
                file_name="book_labels.pdf",
                mime="application/pdf"
            )

    with st.expander("Debug Log"): # Add this block
        st.download_button(
            label="Download Full Debug Log",
            data=log_capture_string.getvalue(),
            file_name="debug_log.txt",
            mime="text/plain"
        )

if __name__ == "__main__":
    main(), cleaned):
        st_logger.debug(f"clean_call_number returning Dewey Decimal: {cleaned}")
        return cleaned
    
    # If it's a number but not 3 digits, still return it (e.g., from LOC 050 field like "PS3515.E37"), keep it as is
    # This allows for LC call numbers to pass through if they are numeric-like
    if re.match(r'^[A-Z]{1,3}\\d+(\\.\\d+)?

def get_book_metadata_initial_pass(title, author, cache, event):
    st_logger.debug(f"Entering get_book_metadata_initial_pass for: {title}")
    safe_title = re.sub(r'[^a-zA-Z0-9\s\.:]', '', title)
    safe_author = re.sub(r'[^a-zA-Z0-9\s,]', '', author)

    metadata = {'classification': '', 'series_name': '', 'volume_number': '', 'publication_year': '', 'genres': [], 'google_genres': [], 'error': None}

    google_meta = get_book_metadata_google_books(title, author, cache)
    metadata.update(google_meta)

    if not metadata.get('google_genres'):
        st_logger.debug(f"No genres in Google Books for {title}. Querying LOC.")
        loc_cache_key = f"loc_{safe_title}|{safe_author}".lower()
        if loc_cache_key in cache:
            st_logger.debug(f"LOC cache hit for '{title}' by '{author}'.")
            cached_loc_meta = cache[loc_cache_key]
            metadata.update(cached_loc_meta)
        else:
            base_url = "http://lx2.loc.gov:210/LCDB"
            query = f'bath.title="{safe_title}" and bath.author="{safe_author}"'
            params = {"version": "1.1", "operation": "searchRetrieve", "query": query, "maximumRecords": "1", "recordSchema": "marcxml"}
            st_logger.debug(f"LOC query for '{title}' by '{author}': {base_url}?{requests.compat.urlencode(params)})")
            
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
                        pub_year_node = root.find('.//marc:datafield[@tag="264"]/marc:subfield[@code="c"]', ns_marc) or root.find('.//marc:datafield[@tag="260"]/marc:subfield[@code="c"]', ns_marc)
                        if pub_year_node is not None and pub_year_node.text:
                            years = re.findall(r'(1[7-9]\d{2}|20\d{2})', pub_year_node.text)
                            if years: metadata['publication_year'] = str(min([int(y) for y in years]))
                        genre_nodes = root.findall('.//marc:datafield[@tag="655"]/marc:subfield[@code="a"]', ns_marc)
                        if genre_nodes:
                            metadata['genres'] = [g.text.strip().rstrip('.') for g in genre_nodes]
                        
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

    event.set()
    return metadata

def generate_pdf_labels(df):
    buffer = StringIO()
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
    return buffer.getvalue()

def extract_year(date_string):
    """Extracts the first 4-digit number from a string, assuming it's a year."""
    if isinstance(date_string, str):
        match = re.search(r'\\b(1[7-9]\d{2}|20\d{2})\\b', date_string)
        if match:
            return match.group(1)
    return ""

def main():
    st_logger.debug("Streamlit app main function started.")
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
        unclassified_books_for_vertex_ai = [] # To collect books for batch Vertex AI processing

        # First pass: Process with Google Books and LOC APIs
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(get_book_metadata_initial_pass, row.get('Title', '').strip(), row.get("Author's Name", '').strip(), loc_cache, threading.Event()): i for i, row in df.iterrows()}
            
            for i, future in enumerate(as_completed(futures)):
                row_index = futures[future]
                lc_meta = future.result()
                row = df.iloc[row_index]
                title = row.get('Title', '').strip()
                author = row.get("Author's Name", '').strip()
                
                # Original Atriuum data
                original_holding_barcode = row.get('Holdings Barcode', '').strip()
                raw_original_call_number = row.get('Call Number', '').strip() # Get raw original
                cleaned_original_call_number = clean_call_number(raw_original_call_number, [], [], title="", is_original_data=True) # Clean original
                original_series_name = row.get('Series Title', '').strip()
                original_series_number = row.get('Series Number', '').strip()
                original_copyright_year = extract_year(row.get('Copyright', '').strip())
                original_publication_date_year = extract_year(row.get('Publication Date', '').strip())

                # Prioritize original copyright/publication year
                final_original_year = ""
                if original_copyright_year: final_original_year = original_copyright_year
                elif original_publication_date_year: final_original_year = original_publication_date_year

                # Mashed-up data from initial pass
                api_call_number = lc_meta.get('classification', '')
                cleaned_call_number = clean_call_number(api_call_number, lc_meta.get('genres', []), lc_meta.get('google_genres', []), title=title)
                mashed_series_name = lc_meta.get('series_name', '').strip()
                mashed_volume_number = lc_meta.get('volume_number', '').strip()
                mashed_publication_year = lc_meta.get('publication_year', '').strip()

                # Merge logic for initial pass
                # Use cleaned_original_call_number if valid, else fallback to mashed-up
                current_call_number = cleaned_original_call_number if cleaned_original_call_number else (SUGGESTION_FLAG + cleaned_call_number if cleaned_call_number else "")
                current_series_name = original_series_name if original_series_name else (SUGGESTION_FLAG + mashed_series_name if mashed_series_name else '')
                current_series_number = original_series_number if original_series_number else (SUGGESTION_FLAG + mashed_volume_number if mashed_volume_number else '')
                current_publication_year = final_original_year if final_original_year else (SUGGESTION_FLAG + mashed_publication_year if mashed_publication_year else '')

                # Collect for Vertex AI batch processing if still unclassified
                if not current_call_number or current_call_number == "UNKNOWN":
                    unclassified_books_for_vertex_ai.append({
                        'title': title,
                        'author': author,
                        'row_index': row_index, # Keep track of original row index
                        'lc_meta': lc_meta # Keep original metadata for later merging
                    })
                
                results.append({
                    'Title': title,
                    'Author': author,
                    'Holdings Barcode': original_holding_barcode,
                    'Call Number': current_call_number,
                    'Series Info': current_series_name, # Will be populated after all processing
                    'Series Number': current_series_number,
                    'Copyright Year': current_publication_year
                })
                progress_bar.progress((i + 1) / len(df))

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
                    
                    for book_data in processed_batch:
                        title = book_data['title']
                        author = book_data['author']
                        row_index = book_data['row_index']
                        lc_meta = book_data['lc_meta']
                        
                        key = f"{title}|{author}".lower()
                        vertex_ai_classification = batch_classifications.get(key, "Unknown")

                        # Update the classification in lc_meta for this book
                        if vertex_ai_classification and vertex_ai_classification != "Unknown":
                            lc_meta['classification'] = vertex_ai_classification
                            # Ensure google_genres is a list before appending
                            if 'google_genres' not in lc_meta or not isinstance(lc_meta['google_genres'], list):
                                lc_meta['google_genres'] = []
                            lc_meta['google_genres'].append(vertex_ai_classification) # Add to google_genres for consistency
                        
                        # Re-clean call number with new Vertex AI classification
                        final_call_number_after_vertex_ai = clean_call_number(lc_meta.get('classification', ''), lc_meta.get('genres', []), lc_meta.get('google_genres', []), title=title)
                        
                        # Update the results list with the new classification
                        results[row_index]['Call Number'] = (SUGGESTION_FLAG + final_call_number_after_vertex_ai if final_call_number_after_vertex_ai else "")

        # Final pass to populate Series Info and ensure all fields are non-blank
        for i, row_data in enumerate(results):
            # Re-combine series name and volume number for display after all processing
            final_series_name = row_data['Series Info'] # This was populated with mashed_series_name
            final_series_number = row_data['Series Number']

            series_info = ""
            if final_series_name and final_series_number:
                series_info = f"{final_series_name}, Vol. {final_series_number}"
            elif final_series_name:
                series_info = final_series_name
            elif final_series_number:
                series_info = f"Vol. {final_series_number}"
            
            results[i]['Series Info'] = series_info

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

                # Update cache only if the cleaned call number has changed
                if row['Call Number'] != original_row['Call Number']:
                    # Remove monkey emoji before saving to cache
                    current_cache[manual_key] = row['Call Number'].replace(SUGGESTION_FLAG, '')
                    updated_count += 1
            save_cache(current_cache)
            st.success(f"Updated {updated_count} manual classifications in cache!")
            st.rerun()

        # PDF Generation Section
        st.subheader("Generate PDF Labels")
        if st.button("Generate PDF"):
            pdf_output = generate_pdf_labels(edited_df)
            st.download_button(
                label="Download PDF Labels",
                data=pdf_output,
                file_name="book_labels.pdf",
                mime="application/pdf"
            )

    with st.expander("Debug Log"): # Add this block
        st.download_button(
            label="Download Full Debug Log",
            data=log_capture_string.getvalue(),
            file_name="debug_log.txt",
            mime="text/plain"
        )

if __name__ == "__main__":
    main(), cleaned) or re.match(r'^\\d+(\\.\\d+)?

def get_book_metadata_initial_pass(title, author, cache, event):
    st_logger.debug(f"Entering get_book_metadata_initial_pass for: {title}")
    safe_title = re.sub(r'[^a-zA-Z0-9\s\.:]', '', title)
    safe_author = re.sub(r'[^a-zA-Z0-9\s,]', '', author)

    metadata = {'classification': '', 'series_name': '', 'volume_number': '', 'publication_year': '', 'genres': [], 'google_genres': [], 'error': None}

    google_meta = get_book_metadata_google_books(title, author, cache)
    metadata.update(google_meta)

    if not metadata.get('google_genres'):
        st_logger.debug(f"No genres in Google Books for {title}. Querying LOC.")
        loc_cache_key = f"loc_{safe_title}|{safe_author}".lower()
        if loc_cache_key in cache:
            st_logger.debug(f"LOC cache hit for '{title}' by '{author}'.")
            cached_loc_meta = cache[loc_cache_key]
            metadata.update(cached_loc_meta)
        else:
            base_url = "http://lx2.loc.gov:210/LCDB"
            query = f'bath.title="{safe_title}" and bath.author="{safe_author}"'
            params = {"version": "1.1", "operation": "searchRetrieve", "query": query, "maximumRecords": "1", "recordSchema": "marcxml"}
            st_logger.debug(f"LOC query for '{title}' by '{author}': {base_url}?{requests.compat.urlencode(params)})")
            
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
                        pub_year_node = root.find('.//marc:datafield[@tag="264"]/marc:subfield[@code="c"]', ns_marc) or root.find('.//marc:datafield[@tag="260"]/marc:subfield[@code="c"]', ns_marc)
                        if pub_year_node is not None and pub_year_node.text:
                            years = re.findall(r'(1[7-9]\d{2}|20\d{2})', pub_year_node.text)
                            if years: metadata['publication_year'] = str(min([int(y) for y in years]))
                        genre_nodes = root.findall('.//marc:datafield[@tag="655"]/marc:subfield[@code="a"]', ns_marc)
                        if genre_nodes:
                            metadata['genres'] = [g.text.strip().rstrip('.') for g in genre_nodes]
                        
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

    event.set()
    return metadata

def generate_pdf_labels(df):
    buffer = StringIO()
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
    return buffer.getvalue()

def extract_year(date_string):
    """Extracts the first 4-digit number from a string, assuming it's a year."""
    if isinstance(date_string, str):
        match = re.search(r'\\b(1[7-9]\d{2}|20\d{2})\\b', date_string)
        if match:
            return match.group(1)
    return ""

def main():
    st_logger.debug("Streamlit app main function started.")
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
        unclassified_books_for_vertex_ai = [] # To collect books for batch Vertex AI processing

        # First pass: Process with Google Books and LOC APIs
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(get_book_metadata_initial_pass, row.get('Title', '').strip(), row.get("Author's Name", '').strip(), loc_cache, threading.Event()): i for i, row in df.iterrows()}
            
            for i, future in enumerate(as_completed(futures)):
                row_index = futures[future]
                lc_meta = future.result()
                row = df.iloc[row_index]
                title = row.get('Title', '').strip()
                author = row.get("Author's Name", '').strip()
                
                # Original Atriuum data
                original_holding_barcode = row.get('Holdings Barcode', '').strip()
                raw_original_call_number = row.get('Call Number', '').strip() # Get raw original
                cleaned_original_call_number = clean_call_number(raw_original_call_number, [], [], title="", is_original_data=True) # Clean original
                original_series_name = row.get('Series Title', '').strip()
                original_series_number = row.get('Series Number', '').strip()
                original_copyright_year = extract_year(row.get('Copyright', '').strip())
                original_publication_date_year = extract_year(row.get('Publication Date', '').strip())

                # Prioritize original copyright/publication year
                final_original_year = ""
                if original_copyright_year: final_original_year = original_copyright_year
                elif original_publication_date_year: final_original_year = original_publication_date_year

                # Mashed-up data from initial pass
                api_call_number = lc_meta.get('classification', '')
                cleaned_call_number = clean_call_number(api_call_number, lc_meta.get('genres', []), lc_meta.get('google_genres', []), title=title)
                mashed_series_name = lc_meta.get('series_name', '').strip()
                mashed_volume_number = lc_meta.get('volume_number', '').strip()
                mashed_publication_year = lc_meta.get('publication_year', '').strip()

                # Merge logic for initial pass
                # Use cleaned_original_call_number if valid, else fallback to mashed-up
                current_call_number = cleaned_original_call_number if cleaned_original_call_number else (SUGGESTION_FLAG + cleaned_call_number if cleaned_call_number else "")
                current_series_name = original_series_name if original_series_name else (SUGGESTION_FLAG + mashed_series_name if mashed_series_name else '')
                current_series_number = original_series_number if original_series_number else (SUGGESTION_FLAG + mashed_volume_number if mashed_volume_number else '')
                current_publication_year = final_original_year if final_original_year else (SUGGESTION_FLAG + mashed_publication_year if mashed_publication_year else '')

                # Collect for Vertex AI batch processing if still unclassified
                if not current_call_number or current_call_number == "UNKNOWN":
                    unclassified_books_for_vertex_ai.append({
                        'title': title,
                        'author': author,
                        'row_index': row_index, # Keep track of original row index
                        'lc_meta': lc_meta # Keep original metadata for later merging
                    })
                
                results.append({
                    'Title': title,
                    'Author': author,
                    'Holdings Barcode': original_holding_barcode,
                    'Call Number': current_call_number,
                    'Series Info': current_series_name, # Will be populated after all processing
                    'Series Number': current_series_number,
                    'Copyright Year': current_publication_year
                })
                progress_bar.progress((i + 1) / len(df))

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
                    
                    for book_data in processed_batch:
                        title = book_data['title']
                        author = book_data['author']
                        row_index = book_data['row_index']
                        lc_meta = book_data['lc_meta']
                        
                        key = f"{title}|{author}".lower()
                        vertex_ai_classification = batch_classifications.get(key, "Unknown")

                        # Update the classification in lc_meta for this book
                        if vertex_ai_classification and vertex_ai_classification != "Unknown":
                            lc_meta['classification'] = vertex_ai_classification
                            # Ensure google_genres is a list before appending
                            if 'google_genres' not in lc_meta or not isinstance(lc_meta['google_genres'], list):
                                lc_meta['google_genres'] = []
                            lc_meta['google_genres'].append(vertex_ai_classification) # Add to google_genres for consistency
                        
                        # Re-clean call number with new Vertex AI classification
                        final_call_number_after_vertex_ai = clean_call_number(lc_meta.get('classification', ''), lc_meta.get('genres', []), lc_meta.get('google_genres', []), title=title)
                        
                        # Update the results list with the new classification
                        results[row_index]['Call Number'] = (SUGGESTION_FLAG + final_call_number_after_vertex_ai if final_call_number_after_vertex_ai else "")

        # Final pass to populate Series Info and ensure all fields are non-blank
        for i, row_data in enumerate(results):
            # Re-combine series name and volume number for display after all processing
            final_series_name = row_data['Series Info'] # This was populated with mashed_series_name
            final_series_number = row_data['Series Number']

            series_info = ""
            if final_series_name and final_series_number:
                series_info = f"{final_series_name}, Vol. {final_series_number}"
            elif final_series_name:
                series_info = final_series_name
            elif final_series_number:
                series_info = f"Vol. {final_series_number}"
            
            results[i]['Series Info'] = series_info

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

                # Update cache only if the cleaned call number has changed
                if row['Call Number'] != original_row['Call Number']:
                    # Remove monkey emoji before saving to cache
                    current_cache[manual_key] = row['Call Number'].replace(SUGGESTION_FLAG, '')
                    updated_count += 1
            save_cache(current_cache)
            st.success(f"Updated {updated_count} manual classifications in cache!")
            st.rerun()

        # PDF Generation Section
        st.subheader("Generate PDF Labels")
        if st.button("Generate PDF"):
            pdf_output = generate_pdf_labels(edited_df)
            st.download_button(
                label="Download PDF Labels",
                data=pdf_output,
                file_name="book_labels.pdf",
                mime="application/pdf"
            )

    with st.expander("Debug Log"): # Add this block
        st.download_button(
            label="Download Full Debug Log",
            data=log_capture_string.getvalue(),
            file_name="debug_log.txt",
            mime="text/plain"
        )

if __name__ == "__main__":
    main(), cleaned):
        st_logger.debug(f"clean_call_number returning LC-like or numeric: {cleaned}")
        return cleaned

    # If none of the above conditions are met, it's an invalid format for a call number
    st_logger.debug(f"clean_call_number returning empty string for invalid format: {cleaned}")
    return ""


def get_book_metadata_initial_pass(title, author, cache, event):
    st_logger.debug(f"Entering get_book_metadata_initial_pass for: {title}")
    safe_title = re.sub(r'[^a-zA-Z0-9\s\.:]', '', title)
    safe_author = re.sub(r'[^a-zA-Z0-9\s,]', '', author)

    metadata = {'classification': '', 'series_name': '', 'volume_number': '', 'publication_year': '', 'genres': [], 'google_genres': [], 'error': None}

    google_meta = get_book_metadata_google_books(title, author, cache)
    metadata.update(google_meta)

    if not metadata.get('google_genres'):
        st_logger.debug(f"No genres in Google Books for {title}. Querying LOC.")
        loc_cache_key = f"loc_{safe_title}|{safe_author}".lower()
        if loc_cache_key in cache:
            st_logger.debug(f"LOC cache hit for '{title}' by '{author}'.")
            cached_loc_meta = cache[loc_cache_key]
            metadata.update(cached_loc_meta)
        else:
            base_url = "http://lx2.loc.gov:210/LCDB"
            query = f'bath.title="{safe_title}" and bath.author="{safe_author}"'
            params = {"version": "1.1", "operation": "searchRetrieve", "query": query, "maximumRecords": "1", "recordSchema": "marcxml"}
            st_logger.debug(f"LOC query for '{title}' by '{author}': {base_url}?{requests.compat.urlencode(params)})")
            
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
                        pub_year_node = root.find('.//marc:datafield[@tag="264"]/marc:subfield[@code="c"]', ns_marc) or root.find('.//marc:datafield[@tag="260"]/marc:subfield[@code="c"]', ns_marc)
                        if pub_year_node is not None and pub_year_node.text:
                            years = re.findall(r'(1[7-9]\d{2}|20\d{2})', pub_year_node.text)
                            if years: metadata['publication_year'] = str(min([int(y) for y in years]))
                        genre_nodes = root.findall('.//marc:datafield[@tag="655"]/marc:subfield[@code="a"]', ns_marc)
                        if genre_nodes:
                            metadata['genres'] = [g.text.strip().rstrip('.') for g in genre_nodes]
                        
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

    event.set()
    return metadata

def generate_pdf_labels(df):
    buffer = StringIO()
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
    return buffer.getvalue()

def extract_year(date_string):
    """Extracts the first 4-digit number from a string, assuming it's a year."""
    if isinstance(date_string, str):
        match = re.search(r'\\b(1[7-9]\d{2}|20\d{2})\\b', date_string)
        if match:
            return match.group(1)
    return ""

def main():
    st_logger.debug("Streamlit app main function started.")
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
        unclassified_books_for_vertex_ai = [] # To collect books for batch Vertex AI processing

        # First pass: Process with Google Books and LOC APIs
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(get_book_metadata_initial_pass, row.get('Title', '').strip(), row.get("Author's Name", '').strip(), loc_cache, threading.Event()): i for i, row in df.iterrows()}
            
            for i, future in enumerate(as_completed(futures)):
                row_index = futures[future]
                lc_meta = future.result()
                row = df.iloc[row_index]
                title = row.get('Title', '').strip()
                author = row.get("Author's Name", '').strip()
                
                # Original Atriuum data
                original_holding_barcode = row.get('Holdings Barcode', '').strip()
                raw_original_call_number = row.get('Call Number', '').strip() # Get raw original
                cleaned_original_call_number = clean_call_number(raw_original_call_number, [], [], title="", is_original_data=True) # Clean original
                original_series_name = row.get('Series Title', '').strip()
                original_series_number = row.get('Series Number', '').strip()
                original_copyright_year = extract_year(row.get('Copyright', '').strip())
                original_publication_date_year = extract_year(row.get('Publication Date', '').strip())

                # Prioritize original copyright/publication year
                final_original_year = ""
                if original_copyright_year: final_original_year = original_copyright_year
                elif original_publication_date_year: final_original_year = original_publication_date_year

                # Mashed-up data from initial pass
                api_call_number = lc_meta.get('classification', '')
                cleaned_call_number = clean_call_number(api_call_number, lc_meta.get('genres', []), lc_meta.get('google_genres', []), title=title)
                mashed_series_name = lc_meta.get('series_name', '').strip()
                mashed_volume_number = lc_meta.get('volume_number', '').strip()
                mashed_publication_year = lc_meta.get('publication_year', '').strip()

                # Merge logic for initial pass
                # Use cleaned_original_call_number if valid, else fallback to mashed-up
                current_call_number = cleaned_original_call_number if cleaned_original_call_number else (SUGGESTION_FLAG + cleaned_call_number if cleaned_call_number else "")
                current_series_name = original_series_name if original_series_name else (SUGGESTION_FLAG + mashed_series_name if mashed_series_name else '')
                current_series_number = original_series_number if original_series_number else (SUGGESTION_FLAG + mashed_volume_number if mashed_volume_number else '')
                current_publication_year = final_original_year if final_original_year else (SUGGESTION_FLAG + mashed_publication_year if mashed_publication_year else '')

                # Collect for Vertex AI batch processing if still unclassified
                if not current_call_number or current_call_number == "UNKNOWN":
                    unclassified_books_for_vertex_ai.append({
                        'title': title,
                        'author': author,
                        'row_index': row_index, # Keep track of original row index
                        'lc_meta': lc_meta # Keep original metadata for later merging
                    })
                
                results.append({
                    'Title': title,
                    'Author': author,
                    'Holdings Barcode': original_holding_barcode,
                    'Call Number': current_call_number,
                    'Series Info': current_series_name, # Will be populated after all processing
                    'Series Number': current_series_number,
                    'Copyright Year': current_publication_year
                })
                progress_bar.progress((i + 1) / len(df))

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
                    
                    for book_data in processed_batch:
                        title = book_data['title']
                        author = book_data['author']
                        row_index = book_data['row_index']
                        lc_meta = book_data['lc_meta']
                        
                        key = f"{title}|{author}".lower()
                        vertex_ai_classification = batch_classifications.get(key, "Unknown")

                        # Update the classification in lc_meta for this book
                        if vertex_ai_classification and vertex_ai_classification != "Unknown":
                            lc_meta['classification'] = vertex_ai_classification
                            # Ensure google_genres is a list before appending
                            if 'google_genres' not in lc_meta or not isinstance(lc_meta['google_genres'], list):
                                lc_meta['google_genres'] = []
                            lc_meta['google_genres'].append(vertex_ai_classification) # Add to google_genres for consistency
                        
                        # Re-clean call number with new Vertex AI classification
                        final_call_number_after_vertex_ai = clean_call_number(lc_meta.get('classification', ''), lc_meta.get('genres', []), lc_meta.get('google_genres', []), title=title)
                        
                        # Update the results list with the new classification
                        results[row_index]['Call Number'] = (SUGGESTION_FLAG + final_call_number_after_vertex_ai if final_call_number_after_vertex_ai else "")

        # Final pass to populate Series Info and ensure all fields are non-blank
        for i, row_data in enumerate(results):
            # Re-combine series name and volume number for display after all processing
            final_series_name = row_data['Series Info'] # This was populated with mashed_series_name
            final_series_number = row_data['Series Number']

            series_info = ""
            if final_series_name and final_series_number:
                series_info = f"{final_series_name}, Vol. {final_series_number}"
            elif final_series_name:
                series_info = final_series_name
            elif final_series_number:
                series_info = f"Vol. {final_series_number}"
            
            results[i]['Series Info'] = series_info

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

                # Update cache only if the cleaned call number has changed
                if row['Call Number'] != original_row['Call Number']:
                    # Remove monkey emoji before saving to cache
                    current_cache[manual_key] = row['Call Number'].replace(SUGGESTION_FLAG, '')
                    updated_count += 1
            save_cache(current_cache)
            st.success(f"Updated {updated_count} manual classifications in cache!")
            st.rerun()

        # PDF Generation Section
        st.subheader("Generate PDF Labels")
        if st.button("Generate PDF"):
            pdf_output = generate_pdf_labels(edited_df)
            st.download_button(
                label="Download PDF Labels",
                data=pdf_output,
                file_name="book_labels.pdf",
                mime="application/pdf"
            )

    with st.expander("Debug Log"): # Add this block
        st.download_button(
            label="Download Full Debug Log",
            data=log_capture_string.getvalue(),
            file_name="debug_log.txt",
            mime="text/plain"
        )

if __name__ == "__main__":
    main()