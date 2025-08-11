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
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from reportlab.lib.units import inch

# --- Page Title ---
st.title("Atriuum Label Generator")

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
    # Create a temporary file to store the credentials
    temp_creds_path = "temp_creds.json"
    retry_delays = [5, 5, 5] # 5-second delay for 3 retries
    try:
        # Convert AttrDict to a standard dictionary
        credentials_dict = dict(vertex_ai_credentials)
        credentials_json = json.dumps(credentials_dict)
        
        with open(temp_creds_path, "w") as f:
            f.write(credentials_json)

        # Set the environment variable for authentication
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = temp_creds_path
        
        vertexai.init(project=credentials_dict["project_id"], location="us-central1")
        model = GenerativeModel("gemini-2.5-flash") # Changed model to gemini-2.5-flash
        
        prompt = (
            f"What is the primary genre of the book titled '{title}' by '{author}'? "
            "If it's fiction, classify as 'FIC'. If non-fiction, provide a general Dewey Decimal category like '300' for Social Sciences, '500' for Science, etc. "
            "Please provide only the classification, without any additional text or explanation. "
            "For example: 'Science Fiction' or 'Historical Fiction'. If you cannot determine, say 'Unknown'."
        )
        
        for i in range(len(retry_delays) + 1):
            try:
                response = model.generate_content(prompt)
                return response.text.strip()
            except Exception as e:
                if i < len(retry_delays):
                    print(f"Vertex AI call failed for {title}: {e}. Retrying in {retry_delays[i]} seconds...")
                    time.sleep(retry_delays[i])
                else:
                    print(f"Vertex AI call failed after multiple retries for {title}: {e}")
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

    metadata = {'classification': '', 'series_name': '', 'volume_number': '', 'publication_year': '', 'genres': [], 'google_genres': [], 'error': None} # Added series_name, volume_number, publication_year

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
        holding_number = str(row['Holding Number']).replace(SUGGESTION_FLAG, '')
        call_number = str(row['Call Number']).replace(SUGGESTION_FLAG, '')
        title = str(row['Title'])
        author = str(row['Author'])
        series_info = str(row['Series Info']).replace(SUGGESTION_FLAG, '')
        copyright_year = str(row['Copyright Year']).replace(SUGGESTION_FLAG, '')

        # Set font and size
        c.setFont('Helvetica', 10)

        # Draw Holding Number
        c.drawString(x + 0.1 * inch, y + label_height - 0.2 * inch, holding_number)

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
                
                # Original Atriuum data
                original_holding_number = row.get('Holding Number', '').strip()
                original_call_number = row.get('Call Number', '').strip()
                original_series_name = row.get('Series Title', '').strip()
                original_volume_number = row.get('Series Number', '').strip()
                original_publication_year = row.get('Copyright Year', '').strip()

                # Mashed-up data
                api_call_number = lc_meta.get('classification', '')
                cleaned_call_number = clean_call_number(api_call_number, lc_meta.get('genres', []), lc_meta.get('google_genres', []), title=title)
                mashed_series_name = lc_meta.get('series_name', '').strip()
                mashed_volume_number = lc_meta.get('volume_number', '').strip()
                mashed_publication_year = lc_meta.get('publication_year', '').strip()

                # Merge logic
                final_holding_number = original_holding_number # Holding number is always from original
                final_call_number = original_call_number if original_call_number else (SUGGESTION_FLAG + cleaned_call_number if cleaned_call_number else '')
                final_series_name = original_series_name if original_series_name else (SUGGESTION_FLAG + mashed_series_name if mashed_series_name else '')
                final_volume_number = original_volume_number if original_volume_number else (SUGGESTION_FLAG + mashed_volume_number if mashed_volume_number else '')
                final_publication_year = original_publication_year if original_publication_year else (SUGGESTION_FLAG + mashed_publication_year if mashed_publication_year else '')

                # Combine series name and volume number for display
                series_info = ""
                if final_series_name and final_volume_number:
                    series_info = f"{final_series_name}, Vol. {final_volume_number}"
                elif final_series_name:
                    series_info = final_series_name
                elif final_volume_number:
                    series_info = f"Vol. {final_volume_number}"

                results.append({
                    'Title': title,
                    'Author': author,
                    'Holding Number': final_holding_number,
                    'Call Number': final_call_number,
                    'Series Info': series_info,
                    'Copyright Year': final_publication_year
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

if __name__ == "__main__":
    main()
