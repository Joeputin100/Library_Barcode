import requests
import re
import time
import json
import os
from lxml import etree
import vertexai
from vertexai.generative_models import GenerativeModel
from caching import save_cache
from data_cleaning import extract_year


def get_book_metadata_google_books(title, author, cache):
    safe_title = re.sub(r'[^a-zA-Z0-9\s\.:]', '', title)
    safe_author = re.sub(r'[^a-zA-Z0-9\s,]', '', author)
    cache_key = f"google_{safe_title}|{safe_author}".lower()
    if cache_key in cache:
        return cache[cache_key], True

    metadata = {
        'google_genres': [],
        'classification': '',
        'series_name': '',
        'volume_number': '',
        'publication_year': '',
        'error': None
    }
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
        save_cache(cache)
        return metadata, False

    except requests.exceptions.RequestException as e:
        if isinstance(e, (requests.exceptions.Timeout, requests.exceptions.ConnectionError)):
            metadata['error'] = f"Temporary Google Books API request failed: {e}"
        else:
            metadata['error'] = f"Permanent Google Books API request failed: {e}"
            cache[cache_key] = metadata
            save_cache(cache)
    except Exception as e:
        metadata['error'] = f"An unexpected error occurred with Google Books API: {e}"
    return metadata, False


def get_vertex_ai_classification_batch(batch_books, vertex_ai_credentials, cache):
    temp_creds_path = "temp_creds.json"
    retry_delays = [10, 20, 30]

    try:
        credentials_dict = dict(vertex_ai_credentials)
        credentials_json = json.dumps(credentials_dict)

        with open(temp_creds_path, "w") as f:
            f.write(credentials_json)

        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = temp_creds_path

        vertexai.init(project=credentials_dict["project_id"], location="us-central1")
        model = GenerativeModel("gemini-2.5-flash")

        batch_prompts = []
        for book in batch_books:
            batch_prompts.append(f"Title: {book['title']}, Author: {book['author']}")

        full_prompt = (
            "For each book in the following list, provide its primary genre or Dewey Decimal classification, "
            "series title, volume number, and copyright year. If it's fiction, classify as 'FIC'. If non-fiction, "
            "provide a general Dewey Decimal category like '300' for Social Sciences, '500' for Science, etc. "
            "Provide the output as a JSON array of objects, where each object has 'title', 'author', 'classification', "
            "'series_title', 'volume_number', and 'copyright_year' fields. "
            "If you cannot determine a value for a field, use an empty string ''.\n\n"
            "Books:\n" + "\n".join(batch_prompts)
        )

        cache_key = f"vertex_{full_prompt}".lower()
        if cache_key in cache:
            return cache[cache_key], True

        for i in range(len(retry_delays) + 1):
            try:
                response = model.generate_content(full_prompt)
                response_text = response.text.strip()
                if response_text.startswith("```json") and response_text.endswith("```"):
                    response_text = response_text[7:-3].strip()

                classifications = json.loads(response_text)
                cache[cache_key] = classifications
                save_cache(cache)
                return classifications, False
            except Exception:
                if i < len(retry_delays):
                    time.sleep(retry_delays[i])
                else:
                    return [], False
    finally:
        if os.path.exists(temp_creds_path):
            os.remove(temp_creds_path)


def get_book_metadata_initial_pass(title, author, cache, is_blank=False, is_problematic=False):
    safe_title = re.sub(r'[^a-zA-Z0-9\s\.:]', '', title)
    safe_author = re.sub(r'[^a-zA-Z0-9\s,]', '', author)

    metadata = {
        'classification': '',
        'series_name': '',
        'volume_number': '',
        'publication_year': '',
        'genres': [],
        'google_genres': [],
        'error': None
    }

    google_meta, google_cached = get_book_metadata_google_books(title, author, cache)
    metadata.update(google_meta)

    loc_cache_key = f"loc_{safe_title}|{safe_author}".lower()
    loc_cached = False
    if loc_cache_key in cache:
        cached_loc_meta = cache[loc_cache_key]
        metadata.update(cached_loc_meta)
        loc_cached = True
    else:
        base_url = "http://lx2.loc.gov:210/LCDB"
        query = f'bath.title="{safe_title}" and bath.author="{safe_author}"'
        params = {
            "version": "1.1",
            "operation": "searchRetrieve",
            "query": query,
            "maximumRecords": "1",
            "recordSchema": "marcxml"
        }

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
                    if "intermittent" not in error_message.text.lower():
                        cache[loc_cache_key] = metadata
                        save_cache(cache)
                else:
                    ns_marc = {'marc': 'http://www.loc.gov/MARC21/slim'}
                    classification_node = root.find(
                        './/marc:datafield[@tag="082"]/marc:subfield[@code="a"]', ns_marc)
                    if classification_node is not None:
                        metadata['classification'] = classification_node.text.strip()
                    series_node = root.find('.//marc:datafield[@tag="490"]/marc:subfield[@code="a"]', ns_marc)
                    if series_node is not None:
                        metadata['series_name'] = series_node.text.strip().rstrip(' ;')
                    volume_node = root.find('.//marc:datafield[@tag="490"]/marc:subfield[@code="v"]', ns_marc)
                    if volume_node is not None:
                        metadata['volume_number'] = volume_node.text.strip()
                    pub_year_node = root.find('.//marc:datafield[@tag="264"]/marc:subfield[@code="c"]', ns_marc)
                    if pub_year_node is None:
                        pub_year_node = root.find('.//marc:datafield[@tag="260"]/marc:subfield[@code="c"]', ns_marc)
                    if pub_year_node is not None and pub_year_node.text:
                        years = re.findall(r'(1[7-9]\d{2}|20\d{2})', pub_year_node.text)
                        if years:
                            metadata['publication_year'] = str(min([int(y) for y in years]))
                    genre_nodes = root.findall('.//marc:datafield[@tag="655"]/marc:subfield[@code="a"]', ns_marc)
                    if genre_nodes:
                        metadata['genres'] = [g.text.strip().rstrip('.') for g in genre_nodes]

                    if not metadata['error']:
                        cache[loc_cache_key] = metadata
                        save_cache(cache)
                break
            except requests.exceptions.RequestException as e:
                if i < len(retry_delays):
                    time.sleep(retry_delays[i])
                    continue
                metadata['error'] = f"LOC API request failed after retries: {e}"
            except Exception as e:
                metadata['error'] = f"An unexpected error occurred with LOC API: {e}"
                break

    return metadata, google_cached, loc_cached
