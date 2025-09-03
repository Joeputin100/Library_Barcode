import requests
import re
import time
import json
import os
from lxml import etree
import vertexai
from vertexai.generative_models import GenerativeModel
from caching import save_cache
from data_transformers import extract_year
import google.auth
from collections import deque
from datetime import datetime, timedelta

# Global rate limiting state for LOC API
loc_rate_limit_state = {
    "request_times": deque(),
    "last_request_time": 0,
    "max_requests_per_hour": 125,    # LOC limit: 125 requests per rolling hour
    "min_request_interval": 0.5,     # LOC limit: 1 request per 500ms (0.5 seconds)
    "current_rate_limit_remaining": None,
    "current_rate_limit_reset": None,
}

def check_loc_rate_limit():
    """Check if we can make a request to LOC API based on rate limits"""
    current_time = time.time()
    
    # Remove requests older than 1 hour
    one_hour_ago = current_time - 3600
    while (loc_rate_limit_state["request_times"] and 
           loc_rate_limit_state["request_times"][0] < one_hour_ago):
        loc_rate_limit_state["request_times"].popleft()
    
    # Check hourly limit
    if len(loc_rate_limit_state["request_times"]) >= loc_rate_limit_state["max_requests_per_hour"]:
        oldest_request = loc_rate_limit_state["request_times"][0]
        wait_time = (oldest_request + 3600) - current_time
        return False, max(wait_time, 0)
    
    # Check minimum interval
    last_request = loc_rate_limit_state["last_request_time"]
    if current_time - last_request < loc_rate_limit_state["min_request_interval"]:
        wait_time = loc_rate_limit_state["min_request_interval"] - (current_time - last_request)
        return False, wait_time
    
    return True, 0

def record_loc_request():
    """Record a successful LOC API request for rate limiting"""
    current_time = time.time()
    loc_rate_limit_state["request_times"].append(current_time)
    loc_rate_limit_state["last_request_time"] = current_time

def update_loc_rate_limit_headers(response):
    """Update rate limiting state from LOC API response headers"""
    if response and hasattr(response, 'headers'):
        remaining = response.headers.get('X-RateLimit-Remaining')
        reset = response.headers.get('X-RateLimit-Reset')
        
        if remaining is not None:
            try:
                loc_rate_limit_state["current_rate_limit_remaining"] = int(remaining)
            except (ValueError, TypeError):
                pass
        
        if reset is not None:
            try:
                # X-RateLimit-Reset is typically a Unix timestamp
                loc_rate_limit_state["current_rate_limit_reset"] = int(reset)
            except (ValueError, TypeError):
                pass

def should_switch_to_alternative_api():
    """Check if we should switch to alternative APIs during LOC rate limiting"""
    current_time = time.time()
    
    # If we have header information, use it
    if (loc_rate_limit_state["current_rate_limit_remaining"] is not None and
        loc_rate_limit_state["current_rate_limit_remaining"] <= 0):
        
        reset_time = loc_rate_limit_state["current_rate_limit_reset"]
        if reset_time is not None:
            wait_time = reset_time - current_time
            if wait_time > 60:  # If wait time > 1 minute, switch to alternatives
                return True, wait_time
    
    # Fallback: check our own rate limiting
    can_request, wait_time = check_loc_rate_limit()
    if not can_request and wait_time > 60:  # Wait time > 1 minute
        return True, wait_time
    
    return False, 0

def parse_loc_rate_limit_message(error_message):
    """Parse LOC API rate limiting messages and extract wait times"""
    if not error_message:
        return None, None
    
    message_text = error_message.text.lower()
    
    # Common LOC rate limiting patterns
    if "rate limit" in message_text or "too many requests" in message_text:
        # Look for time-based patterns
        if "hour" in message_text:
            return "hourly", 3600  # Wait 1 hour
        elif "minute" in message_text:
            # Extract minutes from message
            minute_match = re.search(r'(\d+)\s*minute', message_text)
            if minute_match:
                minutes = int(minute_match.group(1))
                return "minute_based", minutes * 60
            return "minute_based", 300  # Default 5 minutes
        elif "second" in message_text:
            # Extract seconds from message
            second_match = re.search(r'(\d+)\s*second', message_text)
            if second_match:
                seconds = int(second_match.group(1))
                return "second_based", seconds
            return "second_based", 30  # Default 30 seconds
    
    # Check for specific LOC error codes
    if "diagnostic" in message_text:
        # Look for diagnostic codes that indicate rate limiting
        diag_match = re.search(r'diagnostic\s*(\d+)', message_text)
        if diag_match:
            diag_code = int(diag_match.group(1))
            # Common rate limiting diagnostic codes
            if diag_code in [10, 11, 12]:  # Common SRU rate limiting codes
                return "sru_rate_limit", 3600  # Wait 1 hour
    
    return None, None

def get_book_metadata_google_books(title, author, isbn, cache):
    safe_title = re.sub(r"[^a-zA-Z0-9\s\.:]", "", title)
    safe_author = re.sub(r"[^a-zA-Z0-9\s, ]", "", author)
    cache_key = f"google_{safe_title}|{safe_author}|{isbn}".lower()
    if cache_key in cache:
        return cache[cache_key], True, True

    metadata = {
        "google_genres": [],
        "classification": "",
        "series_name": "",
        "volume_number": "",
        "publication_year": "",
        "error": None,
    }
    try:
        if isbn:
            query = f"isbn:{isbn}"
        else:
            query = f'intitle:"{safe_title}"+inauthor:"{safe_author}"'
        api_key = os.environ.get("GOOGLE_API_KEY", "")
        url = f"https://www.googleapis.com/books/v1/volumes?q={query}&maxResults=1&key={api_key}"
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        data = response.json()
        print(f"Google Books API response: {data}")

        if "items" in data and data["items"]:
            item = data["items"][0]
            volume_info = item.get("volumeInfo", {})

            if "title" in volume_info:
                metadata["title"] = volume_info["title"]
            if "authors" in volume_info:
                metadata["author"] = ", ".join(volume_info["authors"])

            if "categories" in volume_info:
                metadata["google_genres"].extend(volume_info["categories"])

            if "description" in volume_info:
                description = volume_info["description"]
                match = re.search(
                    r"Subject: (.*?)(?:\n|$)", description, re.IGNORECASE
                )
                if match:
                    subjects = [s.strip() for s in match.group(1).split(",")]
                    metadata["google_genres"].extend(subjects)

            if "publishedDate" in volume_info:
                metadata["publication_year"] = extract_year(
                    volume_info["publishedDate"]
                )

            if "seriesInfo" in volume_info:
                series_info = volume_info["seriesInfo"]
                if "bookDisplayNumber" in series_info:
                    metadata["volume_number"] = series_info[
                        "bookDisplayNumber"
                    ]
                if "series" in series_info and series_info["series"]:
                    if "title" in series_info["series"][0]:
                        metadata["series_name"] = series_info["series"][0][
                            "title"
                        ]

        cache[cache_key] = metadata
        save_cache(cache)
        success = metadata["error"] is None
        return metadata, False, success

    except requests.exceptions.RequestException as e:
        if isinstance(
            e,
            (
                requests.exceptions.Timeout,
                requests.exceptions.ConnectionError,
            ),
        ):
            metadata["error"] = (
                f"Temporary Google Books API request failed: {e}"
            )
        else:
            metadata["error"] = (
                f"Permanent Google Books API request failed: {e}"
            )
            cache[cache_key] = metadata
            save_cache(cache)
    except Exception as e:
        print(f"Google Books API exception: {e}")
        metadata["error"] = (
            f"An unexpected error occurred with Google Books API: {e}"
        )
    success = metadata["error"] is None
    return metadata, False, success

def get_book_metadata_open_library(title, author, isbn, cache):
    """Gets book metadata from the Open Library API."""
    safe_title = re.sub(r"[^a-zA-Z0-9\s\.:]", "", title)
    safe_author = re.sub(r"[^a-zA-Z0-9\s, ]", "", author)
    cache_key = f"openlibrary_{safe_title}|{safe_author}|{isbn}".lower()
    if cache_key in cache:
        return cache[cache_key], True, True

    metadata = {
        "classification": "",
        "series_name": "",
        "volume_number": "",
        "publication_year": "",
        "genres": [],
        "error": None,
    }

    try:
        if isbn:
            url = f"https://openlibrary.org/isbn/{isbn}.json"
            response = requests.get(url, timeout=15)
            response.raise_for_status()
            data = response.json()
        else:
            query = f'{safe_title} {safe_author}'.strip()
            url = f"http://openlibrary.org/search.json?q={query}"
            response = requests.get(url, timeout=15)
            response.raise_for_status()
            search_data = response.json()
            if "docs" in search_data and search_data["docs"]:
                # Assume the first result is the most relevant
                data = search_data["docs"][0]
            else:
                data = {}

        if "publish_date" in data:
            metadata["publication_year"] = extract_year(data["publish_date"])

        if "subjects" in data:
            metadata["genres"].extend(data["subjects"])

        if "series" in data:
            metadata["series_name"] = data["series"][0]

        cache[cache_key] = metadata
        save_cache(cache)
        success = metadata["error"] is None
        return metadata, False, success

    except requests.exceptions.RequestException as e:
        if isinstance(
            e,
            (
                requests.exceptions.Timeout,
                requests.exceptions.ConnectionError,
            ),
        ):
            metadata["error"] = (
                f"Temporary Open Library API request failed: {e}"
            )
        else:
            metadata["error"] = (
                f"Permanent Open Library API request failed: {e}"
            )
            cache[cache_key] = metadata
            save_cache(cache)
    except Exception as e:
        metadata["error"] = (
            f"An unexpected error occurred with Open Library API: {e}"
        )
    success = metadata["error"] is None
    return metadata, False, success

def get_vertex_ai_classification_batch(batch_books, cache):
    retry_delays = [10, 20, 30]

    try:
        credentials, project_id = google.auth.default()
        vertexai.init(project=project_id, credentials=credentials, location="us-central1")
        model = GenerativeModel("gemini-2.5-flash")

        batch_prompts = []
        for book in batch_books:
            batch_prompts.append(
                f"Title: {book['title']}, Author: {book['author']}"
            )

        full_prompt = (
            "Perform DEEP RESEARCH analysis for each book. Provide:"
            "1. Primary classification (genre or Dewey Decimal)"
            "2. Quality score (1-10 based on information completeness and accuracy)"
            "3. Confidence level (high/medium/low)"
            "4. Alternative classifications (if any)"
            "\nBooks:\n" + "\n".join(batch_prompts) +
            "\nProvide the output as a JSON array of objects with these fields: "
            "classification, quality_score, confidence_level, alternative_classifications"
        )

        cache_key = f"vertex_{full_prompt}".lower()
        if cache_key in cache:
            return cache[cache_key], True

        for i in range(len(retry_delays) + 1):
            try:
                response = model.generate_content(full_prompt)
                print(f"Vertex AI response object: {response}")
                response_text = response.text.strip()
                if response_text.startswith(
                    "```json"
                ) and response_text.endswith("```"):
                    response_text = response_text[7:-3].strip()
                
                print(f"Vertex AI response: {response_text}")
                classifications = json.loads(response_text)
                cache[cache_key] = classifications
                save_cache(cache)
                return classifications, False
            except Exception as e:
                if i < len(retry_delays):
                    time.sleep(retry_delays[i])
                else:
                    return [], False
    except Exception as e:
        return [], False

def get_book_metadata_initial_pass(
    title, author, isbn, lccn, cache, is_blank=False, is_problematic=False
):
    safe_title = re.sub(r"[^a-zA-Z0-9\s\.:]", "", title)
    safe_author = re.sub(r"[^a-zA-Z0-9\s, ]", "", author)

    metadata = {
        "classification": "",
        "series_name": "",
        "volume_number": "",
        "publication_year": "",
        "genres": [],
        "google_genres": [],
        "error": None,
    }

    print(f"Before google call: title='{title}', author='{author}'")
    google_meta, google_cached, google_success = get_book_metadata_google_books(
        title, author, isbn, cache
    )
    print(f"After google call: google_meta={google_meta}")
    metadata.update(google_meta)
    if not title and metadata.get("title"):
        title = metadata.get("title")
    if not author and metadata.get("author"):
        author = metadata.get("author")

    openlibrary_meta, openlibrary_cached, openlibrary_success = get_book_metadata_open_library(
        title, author, isbn, cache
    )
    metadata.update(openlibrary_meta)

    loc_cache_key = f"loc_{safe_title}|{safe_author}".lower()
    loc_cached = False
    loc_success = False
    if loc_cache_key in cache:
        cached_loc_meta = cache[loc_cache_key]
        metadata.update(cached_loc_meta)
        loc_cached = True
        loc_success = cached_loc_meta.get("error") is None
    else:
        # Check if we should switch to alternative APIs due to LOC rate limiting
        should_switch, wait_time = should_switch_to_alternative_api()
        if should_switch:
            print(f"LOC API rate limited for {wait_time:.1f} seconds, using alternative APIs")
            # Skip LOC API and rely on other sources
            metadata["loc_skipped_due_to_rate_limit"] = True
            metadata["loc_rate_limit_wait_time"] = wait_time
            loc_success = False
        else:
            base_url = "http://lx2.loc.gov:210/LCDB"
            if isbn:
                query = f'bath.isbn="{isbn}"'
            elif lccn:
                query = f'bath.lccn="{lccn}"'
            else:
                query = f'bath.title="{safe_title}" and bath.author="{safe_author}"'
            params = {
                "version": "1.1",
                "operation": "searchRetrieve",
                "query": query,
                "maximumRecords": "1",
                "recordSchema": "marcxml",
            }

            retry_delays = [5, 15, 30]
            for i in range(len(retry_delays) + 1):
                try:
                    # Check rate limiting before making request
                    can_request, wait_time = check_loc_rate_limit()
                    if not can_request:
                        print(f"LOC API rate limited: waiting {wait_time:.1f} seconds")
                        time.sleep(wait_time)
                        # Re-check after waiting
                        can_request, wait_time = check_loc_rate_limit()
                        if not can_request:
                            metadata["error"] = f"LOC API rate limited: please wait {wait_time:.1f} seconds"
                            cache[loc_cache_key] = metadata
                            save_cache(cache)
                            loc_success = False
                            break
                    
                    response = requests.get(base_url, params=params, timeout=20)
                    response.raise_for_status()
                    
                    # Update rate limiting state from response headers
                    update_loc_rate_limit_headers(response)
                    
                    root = etree.fromstring(response.content)
                    ns_diag = {"diag": "http://www.loc.gov/zing/srw/diagnostic/"}
                    error_message = root.find(".//diag:message", ns_diag)
                    if error_message is not None:
                        # Parse rate limiting messages from LOC response
                        limit_type, wait_time = parse_loc_rate_limit_message(error_message)
                        if limit_type:
                            print(f"LOC API rate limit detected ({limit_type}): waiting {wait_time} seconds")
                            metadata["error"] = f"LOC API {limit_type} rate limit: please wait {wait_time} seconds"
                            # For rate limits, we should wait and potentially retry
                            if i < len(retry_delays):
                                time.sleep(wait_time)
                                continue
                        else:
                            metadata["error"] = f"LOC API Error: {error_message.text}"
                        
                        if "intermittent" not in error_message.text.lower():
                            cache[loc_cache_key] = metadata
                            save_cache(cache)
                    else:
                        ns_marc = {"marc": "http://www.loc.gov/MARC21/slim"}
                        classification_node = root.find(
                            './/marc:datafield[@tag="082"]/marc:subfield[@code="a"]',
                            ns_marc,
                        )
                        if classification_node is not None:
                            metadata["classification"] = classification_node.text.strip()
                        series_node = root.find(
                            './/marc:datafield[@tag="490"]/marc:subfield[@code="a"]',
                            ns_marc,
                        )
                        if series_node is not None:
                            metadata["series_name"] = series_node.text.strip().rstrip(" ;")
                        volume_node = root.find(
                            './/marc:datafield[@tag="490"]/marc:subfield[@code="v"]',
                            ns_marc,
                        )
                        if volume_node is not None:
                            metadata["volume_number"] = volume_node.text.strip()
                        pub_year_node = root.find(
                            './/marc:datafield[@tag="264"]/marc:subfield[@code="c"]',
                            ns_marc,
                        )
                        if pub_year_node is None:
                            pub_year_node = root.find(
                                './/marc:datafield[@tag="260"]/marc:subfield[@code="c"]',
                                ns_marc,
                            )
                        if pub_year_node is not None and pub_year_node.text:
                            years = re.findall(
                                r"(1[7-9]\d{2}|20\d{2})", pub_year_node.text
                            )
                            if years:
                                metadata["publication_year"] = str(
                                    min([int(y) for y in years])
                                )
                        genre_nodes = root.findall(
                            './/marc:datafield[@tag="655"]/marc:subfield[@code="a"]',
                            ns_marc,
                        )
                        if genre_nodes:
                            metadata["genres"] = [
                                g.text.strip().rstrip(".") for g in genre_nodes
                            ]

                        if not metadata["error"]:
                            cache[loc_cache_key] = metadata
                            save_cache(cache)
                            record_loc_request()  # Record successful request for rate limiting
                    loc_success = metadata["error"] is None
                    break
                except requests.exceptions.RequestException as e:
                    if i < len(retry_delays):
                        time.sleep(retry_delays[i])
                        continue
                    metadata["error"] = f"LOC API request failed after retries: {e}"
                    loc_success = False
                except Exception as e:
                    metadata["error"] = f"An unexpected error occurred with LOC API: {e}"
                    loc_success = False
                    break

    # Add Vertex AI Deep Research integration
    vertex_ai_meta = {}
    vertex_ai_cached = False
    vertex_ai_success = False
    
    # Call Vertex AI for ALL records for cross-checking and better enrichment
    if True:  # Always call Vertex AI for comprehensive enrichment
        try:
            # Prepare book data for Vertex AI batch processing
            book_data = [{
                "title": title,
                "author": author,
                "isbn": isbn,
                "existing_metadata": metadata
            }]
            
            # Call Vertex AI for deep research
            vertex_results, vertex_ai_cached = get_vertex_ai_classification_batch(book_data, cache)
            
            if vertex_results and not vertex_ai_cached:
                vertex_result = vertex_results[0]  # Get first result from batch
                vertex_ai_meta.update({
                    "vertex_ai_classification": vertex_result.get("classification", ""),
                    "vertex_ai_quality_score": vertex_result.get("quality_score", 0),
                    "vertex_ai_confidence": vertex_result.get("confidence_level", "low"),
                    "vertex_ai_alternative_classifications": vertex_result.get("alternative_classifications", [])
                })
                vertex_ai_success = True
                
                # Update main metadata with Vertex AI results
                metadata.update(vertex_ai_meta)
                
        except Exception as e:
            print(f"Vertex AI Deep Research failed: {e}")
            vertex_ai_meta["vertex_ai_error"] = str(e)
    
    return metadata, google_cached, loc_cached, openlibrary_success, loc_success, vertex_ai_success