import json
import requests
import time
import os
import subprocess
from api_calls import get_book_metadata_google_books, get_vertex_ai_classification_batch
from data_transformers import clean_call_number


def update_status(task_id, status):
    """Helper function to update task status in project_plan.json."""
    subprocess.run(["python", "update_task_status.py", task_id, status])


def get_loc_data(lccn):
    """
    Fetches data from the Library of Congress API for a given LCCN.
    Returns the call number if found, otherwise None.
    """
    url = f"https://www.loc.gov/item/{lccn}/?fo=json"
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for bad status codes
        data = response.json()
        # The call number is usually in item -> call_number
        if "item" in data and "call_number" in data["item"]:
            return data["item"]["call_number"][0]
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for LCCN {lccn}: {e}")
    except json.JSONDecodeError:
        print(f"Error parsing JSON for LCCN {lccn}")
    return None


def enrich_data_with_loc():
    """
    Enriches the extracted data with information from the Library of Congress API,
    Google Books, and Vertex AI.
    """
    # update_status("2.2.3", "PROCESSING")  # Set status to PROCESSING

    with open("extracted_data.json", "r") as f:
        extracted_data = json.load(f)

    barcodes_to_process = []
    if os.path.exists("enrichment_queue.txt"):
        with open("enrichment_queue.txt", "r") as f:
            barcodes_to_process = [line.strip() for line in f]
    else:
        barcodes_to_process = list(
            extracted_data.keys()
        )  # Convert to list for iteration

    # Placeholder for Vertex AI credentials
    # In a real scenario, these would be loaded securely (e.g., from environment variables or a config file)
    vertex_ai_credentials = {
        "project_id": "your-gcp-project-id",
        "private_key_id": "your-private-key-id",
        "private_key": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n",
        "client_email": "your-service-account-email",
        "client_id": "your-client-id",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": "your-client-x509-cert-url",
    }

    # Cache for Google Books and LOC API calls
    loc_cache = (
        {}
    )  # This should ideally be loaded from a file and saved after processing

    # Collect books for batch Vertex AI processing
    unclassified_books_for_vertex_ai = []

    for barcode in barcodes_to_process:
        data = extracted_data.get(barcode)
        if not data:
            continue

        # Try to enrich with LOC API first if LCCN is available and call_number is missing
        if data.get("lccn") and not data.get("call_number"):
            call_number = get_loc_data(data["lccn"])
            if call_number:
                data["call_number"] = call_number
                print(f"  -> Found call number from LOC: {call_number}")
            else:
                print(
                    f"  -> Could not find call number from LOC for LCCN {data['lccn']}"
                )
            time.sleep(1)  # Rate limit for LOC API

        # Google Books enrichment (always try if title/author available)
        google_meta = get_book_metadata_google_books(
            data.get("title", ""), data.get("author", ""), loc_cache
        )
        if google_meta.get("google_genres"):
            if "google_genres" not in data or not isinstance(
                data["google_genres"], list
            ):
                data["google_genres"] = []
            data["google_genres"].extend(google_meta["google_genres"])
        if google_meta.get("series_name") and not data.get("series_name"):
            data["series_name"] = google_meta["series_name"]
        if google_meta.get("volume_number") and not data.get("volume_number"):
            data["volume_number"] = google_meta["volume_number"]
        if google_meta.get("publication_year") and not data.get(
            "publication_year"
        ):
            data["publication_year"] = google_meta["publication_year"]

        # Decide if Vertex AI is needed (if call_number is still missing after LOC and Google Books)
        current_call_number = data.get("call_number")

        if not current_call_number or current_call_number == "UNKNOWN":
            unclassified_books_for_vertex_ai.append(
                {
                    "title": data.get("title", ""),
                    "author": data.get("author", ""),
                    "barcode": barcode,  # Keep track of original barcode
                    "lc_meta": data,  # Pass the current data for merging
                }
            )

        # Update extracted_data with potentially new info
        extracted_data[barcode] = data
        time.sleep(0.1)  # Small delay to avoid hammering APIs

    print(
        f"Number of unclassified books for Vertex AI: {len(unclassified_books_for_vertex_ai)}"
    )
    # Second pass: Batch process unclassified books with Vertex AI
    if unclassified_books_for_vertex_ai:
        print(
            f"Unclassified books for Vertex AI: {len(unclassified_books_for_vertex_ai)} books"
        )
        BATCH_SIZE = 5
        batches = [
            unclassified_books_for_vertex_ai[j: j + BATCH_SIZE]
            for j in range(
                0, len(unclassified_books_for_vertex_ai), BATCH_SIZE
            )
        ]

        for batch in batches:
            print(f"  Processing batch: {batch}")
            batch_classifications = get_vertex_ai_classification_batch(
                batch, vertex_ai_credentials
            )
            print(f"  Received batch classifications: {batch_classifications}")

            if not isinstance(batch_classifications, list):
                print(
                    f"Vertex AI returned non-list object: {batch_classifications}"
                )
                continue

            for book_data, vertex_ai_results in zip(
                batch, batch_classifications
            ):
                print(
                    f"    Vertex AI results for {book_data['barcode']}: {vertex_ai_results}"
                )
                barcode = book_data["barcode"]
                current_data = extracted_data[
                    barcode
                ]  # Get the latest data for this barcode

                # Replace "Unknown" with empty string
                for k, v in vertex_ai_results.items():
                    if v == "Unknown":
                        vertex_ai_results[k] = ""

                # Update the classification in current_data
                if vertex_ai_results.get(
                    "classification"
                ) and not current_data.get("call_number"):
                    current_data["call_number"] = clean_call_number(
                        vertex_ai_results["classification"],
                        current_data.get("genres", []),
                        current_data.get("google_genres", []),
                    )
                    print(
                        f"      Updated call_number for {barcode}: {current_data.get('call_number')}"
                    )

                if vertex_ai_results.get(
                    "series_title"
                ) and not current_data.get("series_name"):
                    current_data["series_name"] = vertex_ai_results[
                        "series_title"
                    ]
                if vertex_ai_results.get(
                    "volume_number"
                ) and not current_data.get("volume_number"):
                    current_data["volume_number"] = vertex_ai_results[
                        "volume_number"
                    ]
                if vertex_ai_results.get(
                    "copyright_year"
                ) and not current_data.get("publication_year"):
                    current_data["publication_year"] = vertex_ai_results[
                        "copyright_year"
                    ]
            time.sleep(1)  # Rate limit for Vertex AI batches

    with open("extracted_data.json", "w") as f:
        json.dump(extracted_data, f, indent=4)

    # update_status("2.2.3", "DONE")  # Set status to DONE


if __name__ == "__main__":
    enrich_data_with_loc()
