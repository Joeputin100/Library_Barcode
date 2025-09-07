import re
import logging
from api_calls import get_book_metadata_initial_pass, get_vertex_ai_classification_batch
from caching import load_cache, save_cache
from data_transformers import (
    clean_title,
    capitalize_title_mla,
    clean_author,
    clean_call_number,
    clean_series_number,
    extract_year,
)
from loc_enricher import get_loc_data

tui_logger = logging.getLogger(__name__)


def read_input_file(file_path):
    """Reads a text file with one book identifier per line."""
    with open(file_path, "r") as f:
        return [line.strip() for line in f]


def enrich_book_data(book_identifiers, cache):
    """Enriches a list of book identifiers with data from various APIs."""
    for identifier in book_identifiers:
        if re.match(r"^\d{10}(\d{3})?$", identifier):
            isbn = identifier
            title, author, lccn = "", "", ""
        else:
            isbn = ""
            parts = identifier.split(" - ")
            title = parts[0] if parts else identifier
            author = parts[1] if len(parts) > 1 else ""
            lccn = ""

        if lccn:
            call_number = get_loc_data(lccn)
        else:
            call_number = ""

        google_meta, google_cached, loc_cached, google_success, loc_success, vertex_ai_success = get_book_metadata_initial_pass(
            title, author, isbn, lccn, cache
        )

        data = {
            "input_identifier": identifier,
            "isbn": isbn,
            "lccn": lccn,
            "title": google_meta.get("title", title),
            "author": google_meta.get("author", author),
            "call_number": call_number,
            "series_title": google_meta.get("series_name", ""),
            "series_number": google_meta.get("volume_number", ""),
            "copyright_year": google_meta.get("publication_year", ""),
            "publication_date": google_meta.get("publication_year", ""),
            "cost": None,
            "price": None,
            "description": google_meta.get("description", ""),
            "summary": "",
            "subject_headings": ", ".join(
                google_meta.get("google_genres", []) + google_meta.get("genres", [])
            ),
            "notes": "",
            "dust_jacket_url": "",
            "raw_marc": "",
            "enriched_marc": "",
            "status": "new",
            "last_modified": None,
            "vertex_ai_classification": google_meta.get("vertex_ai_classification", ""),
            "vertex_ai_confidence": google_meta.get("vertex_ai_confidence", 0.0),
        }

        data["title"] = capitalize_title_mla(clean_title(data["title"]))
        data["author"] = clean_author(data["author"])
        data["call_number"] = clean_call_number(
            data["call_number"],
            google_meta.get("genres", []),
            google_meta.get("google_genres", []),
            data["title"],
        )
        data["series_number"] = clean_series_number(data["series_number"])
        data["copyright_year"] = extract_year(data["copyright_year"])

        key_fields = ["call_number", "series_title", "copyright_year", "subject_headings"]
        completeness_score = sum(1 for field in key_fields if data.get(field)) / len(key_fields)

        metrics = {
            "google_cached": google_cached,
            "loc_cached": loc_cached,
            "google_success": google_success,
            "loc_success": loc_success,
            "completeness_score": completeness_score,
        }

        yield data, metrics
        tui_logger.info(f"Enriched data: {data}")


def enrich_with_vertex_ai(books, cache):
    """Enriches a list of books with missing info using Vertex AI in batches."""
    books_to_process = [book for book in books if not book.get("call_number")]
    if not books_to_process:
        yield len(books), books # Yield total and final list
        return

    BATCH_SIZE = 5
    for i in range(0, len(books_to_process), BATCH_SIZE):
        batch = books_to_process[i:i + BATCH_SIZE]
        classifications, _ = get_vertex_ai_classification_batch(batch, cache)
        # Merge results back into the original list
        for book in batch:
            for classification in classifications:
                if book["title"] == classification["title"] and book["author"] == classification["author"]:
                    if not book.get("call_number") and classification.get("classification"):
                        book["call_number"] = classification["classification"]
                    if not book.get("series_title") and classification.get("series_title"):
                        book["series_title"] = classification["series_title"]
                    if not book.get("volume_number") and classification.get("volume_number"):
                        book["volume_number"] = classification["volume_number"]
                    if not book.get("copyright_year") and classification.get("copyright_year"):
                        book["copyright_year"] = classification["copyright_year"]
        yield len(batch), batch
        tui_logger.info(f"Enriched batch: {batch}")


def insert_books_to_bigquery(books, client):
    """Inserts a list of book data into the BigQuery table."""
    from google.cloud import bigquery
    from google.api_core import exceptions

    dataset_id = f"{client.project}.barcode"
    table_id = f"{dataset_id}.new_books"

    try:
        client.get_dataset(dataset_id)
    except exceptions.NotFound:
        print(f"Dataset {dataset_id} not found. Creating it.")
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        client.create_dataset(dataset, timeout=30)

    try:
        client.get_table(table_id)
    except exceptions.NotFound:
        print(f"Table {table_id} not found. Creating it.")
        schema = [
            bigquery.SchemaField("input_identifier", "STRING"),
            bigquery.SchemaField("isbn", "STRING"),
            bigquery.SchemaField("lccn", "STRING"),
            bigquery.SchemaField("title", "STRING"),
            bigquery.SchemaField("author", "STRING"),
            bigquery.SchemaField("call_number", "STRING"),
            bigquery.SchemaField("series_title", "STRING"),
            bigquery.SchemaField("series_number", "STRING"),
            bigquery.SchemaField("copyright_year", "STRING"),
            bigquery.SchemaField("publication_date", "STRING"),
            bigquery.SchemaField("cost", "FLOAT"),
            bigquery.SchemaField("price", "FLOAT"),
            bigquery.SchemaField("description", "STRING"),
            bigquery.SchemaField("summary", "STRING"),
            bigquery.SchemaField("subject_headings", "STRING"),
            bigquery.SchemaField("notes", "STRING"),
            bigquery.SchemaField("dust_jacket_url", "STRING"),
            bigquery.SchemaField("raw_marc", "STRING"),
            bigquery.SchemaField("enriched_marc", "STRING"),
            bigquery.SchemaField("status", "STRING"),
            bigquery.SchemaField("last_modified", "TIMESTAMP"),
        ]
        table = bigquery.Table(table_id, schema=schema)
        client.create_table(table)

    if not books:
        print("No books to insert.")
        return

    tui_logger.info(f"Inserting books: {books}")
    BATCH_SIZE = 50
    for i in range(0, len(books), BATCH_SIZE):
        batch = books[i:i + BATCH_SIZE]
        errors = client.insert_rows_json(table_id, batch)
        if not errors:
            print(f"Batch {i // BATCH_SIZE + 1} inserted successfully.")
        else:
            print(f"Encountered errors while inserting rows in batch {i // BATCH_SIZE + 1}: {errors}")


if __name__ == "__main__":
    from google.cloud import bigquery

    with open("book_list.txt", "w") as f:
        f.write("9780765326355\n")
        f.write("The Way of Kings - Brandon Sanderson\n")

    cache = load_cache()
    book_identifiers = read_input_file("book_list.txt")
    enriched_books = [book for book, metrics in enrich_book_data(book_identifiers, cache)]
    save_cache(cache)
    client = bigquery.Client()
    insert_books_to_bigquery(enriched_books, client)
    print("Done.")