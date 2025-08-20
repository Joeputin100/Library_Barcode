import re
from google.cloud import bigquery
from api_calls import get_book_metadata_initial_pass
from caching import load_cache, save_cache
from data_cleaning import (
    clean_title,
    capitalize_title_mla,
    clean_author,
    clean_call_number,
    clean_series_number,
    extract_year,
)
from loc_enricher import get_loc_data


def read_input_file(file_path):
    """Reads a text file with one book identifier per line."""
    with open(file_path, "r") as f:
        return [line.strip() for line in f]


def enrich_book_data(book_identifiers):
    """Enriches a list of book identifiers with data from various APIs."""
    loc_cache = load_cache()
    enriched_data = []

    for identifier in book_identifiers:
        # Determine if the identifier is an ISBN or a title/author combo
        if re.match(r"^\d{10}(\d{3})?$", identifier):
            isbn = identifier
            title = ""
            author = ""
            lccn = ""
        else:
            isbn = ""
            parts = identifier.split(" - ")
            if len(parts) == 2:
                title = parts[0]
                author = parts[1]
            else:
                title = identifier
                author = ""
            lccn = ""

        # Get data from LOC
        if lccn:
            call_number = get_loc_data(lccn)
        else:
            call_number = ""

        # Get data from Google Books and LOC
        google_meta, _, _ = get_book_metadata_initial_pass(
            title, author, isbn, lccn, loc_cache
        )

        # Combine data
        data = {
            "input_identifier": identifier,
            "isbn": isbn,
            "lccn": lccn,
            "title": title,
            "author": author,
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
                google_meta.get("google_genres", [])
                + google_meta.get("genres", [])
            ),
            "notes": "",
            "dust_jacket_url": "",
            "raw_marc": "",
            "enriched_marc": "",
            "status": "new",
            "last_modified": None,
        }

        # Clean data
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

        enriched_data.append(data)

    # Use Vertex AI for gap-filling
    # This part will be implemented later

    save_cache(loc_cache)
    return enriched_data


def insert_books_to_bigquery(books, client):
    """Inserts a list of book data into the BigQuery table."""

    table_id = (
        "barcode.new_books"  # Replace with your project and dataset if needed
    )

    errors = client.insert_rows_json(table_id, books)
    if errors == []:
        print("New books have been added to BigQuery.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))


if __name__ == "__main__":
    # This is for testing purposes
    with open("book_list.txt", "w") as f:
        f.write("9780765326355\n")
        f.write("The Way of Kings - Brandon Sanderson\n")

    book_identifiers = read_input_file("book_list.txt")
    enriched_books = enrich_book_data(book_identifiers)
    client = bigquery.Client()
    insert_books_to_bigquery(enriched_books, client)
    print("Done.")
