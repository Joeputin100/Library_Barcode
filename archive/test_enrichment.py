import logging
from book_importer import (
    enrich_book_data,
    enrich_with_vertex_ai,
    read_input_file,
)
from caching import load_cache, save_cache

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    cache = load_cache()
    book_identifiers = read_input_file("isbns_to_be_entered_2025088.txt")
    enriched_books = []
    for i, (book_data, metrics) in enumerate(enrich_book_data(book_identifiers, cache), start=1):
        enriched_books.append(book_data)
    final_books = []
    for classification, cached in enrich_with_vertex_ai(enriched_books, cache):
        for i, book in enumerate(enriched_books):
            if book['title'] == classification['title'] and book['author'] == classification['author']:
                if not book.get("call_number") and classification.get("classification"):
                    enriched_books[i]["call_number"] = classification["classification"]
                if not book.get("series_title") and classification.get("series_title"):
                    enriched_books[i]["series_title"] = classification["series_title"]
                if not book.get("volume_number") and classification.get("volume_number"):
                    enriched_books[i]["volume_number"] = classification["volume_number"]
                if not book.get("copyright_year") and classification.get("copyright_year"):
                    enriched_books[i]["copyright_year"] = classification["copyright_year"]
    print(enriched_books)
    save_cache(cache)
