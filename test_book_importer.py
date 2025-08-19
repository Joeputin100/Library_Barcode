import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock
from book_importer import read_input_file, enrich_book_data, insert_books_to_bigquery

def test_read_input_file(tmp_path):
    """Tests that read_input_file reads a file with one identifier per line."""
    # Create a temporary file with some test data
    file_path = tmp_path / "book_list.txt"
    file_path.write_text("9780765326355\n"  # Corrected newline escape
                         "The Way of Kings - Brandon Sanderson\n"  # Corrected newline escape
                         "\n"  # empty line should be handled gracefully, Corrected newline escape
                         "9780316160171")

    # Call the function
    result = read_input_file(file_path)

    # Assert the result
    expected = ["9780765326355", "The Way of Kings - Brandon Sanderson", "", "9780316160171"]
    assert result == expected

@patch('book_importer.get_book_metadata_initial_pass')
@patch('book_importer.save_cache')
@patch('book_importer.load_cache')
def test_enrich_book_data_isbn(mock_load_cache, mock_save_cache, mock_get_book_metadata):
    """Tests that enrich_book_data correctly processes an ISBN."""
    # Configure mocks
    mock_load_cache.return_value = {}
    mock_get_book_metadata.return_value = ({
        "series_name": "The Stormlight Archive",
        "volume_number": "1",
        "publication_year": "2010",
        "description": "A fantasy novel.",
        "google_genres": ["Fantasy"],
        "genres": [],
        "title": "The Way of Kings",
        "author": "Brandon Sanderson"
    }, None, None)

    # Call the function
    enriched_data = enrich_book_data(["9780765326355"])

    # Assertions
    assert len(enriched_data) == 1
    book = enriched_data[0]
    assert book["input_identifier"] == "9780765326355"
    assert book["isbn"] == "9780765326355"
    assert book["title"] == ""
    assert book["author"] == ""
    assert book["call_number"] == "FIC"
    assert book["series_title"] == "The Stormlight Archive"
    assert book["series_number"] == "1"
    assert book["copyright_year"] == "2010"
    mock_save_cache.assert_called_once()


@patch('book_importer.get_book_metadata_initial_pass')
@patch('book_importer.save_cache')
@patch('book_importer.load_cache')
def test_enrich_book_data_title_author(mock_load_cache, mock_save_cache, mock_get_book_metadata):
    """Tests that enrich_book_data correctly processes a title-author string."""
    # Configure mocks
    mock_load_cache.return_value = {}
    mock_get_book_metadata.return_value = ({
        "publication_year": "2010",
        "description": "A fantasy novel.",
        "google_genres": ["Fantasy"],
        "genres": ["Epic"]
    }, None, None)

    # Call the function
    enriched_data = enrich_book_data(["The Way of Kings - Brandon Sanderson"])

    # Assertions
    assert len(enriched_data) == 1
    book = enriched_data[0]
    assert book["input_identifier"] == "The Way of Kings - Brandon Sanderson"
    assert book["isbn"] == ""
    assert book["title"] == "Way of Kings, The"
    assert book["author"] == "Brandon Sanderson"
    assert book["call_number"] == "FIC"
    assert book["copyright_year"] == "2010"
    assert "Epic" in book["subject_headings"]
    mock_save_cache.assert_called_once()

@patch('book_importer.bigquery.Client')
def test_insert_books_to_bigquery_success(mock_bigquery_client):
    """Tests that insert_books_to_bigquery calls the BigQuery client correctly."""
    # Configure mock
    mock_instance = mock_bigquery_client.return_value
    mock_instance.insert_rows_json.return_value = []

    # Sample data
    books = [
        {"title": "Book 1", "author": "Author 1"},
        {"title": "Book 2", "author": "Author 2"}
    ]

    # Call the function
    insert_books_to_bigquery(books)

    # Assertions
    mock_instance.insert_rows_json.assert_called_once_with("barcode.new_books", books)

@patch('book_importer.bigquery.Client')
def test_insert_books_to_bigquery_error(mock_bigquery_client, capsys):
    """Tests that insert_books_to_bigquery handles errors from the BigQuery client."""
    # Configure mock
    mock_instance = mock_bigquery_client.return_value
    mock_instance.insert_rows_json.return_value = [{"index": 0, "errors": ["Error"]}]

    # Sample data
    books = [
        {"title": "Book 1", "author": "Author 1"}
    ]

    # Call the function
    insert_books_to_bigquery(books)

    # Assertions
    mock_instance.insert_rows_json.assert_called_once_with("barcode.new_books", books)
    captured = capsys.readouterr()
    assert "Encountered errors" in captured.out