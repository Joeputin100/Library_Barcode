import json
import re
from data_transformers import clean_call_number, clean_title, capitalize_title_mla, clean_author, extract_year


def clean_data():
    """
    Cleans and normalizes the extracted and enriched data.
    """
    with open("extracted_data.json", "r") as f:
        extracted_data = json.load(f)

    cleaned_data = {}
    for barcode, data in extracted_data.items():
        cleaned_data[barcode] = {
            "title": capitalize_title_mla(clean_title(data.get("title", ""))),
            "author": clean_author(data.get("author", "")),
            "lccn": data.get("lccn"),
            "call_number": clean_call_number(
                data.get("call_number", ""),
                data.get("genres", []),
                data.get("google_genres", []),
                title=data.get("title", ""),
            ),
            "series_name": data.get("series_name"),
            "volume_number": data.get("volume_number"),
            "publication_year": extract_year(data.get("publication_year", "")),
        }

    with open("cleaned_data.json", "w") as f:
        json.dump(cleaned_data, f, indent=4)


if __name__ == "__main__":
    clean_data()
