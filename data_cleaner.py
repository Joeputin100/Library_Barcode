import json
import re
from external_enricher import clean_call_number


def clean_title(title):
    """Cleans title by moving leading articles to the end."""
    if not isinstance(title, str):
        return ""
    articles = ['The ', 'A ', 'An ']
    for article in articles:
        if title.startswith(article):
            return title[len(article):] + ", " + title[:len(article) - 1]
    return title


def capitalize_title_mla(title):
    """Capitalizes a title according to MLA standards."""
    if not isinstance(title, str) or not title:
        return ""

    words = title.lower().split()
    # List of articles, prepositions, and conjunctions that should not be capitalized
    # unless they are the first or last word.
    minor_words = ['a', 'an', 'the', 'and', 'but', 'or', 'for', 'nor', 'on', 'at', 'to', 'from', 'by', 'in', 'of',
                   'off', 'out', 'up', 'so', 'yet']

    capitalized_words = []
    for i, word in enumerate(words):
        if i == 0 or i == len(words) - 1 or word not in minor_words:
            capitalized_words.append(word.capitalize())
        else:
            capitalized_words.append(word)

    return " ".join(capitalized_words)


def clean_author(author):
    """Cleans author name to Last, First Middle."""
    if not isinstance(author, str):
        return ""
    parts = author.split(',')
    if len(parts) == 2:
        return f"{parts[0].strip()}, {parts[1].strip()}"
    return author


def extract_year(date_string):
    """Extracts the first 4-digit number from a string, assuming it's a year."""
    if isinstance(date_string, str):
        # Regex to find a 4-digit year, ignoring surrounding brackets, c, or ©
        match = re.search(r'[\(\)\[©c]?(\d{4})[\)\]]?', date_string)
        if match:
            return match.group(1)
    return ""


def clean_data():
    """
    Cleans and normalizes the extracted and enriched data.
    """
    with open('extracted_data.json', 'r') as f:
        extracted_data = json.load(f)

    cleaned_data = {}
    for barcode, data in extracted_data.items():
        cleaned_data[barcode] = {
            'title': capitalize_title_mla(clean_title(data.get('title', ''))),
            'author': clean_author(data.get('author', '')),
            'lccn': data.get('lccn'),
            'call_number': clean_call_number(
                data.get('call_number', ''),
                data.get('genres', []),
                data.get('google_genres', []),
                title=data.get('title', '')
            ),
            'series_name': data.get('series_name'),
            'volume_number': data.get('volume_number'),
            'publication_year': extract_year(data.get('publication_year', '')),
        }

    with open('cleaned_data.json', 'w') as f:
        json.dump(cleaned_data, f, indent=4)


if __name__ == '__main__':
    clean_data()
