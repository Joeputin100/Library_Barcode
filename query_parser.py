import re


def parse_query(query_str):
    """
    Parses a natural language query about MARC records.

    Args:
        query_str: The query string to parse.

    Returns:
        A dictionary representing the parsed query, or None if the query is not understood.
    """
    query_str = query_str.strip() # Keep original case for field values
    query_lower = query_str.lower()

    # Individual barcode (existing)
    match = re.match(r"^(?:barcode|b)\s+([0-9]+)$", query_lower)
    if match:
        return {"type": "barcode", "value": match.group(1)}

    # Barcode range (existing)
    match = re.match(r"^(?:barcode|b)\s+([0-9]+)\s*-\s*([0-9]+)$", query_lower)
    if match:
        return {"type": "barcode_range", "start": match.group(1), "end": match.group(2)}

    # Barcodes starting with (existing)
    match = re.match(r"^(?:barcodes|b)\s+starting\s+with\s+([0-9]+)$", query_lower)
    if match:
        return {"type": "barcode_starts_with", "prefix": match.group(1)}

    # Field-specific queries
    field_patterns = {
        "author": r"^(?:author|by):\s*(.+)$",
        "title": r"^(?:title):\s*(.+)$",
        "series": r"^(?:series):\s*(.+)$",
        "isbn": r"^(?:isbn):\s*([0-9X-]+)$",
        "call number": r"^(?:call number|cn):\s*(.+)$",
        "holding barcode": r"^(?:holding barcode|hb):\s*([0-9]+)$"
    }

    for field, pattern in field_patterns.items():
        match = re.match(pattern, query_lower)
        if match:
            # Extract the value, preserving original case for the value part
            value = query_str[match.start(1):match.end(1)].strip()
            return {"type": "field_query", "field": field, "value": value}

    return None


if __name__ == '__main__':
    queries = [
        "barcode 12345",
        "b 54321",
        "barcode 100-200",
        "b 300 - 400",
        "barcodes starting with 5",
        "b starting with 9",
        "author: Brandon Sanderson",
        "by: Jane Doe",
        "title: The Way of Kings",
        "series: Stormlight Archive",
        "isbn: 978-0765326355",
        "call number: PS3619.A49 S76 2010",
        "cn: QA76.73.P98 C66 2006",
        "holding barcode: 00004478",
        "hb: 12345678",
        "invalid query"
    ]

    for query in queries:
        parsed = parse_query(query)
        print(f"Query: '{query}' -> Parsed: {parsed}")
