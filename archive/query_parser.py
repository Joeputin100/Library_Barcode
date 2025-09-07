from gemini_query_parser import parse_query_with_gemini
import json


def parse_query(query_string: str) -> dict:
    """
    Parses a natural language query into a structured query dictionary.
    """
    try:
        response_text = parse_query_with_gemini(query_string)
        response_json = json.loads(response_text)
        if "error" in response_json:
            return {"error": response_json["error"]}
        return response_json
    except (json.JSONDecodeError, Exception) as e:
        print(f"Error parsing query: {e}")
        return None


if __name__ == "__main__":
    # Example usage:
    queries = [
        "barcode 12345",
        "barcodes from b100 to b200",
        "barcodes starting with B",
        "author: brandon sanderson",
        "title: the way of kings",
        "series: stormlight archive",
        "isbn: 9780765326355",
        "call number: F SAN",
        "holding barcode: 31234567890123",
        "all barcodes by Aveyard.",
        "b1-b290 but not b100",
    ]

    for query in queries:
        parsed = parse_query(query)
        print(f"Query: '{query}'\nParsed: {parsed}\n")
