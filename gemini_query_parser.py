import vertexai
from vertexai.generative_models import GenerativeModel
import google.auth


def parse_query_with_gemini(query_string: str) -> str:
    """
    Uses the Duet AI API to parse a natural language query into a structured JSON format.
    """
    credentials, project_id = google.auth.load_credentials_from_file(
        "vertex ai key static-webbing-461904-c4-aa8f7ec18c62.json"
    )

    vertexai.init(project=project_id, location="us-central1", credentials=credentials)
    model = GenerativeModel("gemini-2.5-pro")

    example_error = '{ "error": "Unsupported or ambiguous query" }'
    example_barcode = '{ "type": "barcode", "value": "12345" }'
    example_range_100_200 = '{ "type": "barcode_range", "start": "100", "end": "200" }'
    example_range_b1_b30 = '{ "type": "barcode_range", "start": "B1", "end": "B30" }'
    example_range_b000100_b000200 = (
        '{ "type": "barcode_range", "start": "B000100", "end": "B000200" }'
    )
    example_starts_with_b = '{ "type": "barcode_starts_with", "prefix": "B" }'
    example_author = (
        '{ "type": "field_query", "field": "author", "value": "brandon sanderson" }'
    )
    example_series = (
        '{ "type": "field_query", "field": "series", "value": "stormlight archive" }'
    )
    example_author_and_series = '{ "queries": [{ "type": "field_query", "field": "author", "value": "brandon sanderson" }, { "type": "field_query", "field": "series", "value": "stormlight archive" }] }'
    example_barcode_list = '{ "type": "barcode_list", "values": ["b100", { "type": "barcode_range", "start": "3957", "end": "4000" }] }'
    example_b123 = '{ "type": "barcode", "value": "123" }'
    example_b1 = '{ "type": "barcode", "value": "B000001" }'

    prompt = f"""You are a JSON-only AI assistant. Your only function is to convert a user's request into a structured JSON object.

The user's request is: "{query_string}"

You must identify the following fields from the request and format them into a single JSON object:
- \"type\": The type of query. Must be one of: `barcode`, `barcode_range`, `barcode_starts_with`, or `field_query`.
- \"value\": The value for `barcode` or `field_query` types.
- \"start\": The start value for `barcode_range` type.
- \"end\": The end value for `barcode_range` type.
- \"prefix\": The prefix for `barcode_starts_with` type.
- \"field\": The field name for `field_query` type. Must be one of: `title`, `author`, `series`, `isbn`, `call number`, or `holding barcode`.

For `barcode_range` and `barcode` queries, ALWAYS extract ONLY the numeric part of the barcode for `start`, `end`, and `value` values. Remove any non-numeric prefixes (like 'b'). Pad the numeric part with leading zeros to a length of 7. For example, "b1" should become "0000001", "b100" should become "0000100". This is CRITICAL for correct barcode processing.

**Field queries MUST be in the format `field: value`. For example, `author: brandon sanderson`. Queries like `all books by brandon sanderson` are NOT supported.**

Return nothing but the single, valid JSON object. If you cannot parse the query, or if the query is ambiguous or unsupported, return: {example_error}

Here are some examples:
- \"find barcode 12345\" -> {example_barcode}
- \"show me barcodes from 100 to 200\" -> {example_range_100_200}
- \"show me barcodes from B1 to B30\" -> {example_range_b1_b30}
- \"show me barcodes from B000100 to B000200\" -> {example_range_b000100_b000200}
- \"find all barcodes starting with B\" -> {example_starts_with_b}
- \"author: brandon sanderson\" -> {example_author}
- \"series: stormlight archive\" -> {example_series}
- \"author: brandon sanderson and series: stormlight archive\" -> {example_author_and_series}
- \"b100 and 3957-4000\" -> {example_barcode_list}
- \"find books by brandon sanderson in the stormlight archive series\" -> {example_error}
- \"all barcodes by Aveyard.\" -> {example_error}
- \"b1-b290 but not b100\" -> {example_error}
- \"b123\" -> {example_b123}
- \"b1\" -> {example_b1}
- \"unsupported query\" -> {example_error}
"""

    response = model.generate_content(prompt)
    try:
        # Extract the text from the response
        text_response = response.candidates[0].content.parts[0].text
        # Clean the text to remove markdown
        if "```json" in text_response:
            text_response = text_response.split("```json")[1].split("```")[0]
        elif "```" in text_response:
            text_response = text_response.split("```")[1].split("```")[0]
        return text_response.strip()
    except (IndexError, AttributeError) as e:
        print(f"Error processing model response: {e}")
        return "{}"
