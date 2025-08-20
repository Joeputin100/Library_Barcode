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

    vertexai.init(
        project=project_id, location="us-central1", credentials=credentials
    )
    model = GenerativeModel("gemini-2.5-pro")

    prompt = "You are a JSON-only AI assistant. Your only function is to convert a user's request into a structured JSON object.\n\nThe user's request is: " + query_string + "\n\nYou must identify the following fields from the request and format them into a single JSON object:\n- \"type\": The type of query. Must be one of: `barcode`, `barcode_range`, `barcode_starts_with`, or `field_query`.\n- \"value\": The value for `barcode` or `field_query` types.\n- \"start\": The start value for `barcode_range` type.\n- \"end\": The end value for `barcode_range` type.\n- \"prefix\": The prefix for `barcode_starts_with` type.\n- \"field\": The field name for `field_query` type. Must be one of: `title`, `author`, `series`, `isbn`, `call number`, or `holding barcode`.\n\nFor `barcode_range` and `barcode` queries, ALWAYS extract ONLY the numeric part of the barcode for `start`, `end`, and `value` values. Remove any non-numeric prefixes (like 'b'). Pad the numeric part with leading zeros to a length of 7. For example, \"b1\" should become \"0000001\", \"b100\" should become \"0000100\". This is CRITICAL for correct barcode processing.\n\n**Field queries MUST be in the format `field: value`. For example, `author: brandon sanderson`. Queries like `all books by brandon sanderson` are NOT supported.**\n\nWhen the user uses the \"and\" operator, you must return a JSON object with a \"queries\" field, which is a list of query objects.\n\nReturn nothing but the single, valid JSON object. If you cannot parse the query, or if the query is ambiguous or unsupported, return: {\"error\": \"Unsupported or ambiguous query\"}\n"

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
