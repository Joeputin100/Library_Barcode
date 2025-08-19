from pymarc import MARCReader
import re


def load_marc_records(file_path):
    """
    Loads MARC records from a given MARC file.

    Args:
        file_path: The path to the MARC file.

    Returns:
        A list of pymarc.Record objects.
    """
    records = []
    try:
        with open(file_path, "rb") as fh:
            reader = MARCReader(fh, force_utf8=True)
            for record in reader:
                if record:
                    records.append(record)
    except FileNotFoundError:
        print(f"Error: MARC file not found at {file_path}")
    except Exception as e:
        print(f"An error occurred while reading MARC file: {e}")
    return records


def get_field_value(record, field_name):
    """
    Helper function to get the value of a specific field from a MARC record.
    """
    if field_name == "author":
        # 100: Main Entry - Personal Name, 700: Added Entry - Personal Name
        author_fields = record.get_fields("100", "700")
        return [f.format_field() for f in author_fields]
    elif field_name == "title":
        # 245: Title Statement
        title_field = record.get_fields("245")
        return [f.format_field() for f in title_field]
    elif field_name == "series":
        # 490: Series Statement, 830: Series Added Entry - Uniform Title
        series_fields = record.get_fields("490", "830")
        return [f.format_field() for f in series_fields]
    elif field_name == "isbn":
        # 020: International Standard Book Number
        isbn_fields = record.get_fields("020")
        return [f["a"] for f in isbn_fields if "a" in f]
    elif field_name == "lccn":
        # 010: Library of Congress Control Number
        lccn_fields = record.get_fields("010")
        return [f["a"] for f in lccn_fields if "a" in f]
    elif field_name == "call number":
        # 050: Library of Congress Call Number, 090: Local Call Number
        call_number_fields = record.get_fields("050", "090")
        return [f.format_field() for f in call_number_fields]
    elif field_name == "holding barcode":
        # 852: Location and Access (common for barcode in subfield p)
        barcode_fields = record.get_fields("852")
        return [f["p"] for f in barcode_fields if "p" in f]
    return []


def _normalize_barcode_for_comparison(barcode_str):
    """
    Normalizes an alphanumeric barcode string for numerical comparison.
    Extracts the leading non-digit prefix and the numeric part.
    Converts prefix to lowercase and pads the numeric part with leading zeros.
    Returns (lowercase_prefix, padded_numeric_part).
    """
    match = re.match(r"([a-zA-Z]*)([0-9]+)", barcode_str)
    if match:
        prefix = match.group(1).lower()
        number_str = match.group(2)
        # Pad the numeric part to 7 digits with leading zeros
        padded_number = number_str.zfill(7)
        return (prefix, padded_number)
    return (barcode_str.lower(), "")  # Fallback for purely non-numeric or unparseable


def _apply_single_query(records, query):
    """
    Applies a single query to a list of records.
    """
    query_type = query.get("type")
    if not query_type:
        return records

    filtered_records = []
    for record in records:
        if query_type == "barcode":
            holding_barcodes = get_field_value(record, "holding barcode")
            # Case-insensitive and leading zero insensitive comparison
            query_value_norm = _normalize_barcode_for_comparison(query.get("value"))
            for barcode_str in holding_barcodes:
                if _normalize_barcode_for_comparison(barcode_str) == query_value_norm:
                    filtered_records.append(record)
                    break
        elif query_type == "barcode_range":
            holding_barcodes = get_field_value(record, "holding barcode")
            start_norm = _normalize_barcode_for_comparison(query.get("start", "0"))
            end_norm = _normalize_barcode_for_comparison(query.get("end", "0"))

            print(
                f"DEBUG: Barcode Range Query - Start: {query.get('start')}, End: {query.get('end')}"
            )
            print(f"DEBUG: Normalized Start: {start_norm}, Normalized End: {end_norm}")

            for barcode_str in holding_barcodes:
                current_norm = _normalize_barcode_for_comparison(barcode_str)
                print(
                    f"DEBUG: Checking barcode: {barcode_str}, Normalized: {current_norm}"
                )

                # Check if prefixes match and numeric part is within range
                if (
                    current_norm[0] == start_norm[0]
                    and current_norm[0] == end_norm[0]
                    and start_norm[1] <= current_norm[1] <= end_norm[1]
                ):
                    filtered_records.append(record)
                    print(f"DEBUG: Match found for {barcode_str}")
                    break
        elif query_type == "barcode_starts_with":
            holding_barcodes = get_field_value(record, "holding barcode")
            prefix = query.get("prefix", "").lower()
            for barcode_str in holding_barcodes:
                if barcode_str.lower().startswith(prefix):
                    filtered_records.append(record)
                    break
        elif query_type == "barcode_list":
            for value in query.get("values", []):
                if isinstance(value, str):
                    # Single barcode
                    holding_barcodes = get_field_value(record, "holding barcode")
                    query_value_norm = _normalize_barcode_for_comparison(value)
                    for barcode_str in holding_barcodes:
                        if (
                            _normalize_barcode_for_comparison(barcode_str)
                            == query_value_norm
                        ):
                            filtered_records.append(record)
                            break
                elif isinstance(value, dict) and value.get("type") == "barcode_range":
                    # Barcode range
                    holding_barcodes = get_field_value(record, "holding barcode")
                    start_norm = _normalize_barcode_for_comparison(
                        value.get("start", "0")
                    )
                    end_norm = _normalize_barcode_for_comparison(value.get("end", "0"))
                    for barcode_str in holding_barcodes:
                        current_norm = _normalize_barcode_for_comparison(barcode_str)
                        if (
                            current_norm[0] == start_norm[0]
                            and current_norm[0] == end_norm[0]
                            and start_norm[1] <= current_norm[1] <= end_norm[1]
                        ):
                            filtered_records.append(record)
                            break
        elif query_type == "field_query":
            field_name = query.get("field")
            query_value = query.get("value", "").lower()
            if not field_name:
                continue
            field_values = get_field_value(record, field_name)
            for value in field_values:
                if query_value in value.lower():
                    filtered_records.append(record)
                    break
    return filtered_records


def filter_marc_records(records, parsed_query):
    """
    Filters MARC records based on a parsed query.
    Can handle single queries or a list of queries.
    """
    if not parsed_query:
        return records

    if "queries" in parsed_query:
        # Multiple queries (AND logic)
        filtered_records = records
        for query in parsed_query["queries"]:
            filtered_records = _apply_single_query(filtered_records, query)
        return filtered_records
    else:
        # Single query
        return _apply_single_query(records, parsed_query)


if __name__ == "__main__":
    # Example usage
    marc_file = "cimb_bibliographic.marc"
    all_records = load_marc_records(marc_file)
    print(f"Loaded {len(all_records)} records from {marc_file}")

    from query_parser import parse_query

    test_queries = [
        "barcode 00002274",
        "barcode 00002274-00002276",
        "barcodes starting with 000022",
        "author: Adams, Will",
        "title: Alexander Cipher",
        "series: BookSysInc",  # This is a placeholder, actual series might be different
        "isbn: 978-0-446-40470-9",
        "call number: FIC",
        "holding barcode: 00002274",
        "find books by brandon sanderson in the stormlight archive series",
    ]

    for q_str in test_queries:
        parsed = parse_query(q_str)
        if parsed:
            filtered = filter_marc_records(all_records, parsed)
            print(f"\nQuery: '{q_str}' -> Found {len(filtered)} records.")
            for rec in filtered[:3]:  # Print first 3 records for brevity
                print(
                    f"  - Barcode: {get_field_value(rec, 'holding barcode')}, Title: {get_field_value(rec, 'title')}"
                )
        else:
            print(f"\nQuery: '{q_str}' -> Could not parse query.")
