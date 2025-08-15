from pymarc import MARCReader

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
        with open(file_path, 'rb') as fh:
            reader = MARCReader(fh)
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
        author_fields = record.get_fields('100', '700')
        return [f.format_field() for f in author_fields]
    elif field_name == "title":
        # 245: Title Statement
        title_field = record.get_fields('245')
        return [f.format_field() for f in title_field]
    elif field_name == "series":
        # 490: Series Statement, 830: Series Added Entry - Uniform Title
        series_fields = record.get_fields('490', '830')
        return [f.format_field() for f in series_fields]
    elif field_name == "isbn":
        # 020: International Standard Book Number
        isbn_fields = record.get_fields('020')
        return [f['a'] for f in isbn_fields if 'a' in f]
    elif field_name == "call number":
        # 050: Library of Congress Call Number, 090: Local Call Number
        call_number_fields = record.get_fields('050', '090')
        return [f.format_field() for f in call_number_fields]
    elif field_name == "holding barcode":
        # 852: Location and Access (common for barcode in subfield p)
        barcode_fields = record.get_fields('852')
        return [f['p'] for f in barcode_fields if 'p' in f]
    return []

def filter_marc_records(records, parsed_query):
    """
    Filters MARC records based on a parsed query.

    Args:
        records: A list of pymarc.Record objects.
        parsed_query: A dictionary representing the parsed query.

    Returns:
        A list of filtered pymarc.Record objects.
    """
    if not parsed_query:
        return records

    query_type = parsed_query["type"]

    filtered_records = []
    for record in records:
        if query_type == "barcode":
            holding_barcodes = get_field_value(record, "holding barcode")
            if parsed_query["value"] in holding_barcodes:
                filtered_records.append(record)
        elif query_type == "barcode_range":
            holding_barcodes = get_field_value(record, "holding barcode")
            start = int(parsed_query["start"])
            end = int(parsed_query["end"])
            for barcode_str in holding_barcodes:
                try:
                    barcode_int = int(barcode_str)
                    if start <= barcode_int <= end:
                        filtered_records.append(record)
                        break
                except ValueError:
                    continue
        elif query_type == "barcode_starts_with":
            holding_barcodes = get_field_value(record, "holding barcode")
            prefix = parsed_query["prefix"]
            for barcode_str in holding_barcodes:
                if barcode_str.startswith(prefix):
                    filtered_records.append(record)
                    break
        elif query_type == "field_query":
            field_name = parsed_query["field"]
            query_value = parsed_query["value"].lower()
            field_values = get_field_value(record, field_name)
            for value in field_values:
                if query_value in value.lower():
                    filtered_records.append(record)
                    break
    return filtered_records

if __name__ == '__main__':
    # Example usage
    marc_file = 'cimb.marc'
    all_records = load_marc_records(marc_file)
    print(f"Loaded {len(all_records)} records from {marc_file}")

    from query_parser import parse_query

    test_queries = [
        "barcode 00002274",
        "barcode 00002274-00002276",
        "barcodes starting with 000022",
        "author: Adams, Will",
        "title: Alexander Cipher",
        "series: BookSysInc", # This is a placeholder, actual series might be different
        "isbn: 978-0-446-40470-9",
        "call number: FIC",
        "holding barcode: 00002274"
    ]

    for q_str in test_queries:
        parsed = parse_query(q_str)
        if parsed:
            filtered = filter_marc_records(all_records, parsed)
            print(f"\nQuery: '{q_str}' -> Found {len(filtered)} records.")
            for rec in filtered[:3]: # Print first 3 records for brevity
                print(f"  - Barcode: {get_field_value(rec, 'holding barcode')}, Title: {get_field_value(rec, 'title')}")
        else:
            print(f"\nQuery: '{q_str}' -> Could not parse query.")
