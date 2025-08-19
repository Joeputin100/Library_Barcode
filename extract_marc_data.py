import json
from marc_processor import load_marc_records, get_field_value


def extract_and_save_marc_data(marc_file_path, output_json_path):
    records = load_marc_records(marc_file_path)
    extracted_data = []

    for record in records:
        barcode = get_field_value(record, "holding barcode")
        title = get_field_value(record, "title")
        author = get_field_value(record, "author")
        call_number = get_field_value(record, "call number")

        extracted_data.append(
            {
                "barcode": barcode[0] if barcode else None,
                "title": title[0] if title else None,
                "author": author[0] if author else None,
                "call_number": call_number[0] if call_number else None,
            }
        )

    with open(output_json_path, "w") as f:
        json.dump(extracted_data, f, indent=4)

    print(f"Extracted {len(extracted_data)} records and saved to {output_json_path}")


if __name__ == "__main__":
    marc_file = "cimb_bibliographic.marc"  # Assuming this is the primary MARC file
    output_file = "extracted_marc_data.json"
    extract_and_save_marc_data(marc_file, output_file)
