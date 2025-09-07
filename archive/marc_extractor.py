import pymarc
import json


def extract_marc_data():
    """
    Extracts key information from MARC records and saves it to a JSON file.
    """
    field_mapping = {
        "barcode": ("852", "p"),
        "title": ("245", "a"),
        "author": ("100", "a"),
        "lccn": ("010", "a"),
    }

    extracted_data = {}
    marc_files = ["cimb.marc", "cimb_bibliographic.marc"]

    for marc_file in marc_files:
        with open(marc_file, "rb") as fh:
            reader = pymarc.MARCReader(fh)
            for record in reader:
                if record is None:
                    continue

                barcode_field = record.get(field_mapping["barcode"][0])
                if barcode_field:
                    barcodes = barcode_field.get_subfields(
                        field_mapping["barcode"][1]
                    )
                    if barcodes:
                        barcode = barcodes[0]

                        title_field = record.get(field_mapping["title"][0])
                        author_field = record.get(field_mapping["author"][0])
                        lccn_field = record.get(field_mapping["lccn"][0])

                        title = (
                            title_field.get_subfields(
                                field_mapping["title"][1]
                            )[0]
                            if title_field
                            and title_field.get_subfields(
                                field_mapping["title"][1]
                            )
                            else None
                        )
                        author = (
                            author_field.get_subfields(
                                field_mapping["author"][1]
                            )[0]
                            if author_field
                            and author_field.get_subfields(
                                field_mapping["author"][1]
                            )
                            else None
                        )

                        lccn_subfields = (
                            lccn_field.get_subfields(field_mapping["lccn"][1])
                            if lccn_field
                            else []
                        )
                        lccn = lccn_subfields[0] if lccn_subfields else None

                        if barcode not in extracted_data:
                            extracted_data[barcode] = {}

                        # Merge data, giving preference to already existing data
                        if not extracted_data[barcode].get("title"):
                            extracted_data[barcode]["title"] = title
                        if not extracted_data[barcode].get("author"):
                            extracted_data[barcode]["author"] = author
                        if not extracted_data[barcode].get("lccn"):
                            extracted_data[barcode]["lccn"] = lccn

    with open("extracted_data.json", "w") as f:
        json.dump(extracted_data, f, indent=4)


if __name__ == "__main__":
    extract_marc_data()
