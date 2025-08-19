from marc_processor import load_marc_records, get_field_value


def get_all_barcodes():
    """Reads all records from the MARC file and returns a set of all unique barcodes."""
    all_records = load_marc_records("cimb_bibliographic.marc")
    all_barcodes = set()
    for record in all_records:
        barcodes = get_field_value(record, "holding barcode")
        for barcode in barcodes:
            all_barcodes.add(barcode)
    return all_barcodes


if __name__ == "__main__":
    unique_barcodes = get_all_barcodes()
    # Sort the barcodes for easier analysis
    sorted_barcodes = sorted(list(unique_barcodes))
    for barcode in sorted_barcodes:
        print(barcode)
