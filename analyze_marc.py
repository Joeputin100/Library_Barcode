import json
from pymarc import MARCReader


def analyze_marc_file(file_path):
    marc_fields = {}
    with open(file_path, 'rb') as fh:
        reader = MARCReader(fh)
        for record in reader:
            if record:
                for field in record.fields:
                    if field.tag not in marc_fields:
                        marc_fields[field.tag] = []
                    if hasattr(field, 'subfields'):
                        for subfield in field.subfields:
                            if subfield.code not in marc_fields[field.tag]:
                                marc_fields[field.tag].append(subfield.code)
    return marc_fields


if __name__ == "__main__":
    atriuum_schema = analyze_marc_file('cimb_bibliographic.marc')
    with open('atriuum_marc_schema.json', 'w') as f:
        json.dump(atriuum_schema, f, indent=4, sort_keys=True)
    print("Successfully analyzed MARC file and created atriuum_marc_schema.json")

