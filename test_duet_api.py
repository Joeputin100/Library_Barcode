import pytest
from duet_api import parse_query_with_duet
import json


@pytest.mark.parametrize(
    "query, expected_json",
    [
        ("barcode 12345", {"type": "barcode", "value": "12345"}),
        (
            "barcodes from b100 to b200",
            {"type": "barcode_range", "start": "100", "end": "200"},
        ),
        ("barcodes starting with B", {"type": "barcode_starts_with", "prefix": "B"}),
        (
            "author: brandon sanderson",
            {"type": "field_query", "field": "author", "value": "brandon sanderson"},
        ),
        (
            "all barcodes by Aveyard.",
            {"type": "field_query", "field": "author", "value": "Aveyard"},
        ),
        ("b1-b290 but not b100", {"error": "Unsupported or ambiguous query"}),
    ],
)
def test_parse_query_with_duet(query, expected_json):
    response_text = parse_query_with_duet(query)
    response_json = json.loads(response_text)
    assert response_json == expected_json
