CREATE TABLE barcode.enriched_marc_records (
    title STRING,
    author STRING,
    isbn STRING,
    barcode STRING,
    lccn STRING,
    call_number STRING,
    series_name STRING,
    volume_number STRING,
    publication_year STRING,
    genres ARRAY<STRING>,
    google_genres ARRAY<STRING>
);