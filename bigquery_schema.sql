CREATE TABLE barcode.marc_records (
    raw_marc STRING,
    title STRING,
    author STRING,
    isbn STRING,
    lccn STRING,
    call_number STRING,
    holding_barcode STRING,
    series_title STRING,
    series_number STRING,
    copyright_year STRING,
    publication_date STRING,
    last_modified TIMESTAMP
);

CREATE OR REPLACE FUNCTION barcode.clean_title(title STRING) AS ((
  REGEXP_REPLACE(title, r'[\/\:\*\?"<>\|]', '')
));

CREATE OR REPLACE FUNCTION barcode.capitalize_title_mla(title STRING) AS ((
  -- This is a simplified version of MLA capitalization.
  -- A more robust implementation would require a dictionary of words to not capitalize.
  (SELECT STRING_AGG(CASE WHEN part IN ('a', 'an', 'the', 'and', 'but', 'or', 'for', 'nor', 'on', 'at', 'to', 'from', 'by', 'with') AND i > 0 THEN part ELSE CONCAT(UPPER(SUBSTR(part, 1, 1)), SUBSTR(part, 2)) END, ' ' ORDER BY i) FROM UNNEST(SPLIT(title, ' ')) AS part WITH OFFSET i)
));

CREATE OR REPLACE FUNCTION barcode.clean_author(author STRING) AS ((
  REGEXP_REPLACE(author, r'[\.,]$', '')
));

CREATE OR REPLACE FUNCTION barcode.clean_series_number(series_number STRING) AS ((
  REGEXP_REPLACE(series_number, r'[^0-9]', '')
));

CREATE OR REPLACE FUNCTION barcode.extract_year(date_string STRING) AS ((
  (SELECT REGEXP_EXTRACT(date_string, r'(1[7-9]\d{2}|20\d{2})'))
));