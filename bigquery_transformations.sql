-- UDF for clean_title (moving leading articles)
CREATE OR REPLACE FUNCTION barcode.clean_title(title STRING) RETURNS STRING AS (
    CASE
        WHEN STARTS_WITH(title, 'The ') THEN CONCAT(SUBSTR(title, 5), ', The')
        WHEN STARTS_WITH(title, 'A ') THEN CONCAT(SUBSTR(title, 3), ', A')
        WHEN STARTS_WITH(title, 'An ') THEN CONCAT(SUBSTR(title, 4), ', An')
        ELSE title
    END
);

-- UDF for capitalize_title_mla (simplified for SQL)
-- This is a complex UDF and might be better handled in a dataflow job or external process
-- For a pure SQL UDF, it would be very verbose. A simplified version might just capitalize first letter of each word.
-- For now, I'll provide a basic capitalization.
CREATE OR REPLACE FUNCTION barcode.capitalize_title_mla(title STRING) RETURNS STRING AS (
    INITCAP(title) -- This is a simplification, MLA rules are more complex
);

-- UDF for clean_author (Last, First Middle)
CREATE OR REPLACE FUNCTION barcode.clean_author(author STRING) RETURNS STRING AS (
    CASE
        WHEN STRPOS(author, ',') > 0 THEN TRIM(author)
        ELSE
            CASE
                WHEN ARRAY_LENGTH(SPLIT(author, ' ')) = 2 THEN CONCAT(SPLIT(author, ' ')[OFFSET(1)], ', ', SPLIT(author, ' ')[OFFSET(0)])
                WHEN ARRAY_LENGTH(SPLIT(author, ' ')) > 2 THEN CONCAT(SPLIT(author, ' ')[OFFSET(ARRAY_LENGTH(SPLIT(author, ' ')) - 1)], ', ', SUBSTR(author, 0, LENGTH(author) - LENGTH(SPLIT(author, ' ')[OFFSET(ARRAY_LENGTH(SPLIT(author, ' ')) - 1)]) - 1))
                ELSE author
            END
    END
);

-- UDF for extract_year
CREATE OR REPLACE FUNCTION barcode.extract_year(date_string STRING) RETURNS STRING AS (
    REGEXP_EXTRACT(date_string, r'[()©c]?(\d{4})[]]?')
);

-- Placeholder for lcc_to_ddc and clean_call_number
-- These are complex and likely require JavaScript UDFs or external processing.
-- For now, we'll leave them as placeholders.
-- CREATE OR REPLACE FUNCTION barcode.lcc_to_ddc(lcc STRING) RETURNS STRING AS (...);
-- CREATE OR REPLACE FUNCTION barcode.clean_call_number(call_num_str STRING, genres ARRAY<STRING>, google_genres ARRAY<STRING>, title STRING) RETURNS STRING AS (...);

-- Example of how to use these UDFs in a SELECT statement
/*
SELECT
    barcode.clean_title(t.original_title) AS cleaned_title,
    barcode.capitalize_title_mla(t.original_title) AS capitalized_title,
    barcode.clean_author(t.original_author) AS cleaned_author,
    barcode.extract_year(t.original_publication_year) AS extracted_year
FROM
    your_raw_marc_table AS t;
*/
