-- BigQuery Schema for Mangle Integration
-- Optimized for Mangle declarative rules and real-time enrichment

CREATE TABLE IF NOT EXISTS barcode.mangle_enriched_books (
    -- Core Identifiers
    barcode STRING NOT NULL,
    isbn STRING,
    lccn STRING,
    source_system STRING NOT NULL,
    
    -- Enrichment Results (Mangle Output)
    final_title STRING NOT NULL,
    final_author STRING NOT NULL,
    final_classification STRING,
    
    -- Source Data with Provenance
    marc_title STRING,
    marc_author STRING,
    marc_call_number STRING,
    
    google_books_title STRING,
    google_books_author STRING,
    google_books_classification STRING,
    google_books_genres ARRAY<STRING>,
    google_books_series STRING,
    google_books_volume STRING,
    google_books_year STRING,
    
    vertex_ai_classification STRING,
    vertex_ai_confidence FLOAT64,
    vertex_ai_source_urls ARRAY<STRING>,
    vertex_ai_genres ARRAY<STRING>,
    vertex_ai_series_info STRING,
    vertex_ai_years STRING,
    
    -- Enrichment Metadata
    enrichment_timestamp TIMESTAMP NOT NULL,
    processing_version STRING NOT NULL,
    confidence_score FLOAT64,
    source_combination ARRAY<STRING>,
    
    -- Mangle Rule Execution Info
    mangle_rule_version STRING,
    rule_execution_time FLOAT64,
    conflicting_sources BOOL,
    
    -- Partitioning and Clustering
    PARTITION BY DATE(enrichment_timestamp),
    CLUSTER BY barcode, source_system, final_classification
)
OPTIONS (
    description = "Mangle-enriched book data with source provenance and confidence scoring",
    labels = [("project", "barcode-enrichment"), ("technology", "mangle"), ("phase", "4")]
);

-- Materialized View for Frequent Queries
CREATE MATERIALIZED VIEW IF NOT EXISTS barcode.mangle_enrichment_summary
OPTIONS (
    enable_refresh = true,
    refresh_interval_minutes = 30,
    description = "Summary view of Mangle enrichment results"
)
AS
SELECT 
    final_classification,
    source_system,
    COUNT(*) as record_count,
    AVG(confidence_score) as avg_confidence,
    APPROX_QUANTILES(rule_execution_time, 100)[OFFSET(50)] as median_processing_time,
    COUNT(DISTINCT barcode) as unique_barcodes
FROM barcode.mangle_enriched_books
GROUP BY final_classification, source_system;

-- Table for Enrichment Rules Metadata
CREATE TABLE IF NOT EXISTS barcode.mangle_rules_metadata (
    rule_id STRING NOT NULL,
    rule_name STRING NOT NULL,
    rule_description STRING,
    rule_content STRING,
    rule_version STRING NOT NULL,
    active BOOL NOT NULL,
    created_timestamp TIMESTAMP NOT NULL,
    last_modified TIMESTAMP NOT NULL,
    success_rate FLOAT64,
    execution_count INT64
)
OPTIONS (
    description = "Metadata and performance tracking for Mangle enrichment rules"
);

-- Stream for Real-time Data Processing
CREATE OR REPLACE STREAM barcode.mangle_enrichment_stream
ON TABLE barcode.mangle_enriched_books
OPTIONS (
    value_format = 'JSON',
    retry_policy = 'RETRY_NEVER'
);