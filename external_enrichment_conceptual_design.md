# External Enrichment Service: Conceptual Design

This document outlines a conceptual design for performing external data enrichment (Google Books API and Library of Congress Z39.50) in a cloud-native environment, populating the `barcode.enriched_marc_records` BigQuery table.

## Architecture Options:

### 1. Cloud Functions (Event-Driven)

*   **Trigger:** A Cloud Function could be triggered by new data landing in a BigQuery table (e.g., a raw MARC data table) or a Pub/Sub topic.
*   **Process:**
    *   The Cloud Function would read a batch of records from the raw data table.
    *   For each record, it would make calls to the Google Books API and/or the Library of Congress Z39.50 gateway.
    *   It would apply the enrichment logic (prioritizing ISBN/LCCN, falling back to author/title).
    *   The enriched data would then be written to the `barcode.enriched_marc_records` BigQuery table.
*   **Pros:** Serverless, scales automatically, cost-effective for intermittent processing.
*   **Cons:** Time limits (max 9 minutes per function execution), potential cold starts, managing state across multiple function invocations for large datasets.

### 2. Dataflow (Batch Processing)

*   **Trigger:** A Dataflow job could be scheduled (e.g., daily, weekly) or triggered manually.
*   **Process:**
    *   A Dataflow pipeline (Apache Beam) would read data from the raw MARC BigQuery table.
    *   It would use custom transforms to make external API calls to Google Books and LoC.
    *   The enriched data would be written to the `barcode.enriched_marc_records` BigQuery table.
*   **Pros:** Highly scalable for large datasets, handles complex transformations and error handling, managed service.
*   **Cons:** Higher cost than Cloud Functions for small, infrequent batches, more complex to develop and deploy.

## Data Flow:

1.  **Raw MARC Data Ingestion:** Raw MARC data (e.g., from `cimb_bibliographic.marc`) is ingested into a BigQuery table (e.g., `barcode.raw_marc_data`). This could be done via Cloud Storage and BigQuery Load Jobs.
2.  **Trigger Enrichment:**
    *   **Cloud Functions:** A BigQuery export to Cloud Storage, which triggers a Cloud Function.
    *   **Dataflow:** A scheduled Dataflow job reads directly from the raw BigQuery table.
3.  **External API Calls:** The chosen cloud service (Cloud Function or Dataflow) makes API calls to Google Books and LoC.
4.  **Data Transformation & Cleaning:** The data is enriched and cleaned using the logic defined in `data_transformers.py` (or its BigQuery SQL/JS UDF equivalents for simpler transformations).
5.  **Load to Enriched Table:** The enriched and cleaned data is loaded into the `barcode.enriched_marc_records` BigQuery table.

## Considerations:

*   **API Quotas and Rate Limits:** Implement robust error handling and retry mechanisms for external API calls to respect quotas and rate limits.
*   **Error Handling:** Design for failures in API calls and data processing. Log errors and potentially quarantine problematic records for manual review.
*   **Cost Optimization:** Choose the appropriate service (Cloud Functions vs. Dataflow) based on data volume and processing frequency.
*   **Data Freshness:** Determine the required data freshness and schedule the enrichment process accordingly.
*   **Authentication:** Ensure the cloud service has the necessary IAM permissions to access external APIs and BigQuery.
