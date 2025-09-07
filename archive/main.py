import base64
import json
import os
from google.cloud import bigquery
from api_calls import get_book_metadata_initial_pass
from caching import load_cache, save_cache


def process_new_marc_record(event, context):
    """Triggered by a new row in the MARC records table."""
    pubsub_message = base64.b64decode(event["data"]).decode("utf-8")
    message_data = json.loads(pubsub_message)

    table_id = os.environ.get("TABLE_ID")
    client = bigquery.Client()

    # The message data will contain the row that was inserted.
    # I will need to parse this to get the title, author, etc.
    # This part depends on the exact format of the BigQuery event.
    # For now, I will assume it's a dictionary.
    row = message_data

    loc_cache = load_cache()

    title = row.get("title", "")
    author = row.get("author", "")
    isbn = row.get("isbn", "")
    lccn = row.get("lccn", "")

    enriched_data, _, _, _, _, _ = get_book_metadata_initial_pass(
        title, author, isbn, lccn, loc_cache
    )

    # Now, update the row in BigQuery with the enriched data.
    # I will need to construct an UPDATE statement.
    # This is a simplified example.

    query = f"""
    UPDATE `{table_id}`
    SET
        google_genres = {enriched_data.get('google_genres', [])},
        classification = '{enriched_data.get('classification', '')}',
        series_name = '{enriched_data.get('series_name', '')}',
        volume_number = '{enriched_data.get('volume_number', '')}',
        publication_year = '{enriched_data.get('publication_year', '')}',
        genres = {enriched_data.get('genres', [])}
    WHERE holding_barcode = '{row.get('holding_barcode')}'
    """

    query_job = client.query(query)
    query_job.result()

    save_cache(loc_cache)
