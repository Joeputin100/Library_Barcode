
from textual.app import App, ComposeResult
from textual.widgets import DataTable, Footer, Header
from google.cloud import bigquery

class BigQueryViewer(App):
    """A Textual app to view BigQuery data."""

    BINDINGS = [("ctrl+q", "quit", "Quit")]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client = bigquery.Client(project='static-webbing-461904-c4')
        self.table_id = "barcode.new_books"

    def compose(self) -> ComposeResult:
        """Create child widgets for the app."""
        yield Header()
        yield DataTable(id="data_table")
        yield Footer()

    def on_mount(self) -> None:
        """Load data when the app is mounted."""
        self.load_data_to_table()

    def load_data_to_table(self):
        """Load data from BigQuery into the DataTable."""
        table = self.query_one(DataTable)
        query = f"SELECT * FROM `{self.table_id}`"
        try:
            query_job = self.client.query(query)
            rows = query_job.result()
            table.clear(columns=True)
            if rows.total_rows > 0:
                schema = [field.name for field in rows.schema]
                table.add_columns(*schema)
                for row in rows:
                    table.add_row(*row.values())
        except Exception as e:
            self.log(f"Error loading data from BigQuery: {e}")

if __name__ == "__main__":
    app = BigQueryViewer()
    app.run()
