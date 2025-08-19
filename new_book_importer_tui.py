from textual.app import App, ComposeResult
from textual.widgets import Header, Footer, Tabs, Tab, Button, ProgressBar, Sparkline, DataTable, Tree
from textual.containers import Vertical
from book_importer import read_input_file, enrich_book_data, insert_books_to_bigquery
from google.cloud import bigquery
from marc_exporter import convert_df_to_marc, write_marc_file

class NewBookImporterTUI(App):
    """A Textual app to import new books."""

    CSS_PATH = "project_viewer.css"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client = bigquery.Client()
        self.table_id = "barcode.new_books"

    def compose(self) -> ComposeResult:
        """Create child widgets for the app."""
        yield Header()
        with Tabs(id="tabs"):
            with Tab("Input & Processing"):
                with Vertical():
                    yield Button("Select Input File", id="select_file_button")
                    yield Button("Process Books", id="process_books_button")
                    yield ProgressBar(id="progress_bar")
                    yield Sparkline([0, 1, 2, 3, 4, 5, 6, 7, 8, 9], id="loc_sparkline")
                    yield Sparkline([9, 8, 7, 6, 5, 4, 3, 2, 1, 0], id="google_sparkline")
                    yield Sparkline([0, 1, 0, 1, 0, 1, 0, 1, 0, 1], id="vertex_sparkline")
            with Tab("Review & Edit"):
                yield DataTable(id="data_table")
            with Tab("MARC Records"):
                yield Tree("MARC Records", id="marc_tree")
            with Tab("MARC Export"):
                yield Button("Generate MARC Export", id="generate_marc_button")
        yield Footer()

    def on_mount(self) -> None:
        self.load_data_to_table()
        self.load_data_to_tree()

    def load_data_to_table(self):
        table = self.query_one(DataTable)
        query = f"SELECT * FROM `{self.table_id}`"
        try:
            df = self.client.query(query).to_dataframe()
            table.clear(columns=True)
            table.add_columns(*df.columns)
            table.add_rows(df.to_records(index=False))
        except Exception as e:
            self.log(f"Error loading data from BigQuery: {e}")

    def load_data_to_tree(self):
        tree = self.query_one("#marc_tree", Tree)
        tree.clear()
        query = f"SELECT * FROM `{self.table_id}`"
        try:
            df = self.client.query(query).to_dataframe()
            # Group by title and author to get bibliographic records
            for (title, author), group in df.groupby(["title", "author"]):
                bib_node = tree.root.add(f"{title} - {author}")
                for index, row in group.iterrows():
                    bib_node.add(f"Holding: {row['holding_barcode']}")
        except Exception as e:
            self.log(f"Error loading data from BigQuery: {e}")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "process_books_button":
            self.process_books()
        elif event.button.id == "generate_marc_button":
            self.generate_marc_export()

    def process_books(self):
        # For now, hardcode the input file path
        input_file = "book_list.txt"
        
        book_identifiers = read_input_file(input_file)
        total_books = len(book_identifiers)
        
        progress_bar = self.query_one("#progress_bar", ProgressBar)
        progress_bar.total = total_books
        
        enriched_books = enrich_book_data(book_identifiers)
        
        # This is a simplified progress update
        for i, book in enumerate(enriched_books):
            progress_bar.advance(1)
            # I will add sparkline updates later
        
        insert_books_to_bigquery(enriched_books)
        self.load_data_to_table()
        self.load_data_to_tree()

    def generate_marc_export(self):
        query = f"SELECT * FROM `{self.table_id}`"
        try:
            df = self.client.query(query).to_dataframe()
            marc_records = convert_df_to_marc(df)
            write_marc_file(marc_records, "export.mrc")
            self.log("MARC export generated successfully!")
        except Exception as e:
            self.log(f"Error generating MARC export: {e}")

if __name__ == "__main__":
    app = NewBookImporterTUI()
    app.run()