import argparse
import io
import logging
import os
from unittest.mock import MagicMock

from textual.app import App, ComposeResult
from textual.containers import Center, Container, Horizontal, Vertical
from textual.screen import ModalScreen, Screen
from textual.widgets import (
    Button,
    DataTable,
    DirectoryTree,
    Footer,
    Header,
    Label,
    Input,
    ProgressBar,
    Tab,
    Tabs,
    Tree,
)
from caching import load_cache, save_cache

# --- Logging Setup ---
tui_log_capture_string = io.StringIO()
tui_logger = logging.getLogger(__name__)
tui_logger.setLevel(logging.DEBUG)
tui_handler = logging.StreamHandler(tui_log_capture_string)
tui_handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
if not tui_logger.handlers:
    tui_logger.addHandler(tui_handler)


# --- Screens ---


class SplashScreen(Screen):
    """A splash screen with a loading indicator."""

    def compose(self) -> ComposeResult:
        yield Center(
            Vertical(
                Label("Loading application..."),
                id="splash-vertical-container",
            )
        )

    def on_mount(self) -> None:
        self.run_worker(self.app.initialize_client)


class FileSelectionScreen(ModalScreen):
    """Modal screen for file selection."""

    def compose(self) -> ComposeResult:
        yield Vertical(
            DirectoryTree("./", id="file_tree"),
            Horizontal(
                Button("Select", id="select_file_button"),
                Button("Cancel", id="cancel_file_button"),
            ),
        )

    def on_button_pressed(self, event: Button.Pressed) -> None:
        """Handle button presses in the modal dialog."""
        if event.button.id == "select_file_button":
            tree = self.query_one("#file_tree", DirectoryTree)
            if tree.cursor_node:
                path = tree.cursor_node.data.path
                if not os.path.isdir(path):
                    self.dismiss(path)
                else:
                    self.dismiss(None)  # It's a directory, dismiss
            else:
                self.dismiss(None)  # Nothing selected
            event.stop()
        elif event.button.id == "cancel_file_button":
            self.dismiss(None)
            event.stop()


class MainScreen(Screen):
    """The main screen of the application."""

    def compose(self) -> ComposeResult:
        """Create child widgets for the app."""
        yield Header()
        yield Tabs(
            Tab("Input & Processing", id="input_processing"),
            Tab("Review & Edit", id="review_edit"),
            Tab("MARC Records", id="marc_records"),
            Tab("MARC Export", id="marc_export"),
            id="tabs",
        )
        with Container(id="content-container"):
            with Vertical(id="input-processing-content"):
                yield Label(
                    "1. Select a file containing a list of ISBNs or Title-Author pairs."
                )
                yield Button("Select Input File", id="select_file_button")
                yield Label("No file selected", id="selected_file_label")
                yield Label("2. Process the books to enrich the data.")
                yield Button("Process Books", id="process_books_button")
                yield Label(id="progress_status_label")
                yield ProgressBar(id="progress_bar")

            yield DataTable(id="data_table", classes="hidden")
            yield Tree("MARC Records", id="marc_tree", classes="hidden")
            yield Button(
                "Generate MARC Export",
                id="generate_marc_button",
                classes="hidden",
            )
        yield Footer()

    def on_mount(self) -> None:
        if self.app.dev_mode:
            self.load_data_to_table()
            self.load_data_to_tree()

    def on_tabs_tab_activated(self, event: Tabs.TabActivated) -> None:
        """Handle tab changes."""
        self.query_one("#input-processing-content").display = False
        self.query_one("#data_table").display = False
        self.query_one("#marc_tree").display = False
        self.query_one("#generate_marc_button").display = False

        if event.tab.id == "input_processing":
            self.query_one("#input-processing-content").display = True
        elif event.tab.id == "review_edit":
            self.query_one("#data_table").display = True
        elif event.tab.id == "marc_records":
            self.query_one("#marc_tree").display = True
        elif event.tab.id == "marc_export":
            self.query_one("#generate_marc_button").display = True

    def on_button_pressed(self, event: Button.Pressed) -> None:
        """Handle button presses on the main screen."""
        if event.button.id == "process_books_button":
            if not self.app.selected_input_file:
                self.app.log("Please select an input file first.")
                return
            
            self.process_books()

        elif event.button.id == "generate_marc_button":
            self.generate_marc_export()
        elif event.button.id == "select_file_button":

            def select_file_callback(selected_path: str | None):
                self.app.log(
                    f"DEBUG: select_file_callback called with path: {selected_path}"
                )
                selected_file_label = self.query_one("#selected_file_label", Label)
                if selected_path:
                    self.app.selected_input_file = selected_path
                    selected_file_label.update(f"Selected: {selected_path}")
                else:
                    selected_file_label.update("No file selected")
                self.app.log("DEBUG: select_file_callback finished.")

            self.app.push_screen(FileSelectionScreen(), select_file_callback)

    def load_data_to_table(self):
        """Load data from BigQuery into the DataTable."""
        table = self.query_one(DataTable)
        query = f"SELECT * FROM `{self.app.table_id}`"
        try:
            df = self.app.client.query(query).to_dataframe()
            table.clear(columns=True)
            table.add_columns(*df.columns)
            table.add_rows(df.to_records(index=False))
        except Exception as e:
            self.app.log(f"Error loading data from BigQuery: {e}")

    def load_data_to_tree(self):
        """Load data from BigQuery into the Tree."""
        tree = self.query_one("#marc_tree", Tree)
        tree.clear()
        query = f"SELECT * FROM `{self.app.table_id}`"
        try:
            df = self.app.client.query(query).to_dataframe()
            if not df.empty:
                for (title, author), group in df.groupby(["title", "author"]):
                    bib_node = tree.root.add(f"{title} - {author}")
                    for index, row in group.iterrows():
                        bib_node.add(f"Holding: {row['holding_barcode']}")
        except Exception as e:
            self.app.log(f"Error loading data from BigQuery: {e}")

    def process_books(self):
        """Process the selected input file synchronously."""
        from book_importer import (
            enrich_book_data,
            enrich_with_vertex_ai,
            insert_books_to_bigquery,
            read_input_file,
        )

        book_identifiers = read_input_file(self.app.selected_input_file)
        total_books = len(book_identifiers)
        
        status_label = self.query_one("#progress_status_label", Label)
        progress_bar = self.query_one("#progress_bar", ProgressBar)

        status_label.update("Starting...")
        progress_bar.update(progress=0, total=total_books)

        enriched_books = []
        for i, (book_data, metrics) in enumerate(enrich_book_data(book_identifiers, self.app.cache), start=1):
            enriched_books.append(book_data)
            status_label.update(f"Processing {i} of {total_books}...")
            progress_bar.advance(1)

        status_label.update("Processing with Vertex AI...")
        final_books = enrich_with_vertex_ai(enriched_books, self.app.cache)

        status_label.update("Inserting books into BigQuery...")
        insert_books_to_bigquery(final_books, self.app.client)
        
        self.load_data_to_table()
        self.load_data_to_tree()
        status_label.update("Processing complete.")

    def generate_marc_export(self):
        """Generate a MARC export file from the data in BigQuery."""
        from marc_exporter import convert_df_to_marc, write_marc_file

        query = f"SELECT * FROM `{self.app.table_id}`"
        try:
            df = self.app.client.query(query).to_dataframe()
            marc_records = convert_df_to_marc(df)
            write_marc_file(marc_records, "export.mrc")
            self.app.log("MARC export generated successfully!")
        except Exception as e:
            self.app.log(f"Error generating MARC export: {e}")


# --- App Class ---


class NewBookImporterTUI(App):
    """A Textual app to import new books."""

    CSS_PATH = "project_viewer.css"
    BINDINGS = [("ctrl+q", "quit", "Quit")]

    def __init__(self, dev_mode=False, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dev_mode = dev_mode
        self.client = None
        self.table_id = "barcode.new_books"
        self.selected_input_file = None
        self.cache = {}

    def on_mount(self) -> None:
        self.push_screen(SplashScreen())
        self.cache = load_cache()

    async def initialize_client(self) -> None:
        """Initialize the BigQuery client."""
        if self.dev_mode:
            import pandas as pd

            self.client = MagicMock()
            mock_query_job = MagicMock()
            mock_df = pd.DataFrame(
                {
                    "title": ["Book 1 (Mock)", "Book 2 (Mock)"],
                    "author": ["Author 1 (Mock)", "Author 2 (Mock)"],
                    "holding_barcode": ["123", "456"],
                }
            )
            mock_query_job.to_dataframe.return_value = mock_df
            self.client.query.return_value = mock_query_job
        else:
            from google.cloud import bigquery

            self.client = bigquery.Client()

        self.push_screen(MainScreen())

    def action_quit(self) -> None:
        save_cache(self.cache)
        with open("new_book_importer_tui.log", "w") as f:
            f.write(tui_log_capture_string.getvalue())
        self.exit()


if __name__ == "__main__":
    print("\033]0;New Book Importer\a", end="", flush=True)
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dev",
        action="store_true",
        help="Run in development mode with mocked BigQuery client.",
    )
    args = parser.parse_args()

    app = NewBookImporterTUI(dev_mode=args.dev)
    app.run()
