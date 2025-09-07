import argparse
import io
import logging
import os
import re
import time
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
    Tab,
    Tabs,
    Tree,
)
from caching import load_cache, save_cache

# --- Logging Setup ---
log_file = "new_book_importer_tui.log"
# Clear the log file at the start of the script
with open(log_file, "w") as f:
    f.write("")
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename=log_file,
    filemode="a",
)
tui_logger = logging.getLogger(__name__)


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

class EditScreen(ModalScreen):
    """A modal screen for editing a row of data."""

    def __init__(self, row_data, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.row_data = row_data

    def compose(self) -> ComposeResult:
        """Create child widgets for the modal dialog."""
        with Vertical():
            for key, value in self.row_data.items():
                yield Label(key)
                yield Input(value=str(value), id=f"edit_{key}")
            with Horizontal():
                yield Button("Save", id="save_button")
                yield Button("Cancel", id="cancel_button")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        """Handle button presses in the modal dialog."""
        if event.button.id == "save_button":
            new_data = {}
            for key in self.row_data.keys():
                new_data[key] = self.query_one(f"#edit_{key}", Input).value
            self.dismiss(new_data)
        elif event.button.id == "cancel_button":
            self.dismiss(None)

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

            yield DataTable(id="data_table", classes="hidden")
            yield Button("Refresh", id="refresh_button", classes="hidden")
            yield Button("View BigQuery Data", id="view_bigquery_button", classes="hidden")
            yield Tree("MARC Records", id="marc_tree", classes="hidden")
            yield Button(
                "Generate MARC Export",
                id="generate_marc_button",
                classes="hidden",
            )
        yield Footer()

    def on_mount(self) -> None:
        pass

    def on_tabs_tab_activated(self, event: Tabs.TabActivated) -> None:
        """Handle tab changes."""
        self.query_one("#input-processing-content").display = False
        self.query_one("#data_table").display = False
        self.query_one("#refresh_button").display = False
        self.query_one("#view_bigquery_button").display = False
        self.query_one("#marc_tree").display = False
        self.query_one("#generate_marc_button").display = False

        if event.tab.id == "input_processing":
            self.query_one("#input-processing-content").display = True
        elif event.tab.id == "review_edit":
            self.query_one("#data_table").display = True
            self.query_one("#refresh_button").display = True
            self.query_one("#view_bigquery_button").display = True
            self.load_data_to_table()
        elif event.tab.id == "marc_records":
            self.query_one("#marc_tree").display = True
            self.load_data_to_tree()
        elif event.tab.id == "marc_export":
            self.query_one("#generate_marc_button").display = True

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        """Handle row selection in the DataTable."""
        # Store the selected row and timestamp for double-tap detection
        current_time = time.time()
        if (hasattr(self, '_last_row_click') and 
            self._last_row_click['row_key'] == event.row_key and
            current_time - self._last_row_click['time'] < 0.5):  # 500ms for double-tap
            # Double-tap detected - open edit modal
            table = self.query_one(DataTable)
            row_data = table.get_row(event.row_key)
            self.app.push_screen(EditScreen(row_data), self.update_row)
            # Reset the click tracker
            self._last_row_click = None
        else:
            # Single click - store for potential double-tap
            self._last_row_click = {'row_key': event.row_key, 'time': current_time}

    def on_button_pressed(self, event: Button.Pressed) -> None:
        """Handle button presses on the main screen."""
        if event.button.id == "process_books_button":
            if not self.app.selected_input_file:
                self.app.log("Please select an input file first.")
                return

            button = self.query_one("#process_books_button", Button)
            button.disabled = True

            def reenable_button():
                button.disabled = False
                status_label = self.query_one("#progress_status_label", Label)
                self.app.call_from_thread(status_label.update, "Processing complete.")

            self.app.log("Starting process_books worker...")
            try:
                self.run_worker(self.process_books, thread=True, callback=reenable_button)
            except Exception as e:
                self.app.log(f"Error starting worker: {e}")

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
        elif event.button.id == "refresh_button":
            self.load_data_to_table()

    def update_row(self, new_data):
        """Updates a row in the DataTable and BigQuery table."""
        if new_data:
            table = self.query_one(DataTable)
            table.update_row(new_data["input_identifier"], **new_data)

            # Update the BigQuery table
            errors = self.app.client.update_rows(self.app.table_id, [new_data], ["input_identifier"])
            if not errors:
                self.app.log("Row updated successfully in BigQuery.")
            else:
                self.app.log(f"Error updating row in BigQuery: {errors}")

    def load_data_to_table(self):
        """Load data from BigQuery into the DataTable."""
        tui_logger.info("Loading data to table...")
        table = self.query_one(DataTable)
        query = f"SELECT * FROM `{self.app.table_id}`"
        try:
            query_job = self.app.client.query(query)
            rows = query_job.result()
            table.clear(columns=True)
            if rows.total_rows > 0:
                schema = [field.name for field in rows.schema]
                table.add_columns(*schema)
                for row in rows:
                    table.add_row(*row.values(), key=row[0])
            tui_logger.info("Data loaded to table successfully.")
        except Exception as e:
            self.app.log(f"Error loading data from BigQuery: {e}")
            tui_logger.error(f"Error loading data from BigQuery: {e}", exc_info=True)

    def load_data_to_tree(self):
        """Load data from BigQuery into the Tree."""
        tree = self.query_one("#marc_tree", Tree)
        tree.clear()
        query = f"SELECT * FROM `{self.app.table_id}`"
        try:
            query_job = self.app.client.query(query)
            rows = query_job.result()
            if rows.total_rows > 0:
                for row in rows:
                    # Assuming 'title' and 'author' are the first two columns
                    title = row[0]
                    author = row[1]
                    bib_node = tree.root.add(f"{title} - {author}")
                    # Assuming 'holding_barcode' is the third column
                    bib_node.add(f"Holding: {row[2]}")
        except Exception as e:
            self.app.log(f"Error loading data from BigQuery: {e}")

    def process_books(self):
        """The synchronous part of processing books."""
        status_label = self.query_one("#progress_status_label", Label)
        self.app.call_from_thread(status_label.update, "Starting...")

        tui_logger.info("process_books started.")
        from book_importer import (
            enrich_book_data,
            enrich_with_vertex_ai,
            insert_books_to_bigquery,
            read_input_file,
        )
        tui_logger.info("book_importer modules imported.")

        tui_logger.info(f"Reading input file: {self.app.selected_input_file}")
        book_identifiers = read_input_file(self.app.selected_input_file)
        tui_logger.info(f"{len(book_identifiers)} book identifiers read.")
        total_books = len(book_identifiers)

        self.app.call_from_thread(status_label.update, "Enriching books...")
        tui_logger.info("Starting book enrichment loop.")
        enriched_books = []
        for i, (book_data, metrics) in enumerate(enrich_book_data(book_identifiers, self.app.cache), start=1):
            enriched_books.append(book_data)
            if i % 10 == 0: # Update every 10 books to avoid too many UI updates
                self.app.call_from_thread(status_label.update, f"Processing {i} of {total_books}...")
        tui_logger.info("Book enrichment loop finished.")

        self.app.call_from_thread(status_label.update, "Enriching with Vertex AI...")
        tui_logger.info("Enriching with Vertex AI.")
        for classification, cached in enrich_with_vertex_ai(enriched_books, self.app.cache):
            for i, book in enumerate(enriched_books):
                if book['title'] == classification['title'] and book['author'] == classification['author']:
                    if not book.get("call_number") and classification.get("classification"):
                        enriched_books[i]["call_number"] = classification["classification"]
                    if not book.get("series_title") and classification.get("series_title"):
                        enriched_books[i]["series_title"] = classification["series_title"]
                    if not book.get("volume_number") and classification.get("volume_number"):
                        enriched_books[i]["volume_number"] = classification["volume_number"]
                    if not book.get("copyright_year") and classification.get("copyright_year"):
                        enriched_books[i]["copyright_year"] = classification["copyright_year"]
        tui_logger.info("Enrichment with Vertex AI finished.")

        final_books = enriched_books

        tui_logger.info(f"Final books: {final_books}")
        self.app.call_from_thread(status_label.update, "Inserting books into BigQuery...")
        tui_logger.info("Inserting books into BigQuery.")
        insert_books_to_bigquery(final_books, self.app.client)

        self.app.call_from_thread(status_label.update, "Loading data to UI...")
        tui_logger.info("Loading data into table and tree.")
        self.app.call_from_thread(self.load_data_to_table)
        self.app.call_from_thread(self.load_data_to_tree)
        tui_logger.info("Finished loading data into table and tree.")

        self.app.call_from_thread(status_label.update, "Processing complete.")
        tui_logger.info("process_books finished.")

    

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
        tui_logger.info("Initializing BigQuery client...")
        if self.dev_mode:
            import pandas as pd
            tui_logger.info("Running in dev mode.")

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
            tui_logger.info("Mock BigQuery client created.")
        else:
            tui_logger.info("Running in normal mode.")
            from google.cloud import bigquery
            
            try:
                tui_logger.info("Attempting to create BigQuery client...")
                self.client = bigquery.Client(project='static-webbing-461904-c4')
                tui_logger.info("BigQuery client created successfully.")
            except Exception as e:
                tui_logger.error(f"Error creating BigQuery client: {e}", exc_info=True)
                self.exit()

        tui_logger.info("Pushing MainScreen...")
        self.push_screen(MainScreen())
        tui_logger.info("MainScreen pushed.")

    def action_quit(self) -> None:
        save_cache(self.cache)
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