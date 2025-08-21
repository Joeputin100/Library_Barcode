import argparse
import io
import logging
import os
from unittest.mock import MagicMock

from textual.app import App, ComposeResult
from textual.containers import Center, Container, Horizontal, Vertical
from textual.screen import ModalScreen, Screen
from textual.worker import Worker, WorkerState
from textual.widgets import (
    Button,
    DataTable,
    DirectoryTree,
    Footer,
    Header,
    Label,
    Input,
    ProgressBar,
    Sparkline,
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
                
                yield Label("Cache Performance:")
                with Horizontal():
                    yield Label("Hits: 0 / 0 (0.0%)", id="cache_stats_label")
                    yield Sparkline([], id="cache_sparkline", classes="sparkline_widget")

                yield Label("Data Completeness Score:")
                with Horizontal():
                    yield Label("Avg: 0.0%", id="completeness_stats_label")
                    yield Sparkline([], id="completeness_sparkline", classes="sparkline_widget")

                yield Label("Google Books API Health:")
                with Horizontal():
                    yield Label("Success: 0 / 0 (0.0%)", id="google_api_stats_label")
                    yield Sparkline([], id="google_api_sparkline", classes="sparkline_widget")
                
                yield Label("Library of Congress API Health:")
                with Horizontal():
                    yield Label("Success: 0 / 0 (0.0%)", id="loc_api_stats_label")
                    yield Sparkline([], id="loc_api_sparkline", classes="sparkline_widget")

                yield Label("Vertex AI Batch Status:", id="vertex_status_title", classes="hidden")
                yield Label("Not started", id="vertex_status_label", classes="hidden")
                yield ProgressBar(id="vertex_progress_bar", classes="hidden")

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

    def on_worker_state_changed(self, event: Worker.StateChanged) -> None:
        """Called when the worker state changes."""
        self.app.log(f"Worker {event.worker.name} state changed to {event.worker.state}")
        if event.worker.state == WorkerState.SUCCESS and event.worker.name == "process_books":
            self.query_one("#progress_status_label", Label).update("Processing complete.")
            self.query_one("#vertex_status_title", Label).add_class("hidden")
            self.query_one("#vertex_status_label", Label).add_class("hidden")
            self.query_one("#vertex_progress_bar", ProgressBar).add_class("hidden")
            self.load_data_to_table()
            self.load_data_to_tree()
        elif event.worker.state == WorkerState.ERROR:
            self.app.log(f"Worker error: {event.worker.error}")
            self.query_one("#progress_status_label", Label).update(f"Worker failed: {event.worker.error}")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        """Handle button presses on the main screen."""
        self.app.button_press_count += 1
        self.app._log_message(
            f"DEBUG: MainScreen.on_button_pressed called. Count: {self.app.button_press_count}"
        )
        if event.button.id == "process_books_button":
            if not self.app.selected_input_file:
                self.app.log("Please select an input file first.")
                return
            
            # Reset UI elements
            self.query_one("#progress_status_label", Label).update("Starting...")
            self.query_one("#progress_bar", ProgressBar).update(progress=0, total=100)
            self.query_one("#cache_stats_label", Label).update("Hits: 0 / 0 (0.0%)")
            self.query_one("#cache_sparkline", Sparkline).data = []
            self.query_one("#completeness_stats_label", Label).update("Avg: 0.0%")
            self.query_one("#completeness_sparkline", Sparkline).data = []
            self.query_one("#google_api_stats_label", Label).update("Success: 0 / 0 (0.0%)")
            self.query_one("#google_api_sparkline", Sparkline).data = []
            self.query_one("#loc_api_stats_label", Label).update("Success: 0 / 0 (0.0%)")
            self.query_one("#loc_api_sparkline", Sparkline).data = []
            self.query_one("#vertex_status_title", Label).add_class("hidden")
            self.query_one("#vertex_status_label", Label).update("Not started")
            self.query_one("#vertex_status_label", Label).add_class("hidden")
            self.query_one("#vertex_progress_bar", ProgressBar).add_class("hidden")

            self.run_worker(self.process_books, name="process_books", thread=True)

        elif event.button.id == "generate_marc_button":
            self.generate_marc_export()
        elif event.button.id == "select_file_button":

            def select_file_callback(selected_path: str | None):
                self.app.log(
                    f"DEBUG: select_file_callback called with path: {selected_path}"
                )
                selected_file_label = self.query_one("#selected_file_label", Label)
                if selected_path:
                    self.app._log_message(f"Selected file: {selected_path}")
                    self.app.selected_input_file = selected_path
                    selected_file_label.update(f"Selected: {selected_path}")
                else:
                    self.app._log_message("File selection cancelled.")
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
            self.app._log_message(f"Error loading data from BigQuery: {e}")

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
            self.app._log_message(f"Error loading data from BigQuery: {e}")

    async def process_books(self):
        """Process the selected input file in a worker."""
        self.app.log("Starting book processing worker...")
        from book_importer import (
            enrich_book_data,
            enrich_with_vertex_ai,
            insert_books_to_bigquery,
            read_input_file,
        )

        book_identifiers = read_input_file(self.app.selected_input_file)
        total_books = len(book_identifiers)
        self.app.log(f"Found {total_books} books to process.")
        
        # UI Widgets
        progress_bar = self.query_one("#progress_bar", ProgressBar)
        status_label = self.query_one("#progress_status_label", Label)
        cache_stats_label = self.query_one("#cache_stats_label", Label)
        cache_sparkline = self.query_one("#cache_sparkline", Sparkline)
        completeness_stats_label = self.query_one("#completeness_stats_label", Label)
        completeness_sparkline = self.query_one("#completeness_sparkline", Sparkline)
        google_api_stats_label = self.query_one("#google_api_stats_label", Label)
        google_api_sparkline = self.query_one("#google_api_sparkline", Sparkline)
        loc_api_stats_label = self.query_one("#loc_api_stats_label", Label)
        loc_api_sparkline = self.query_one("#loc_api_sparkline", Sparkline)
        vertex_status_title = self.query_one("#vertex_status_title", Label)
        vertex_status_label = self.query_one("#vertex_status_label", Label)
        vertex_progress_bar = self.query_one("#vertex_progress_bar", ProgressBar)

        # Metric trackers
        cache_hits = 0
        google_successes = 0
        loc_successes = 0
        completeness_scores = []
        cache_performance_data = []
        google_api_success_data = []
        loc_api_success_data = []

        self.app.call_from_thread(progress_bar.update, total=total_books)

        enriched_books = []
        self.app.log("Starting per-book enrichment...")
        for i, (book_data, metrics) in enumerate(enrich_book_data(book_identifiers, self.app.cache), start=1):
            self.app.log(f"WORKER: Processing book {i}, Metrics: {metrics}")
            enriched_books.append(book_data)

            # Update metrics
            if metrics.get("google_cached") or metrics.get("loc_cached"):
                cache_hits += 1
                cache_performance_data.append(1)
            else:
                cache_performance_data.append(0)
            
            if metrics.get("google_success", False):
                google_successes += 1
                google_api_success_data.append(1)
            else:
                google_api_success_data.append(0)

            if metrics.get("loc_success", False):
                loc_successes += 1
                loc_api_success_data.append(1)
            else:
                loc_api_success_data.append(0)

            completeness_scores.append(metrics["completeness_score"])
            avg_completeness = (sum(completeness_scores) / len(completeness_scores)) * 100

            # Update UI from thread
            self.app.call_from_thread(status_label.update, f"Processing {i} of {total_books}...")
            self.app.call_from_thread(progress_bar.advance, 1)
            
            self.app.call_from_thread(cache_stats_label.update, f"Hits: {cache_hits} / {i} ({cache_hits/i:.1%})")
            cache_sparkline.data = cache_performance_data
            self.app.call_from_thread(cache_sparkline.refresh)

            self.app.call_from_thread(completeness_stats_label.update, f"Avg: {avg_completeness:.1f}%")
            completeness_sparkline.data = completeness_scores
            self.app.call_from_thread(completeness_sparkline.refresh)

            self.app.call_from_thread(google_api_stats_label.update, f"Success: {google_successes} / {i} ({google_successes/i:.1%})")
            google_api_sparkline.data = google_api_success_data
            self.app.call_from_thread(google_api_sparkline.refresh)

            self.app.call_from_thread(loc_api_stats_label.update, f"Success: {loc_successes} / {i} ({loc_successes/i:.1%})")
            loc_api_sparkline.data = loc_api_success_data
            self.app.call_from_thread(loc_api_sparkline.refresh)

        self.app.log("Finished per-book enrichment.")

        # Vertex AI Processing
        self.app.log("Starting Vertex AI processing...")
        self.app.call_from_thread(vertex_status_title.remove_class, "hidden")
        self.app.call_from_thread(vertex_status_label.remove_class, "hidden")
        self.app.call_from_thread(vertex_progress_bar.remove_class, "hidden")
        self.app.call_from_thread(vertex_status_label.update, "Processing with Vertex AI...")
        
        vertex_processed_count = 0
        final_books = []
        for processed_count, books in enrich_with_vertex_ai(enriched_books, self.app.cache):
            final_books = books
            vertex_processed_count += processed_count
            self.app.call_from_thread(vertex_progress_bar.update, total=len(enriched_books), advance=vertex_processed_count)
            self.app.refresh()

        self.app.call_from_thread(vertex_status_label.update, "Vertex AI processing complete.")
        self.app.log("Finished Vertex AI processing.")

        self.app.log("Inserting books into BigQuery...")
        insert_books_to_bigquery(final_books, self.app.client)
        self.app.log("Finished inserting books.")

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
            self.app._log_message(f"Error generating MARC export: {e}")


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
        self.button_press_count = 0
        self.cache = {}

    def on_mount(self) -> None:
        self.push_screen(SplashScreen())
        self.cache = load_cache()

    def _log_message(self, message: str) -> None:
        tui_logger.debug(message)

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
