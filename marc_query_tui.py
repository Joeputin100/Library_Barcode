import json
from textual.app import App, ComposeResult
from textual.widgets import Header, Footer, Input, Static, RichLog
from textual.containers import Vertical, Container

from query_parser import parse_query
from marc_processor import load_marc_records, filter_marc_records, get_field_value


class MarcQueryTUI(App):
    """A Textual app to query MARC records."""

    CSS_PATH = "project_viewer.css" # Reusing the CSS for now, might rename later

    BINDINGS = [
        ("q", "quit", "Quit"),
    ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.all_marc_records = []
        self.load_initial_data()
        self.current_parsed_query = None
        self.waiting_for_confirmation = False

    def load_initial_data(self):
        """Load all MARC records when the app starts."""
        self.all_marc_records = load_marc_records('cimb.marc')
        self.log(f"Loaded {len(self.all_marc_records)} MARC records.")

    def compose(self) -> ComposeResult:
        """Create child widgets for the app."""
        yield Header()
        with Vertical():
            yield Input(placeholder="Enter your query", id="query-input")
            yield RichLog(id="results")
        yield Footer()

    def on_input_submitted(self, event: Input.Submitted):
        """Handle input submission."""
        query_str = event.value.strip()
        results_widget = self.query_one("#results", RichLog)
        query_input = self.query_one("#query-input", Input)

        if self.waiting_for_confirmation:
            if query_str.lower() == 'y':
                self.waiting_for_confirmation = False
                if self.current_parsed_query:
                    self.execute_query(self.current_parsed_query)
                else:
                    results_widget.write("Error: No query to execute.")
            elif query_str.lower() == 'n':
                self.waiting_for_confirmation = False
                self.current_parsed_query = None
                results_widget.write("Query cancelled. Please enter a new query.")
            else:
                results_widget.write("Invalid input. Type 'y' to confirm or 'n' to cancel.")
            query_input.value = "" # Clear input after confirmation attempt
        else:
            parsed_query = parse_query(query_str)
            if parsed_query:
                self.current_parsed_query = parsed_query
                self.waiting_for_confirmation = True

                confirmation_message = f"You entered: '{query_str}'\n"
                confirmation_message += f"I understood this as: {parsed_query}\n"
                confirmation_message += "Is this correct? (Type 'y' to confirm, or 'n' to cancel)"
                results_widget.write(confirmation_message)
                query_input.value = "" # Clear input after displaying confirmation
            else:
                self.current_parsed_query = None
                results_widget.write("Could not parse query. Try 'barcode 123', 'author: John Doe', etc.")
                query_input.value = "" # Clear input after invalid query

    def execute_query(self, parsed_query):
        """Executes the parsed query and displays results."""
        results_widget = self.query_one("#results", RichLog)
        results_widget.clear()

        filtered_records = filter_marc_records(self.all_marc_records, parsed_query)

        results_widget.write(f"Found {len(filtered_records)} records.\n\n")
        if filtered_records:
            results_widget.write("Full list of records:\n")
            for i, record in enumerate(filtered_records):
                barcode = get_field_value(record, 'holding barcode')
                title = get_field_value(record, 'title')
                results_widget.write(f"  {i + 1}. Barcode: {barcode if barcode else 'N/A'}, Title: {title if title else 'N/A'}\n")

    def action_quit(self):
        """Quit the application."""
        self.exit()


if __name__ == "__main__":
    app = MarcQueryTUI()
    app.run()
