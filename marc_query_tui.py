import datetime
from textual.app import (
    App,
    ComposeResult,
    events,
)
from textual.widgets import (
    Header,
    Footer,
    Input,
    RichLog,
    Markdown,
    LoadingIndicator,
)
from textual.containers import Vertical
from textual.screen import ModalScreen

from query_parser import parse_query
from marc_processor import (
    load_marc_records,
    filter_marc_records,
    get_field_value,
)


class HelpScreen(ModalScreen):
    """A modal screen to display help information."""

    def compose(self) -> ComposeResult:
        yield Vertical(
            Markdown(
                """
# MARC Query TUI Help

## Query Syntax

The query parser understands the following nouns and operators:

### Nouns

*   `barcode`: Search for a specific barcode.
*   `author`: Search for an author.
*   `title`: Search for a title.
*   `series`: Search for a series.
*   `isbn`: Search for an ISBN.
*   `call number`: Search for a call number.
*   `holding barcode`: Search for a holding barcode.

### Operators

*   `:`: Used to separate a field from its value (e.g., `author: brandon sanderson`).
*   `-`: Used to specify a range of barcodes (e.g., `b100-b200`).
*   `and`: Used to combine multiple queries (e.g., `author: brandon sanderson and series: stormlight archive`).
*   `starts with`: Used to search for barcodes that start with a specific prefix (e.g., `barcodes starting with B`).

### Examples

*   `barcode 12345`
*   `barcodes from b100 to b200`
*   `barcodes starting with B`
*   `author: brandon sanderson`
*   `title: the way of kings`
*   `series: stormlight archive`
*   `isbn: 9780765326355`
*   `call number: F SAN`
*   `holding barcode: 31234567890123`
*   `author: brandon sanderson and series: stormlight archive`
            """
            ),
            id="help-container",
        )

    def on_key(self, event: "events.Key") -> None:
        if event.key == "escape":
            self.app.pop_screen()


def generate_natural_language_query(parsed_query):
    if not parsed_query:
        return "An empty query."

    # Handle combined queries first
    if "queries" in parsed_query and "operator" in parsed_query:

        sub_queries = [
            generate_natural_language_query(q).replace("a search for ", "")
            for q in parsed_query["queries"]
        ]
        return f"a search for records that match all of the following: {', '.join(sub_queries)}"

    # Handle single queries
    query_type = parsed_query.get("type")
    if query_type == "barcode_range":
        return f"a search for barcodes between {parsed_query.get('start')} and {parsed_query.get('end')}"
    elif query_type == "barcode_prefix":
        return f"a search for barcodes starting with '{parsed_query.get('prefix')}'"
    elif query_type == "field_query":
        field = parsed_query.get("field")
        value = parsed_query.get("value")
        return f"a search for {field} containing '{value}'"
    elif query_type == "barcode":
        return f"a search for barcode '{parsed_query.get('value')}'"
    else:
        return "an unknown query type."


class MarcQueryTUI(App):
    """A Textual app to query MARC records."""

    CSS_PATH = (
        "project_viewer.css"  # Reusing the CSS for now, might rename later
    )

    BINDINGS = [
        ("q", "quit", "Quit"),
        ("ctrl+h", "show_help", "Help"),
    ]

    def action_show_help(self):
        """Show the help screen."""
        self.push_screen(HelpScreen())

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.all_marc_records = []
        self.load_initial_data()
        self.current_parsed_query = None
        self.waiting_for_confirmation = False
        self.log_file = open("marc_query_tui_output.log", "a")  # Open log file
        self._log_message("App initialized.")  # Log app initialization

    def _log_message(self, message):
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.log_file.write(f"[{timestamp}] {message}\n")

    def load_initial_data(self):
        """Load all MARC records when the app starts."""
        self.all_marc_records = load_marc_records("cimb_bibliographic.marc")

    def compose(self) -> ComposeResult:
        """Create child widgets for the app."""
        yield Header()
        with Vertical():
            yield Input(placeholder="Enter your query", id="query-input")
            yield LoadingIndicator(
                id="loading-indicator", classes="hidden"
            )  # Add LoadingIndicator, initially hidden
            yield RichLog(id="results", wrap=True)
        yield Footer()

    def on_input_submitted(self, event: Input.Submitted):
        """Handle input submission."""
        results_widget = self.query_one("#results", RichLog)
        loading_indicator = self.query_one(
            "#loading-indicator", LoadingIndicator
        )

        self._log_message(
            f"DEBUG: waiting_for_confirmation = {self.waiting_for_confirmation}"
        )
        results_widget.write(
            f"DEBUG: waiting_for_confirmation = {self.waiting_for_confirmation}"
        )

        query_str = event.value.strip()
        query_input = self.query_one("#query-input", Input)

        self._log_message(f"DEBUG: Input value (repr): {repr(query_str)}")
        self._log_message(
            f"DEBUG: query_str.lower() == 'y': {query_str.lower() == 'y'}"
        )
        self._log_message(
            f"DEBUG: query_str.lower() == 'n': {query_str.lower() == 'n'}"
        )

        if self.waiting_for_confirmation:
            if query_str.lower() == "y":
                self.waiting_for_confirmation = False
                if self.current_parsed_query:
                    self._log_message(
                        f"Executing query: {self.current_parsed_query}"
                    )
                    self.execute_query(self.current_parsed_query)
                else:
                    self._log_message("Error: No query to execute.")
                    results_widget.write("Error: No query to execute.")
            elif query_str.lower() == "n":
                self.waiting_for_confirmation = False
                self.current_parsed_query = None
                self._log_message("Query cancelled. Please enter a new query.")
                results_widget.write(
                    "Query cancelled. Please enter a new query."
                )
            else:
                self._log_message(
                    "Invalid input. Type 'y' to confirm or 'n' to cancel."
                )
                results_widget.write(
                    "Invalid input. Type 'y' to confirm or 'n' to cancel."
                )
            query_input.value = ""  # Clear input after confirmation attempt
            query_input.blur()  # Unfocus the input
        else:
            loading_indicator.remove_class("hidden")  # Show loading indicator
            parsed_query = parse_query(query_str)
            loading_indicator.add_class("hidden")  # Hide loading indicator

            self._log_message(f"Parsed query: {parsed_query}")
            self._log_message(
                f"parsed_query.get('error'): {parsed_query.get('error') if parsed_query else 'N/A'}"
            )
            if parsed_query and not parsed_query.get("error"):
                self.current_parsed_query = parsed_query
                self.waiting_for_confirmation = True

                natural_language_query = generate_natural_language_query(
                    parsed_query
                )
                confirmation_message = f"You entered: '{query_str}'\n"
                confirmation_message += (
                    f"I understood this as: {natural_language_query}\n"
                )
                confirmation_message += (
                    "Is this correct? (Type 'y' to confirm, or 'n' to cancel)"
                )
                self._log_message(confirmation_message)
                results_widget.write(confirmation_message)
                query_input.value = (
                    ""  # Clear input after displaying confirmation
                )
                query_input.blur()  # Unfocus the input
            else:
                self.current_parsed_query = None
                error_message = (
                    parsed_query.get(
                        "error",
                        "Could not parse query. Try 'barcode 123', 'author: John Doe', etc.",
                    )
                    if parsed_query
                    else "Could not parse query. Try 'barcode 123', 'author: John Doe', etc."
                )
                self._log_message(error_message)
                results_widget.write(error_message)
                query_input.value = ""  # Clear input after invalid query
                query_input.blur()  # Unfocus the input

    def execute_query(self, parsed_query):
        """Executes the parsed query and displays results."""
        results_widget = self.query_one("#results", RichLog)
        results_widget.clear()
        self._log_message("Executing query and clearing results.")

        filtered_records = filter_marc_records(
            self.all_marc_records, parsed_query
        )

        self._log_message(f"Found {len(filtered_records)} records.\n\n")
        results_widget.write(f"Found {len(filtered_records)} records.\n\n")
        if filtered_records:
            self._log_message("Full list of records:\n")
            results_widget.write("Full list of records:\n")
            for i, record in enumerate(filtered_records):
                barcode = get_field_value(record, "holding barcode")
                title = get_field_value(record, "title")
                message = f"  {i + 1}. Barcode: {barcode if barcode else 'N/A'}, Title: {title if title else 'N/A'}\n"
                self._log_message(message)
                results_widget.write(message)

    def action_quit(self):
        """Quit the application."""
        self.bell()
        self._log_message("action_quit called")
        self.log_file.close()  # Close the log file on exit
        self.exit()


if __name__ == "__main__":
    app = MarcQueryTUI()
    app.run()
