import pytest
import asyncio
from unittest.mock import MagicMock, patch
from textual.pilot import Pilot
from new_book_importer_tui import NewBookImporterTUI, MainScreen
import os

# Create a dummy input file for testing
TEST_INPUT_FILE = "test_input_file.txt"
with open(TEST_INPUT_FILE, "w") as f:
    f.write("978-0321765723\n") # The C++ Programming Language
    f.write("978-0134494166\n") # Effective Modern C++

@pytest.fixture
def mock_api_calls():
    # This single fixture will mock all API calls
    with patch("api_calls.get_book_metadata_initial_pass") as mock_initial_pass, \
         patch("book_importer.enrich_with_vertex_ai") as mock_vertex:
        
        # Configure the side effect for the initial pass (Google/LOC)
        mock_initial_pass.side_effect = [
            # Return value for the first book
            ({}, False, True, True, False), # metadata, google_cached, loc_cached, google_success, loc_success
            # Return value for the second book
            ({}, True, False, True, True), # metadata, google_cached, loc_cached, google_success, loc_success
        ]
        
        # Configure the mock for vertex AI
        def mock_vertex_func(books, cache):
            return books # Just return the books as is
        mock_vertex.side_effect = mock_vertex_func
        
        yield mock_initial_pass, mock_vertex

async def wait_for_condition(pilot, condition, timeout=5):
    """Waits for a condition to be true, with a timeout."""
    for _ in range(int(timeout / 0.1)):
        if condition():
            return True
        await pilot.pause(0.1)
    return False

@pytest.mark.asyncio
async def test_full_process_flow_synchronous(mock_api_calls):
    """Test the full data processing flow in a synchronous manner."""
    app = NewBookImporterTUI(dev_mode=True)

    async with app.run_test() as pilot:
        # 1. Wait for the main screen to be ready
        await wait_for_condition(pilot, lambda: isinstance(app.screen, MainScreen))
        
        # 2. Simulate file selection
        app.selected_input_file = TEST_INPUT_FILE
        app.screen.query_one("#selected_file_label").update(f"Selected: {TEST_INPUT_FILE}")
        await pilot.pause(0.1)

        # 3. Press the "Process Books" button
        app.screen.process_books()
        await pilot.pause(0.1) # Allow UI to refresh

        # 5. Assertions: Check the final state of the UI
        final_status = app.screen.query_one("#progress_status_label").renderable
        assert "Processing complete" in str(final_status)

        progress_bar = app.screen.query_one("#progress_bar")
        assert progress_bar.progress == 2
        assert progress_bar.total == 2

    # Cleanup the dummy file
    os.remove(TEST_INPUT_FILE)