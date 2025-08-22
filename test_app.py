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
         patch("api_calls.get_vertex_ai_classification_batch") as mock_vertex:
        
        # Configure the side effect for the initial pass (Google/LOC)
        mock_initial_pass.side_effect = [
            # Return value for the first book
            ({}, False, True, True, False), # metadata, google_cached, loc_cached, google_success, loc_success
            # Return value for the second book
            ({}, True, False, True, True), # metadata, google_cached, loc_cached, google_success, loc_success
        ]
        
        # Configure the mock for vertex AI
        mock_vertex.return_value = ([], False) # classifications, cached
        
        yield mock_initial_pass, mock_vertex

@pytest.mark.asyncio
async def test_full_process_flow_synchronous(mock_api_calls):
    """Test the full data processing flow in a synchronous manner."""
    app = NewBookImporterTUI(dev_mode=True)

    async with app.run_test() as pilot:
        # 1. Wait for the main screen to be ready
        while not isinstance(app.screen, MainScreen):
            await pilot.pause(0.1)
        
        # 2. Simulate file selection
        app.screen.selected_input_file = TEST_INPUT_FILE
        app.screen.query_one("#selected_file_label").update(f"Selected: {TEST_INPUT_FILE}")
        await pilot.pause(0.1)

        # 3. Press the "Process Books" button
        await pilot.click("#process_books_button")
        
        # 4. Wait for the UI to update
        await pilot.pause(0.5)

        # 5. Assertions: Check if the metric labels were updated
        cache_label = app.screen.query_one("#cache_stats_label").renderable
        completeness_label = app.screen.query_one("#completeness_stats_label").renderable
        google_label = app.screen.query_one("#google_api_stats_label").renderable
        loc_label = app.screen.query_one("#loc_api_stats_label").renderable

        # Check that the labels have been updated with final values
        assert "Hits: 1 / 2" in str(cache_label)
        assert "Avg: " in str(completeness_label)
        assert "Success: 2 / 2" in str(google_label)
        assert "Success: 1 / 2" in str(loc_label)

        # Check for the final status message
        final_status = app.screen.query_one("#progress_status_label").renderable
        assert "Processing complete" in str(final_status)

    # Cleanup the dummy file
    os.remove(TEST_INPUT_FILE)

