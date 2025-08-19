
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from new_book_importer_tui import NewBookImporterTUI
from textual.widgets import DataTable, Button

@pytest.mark.asyncio
@patch('new_book_importer_tui.bigquery.Client')
async def test_initial_state(mock_bigquery_client):
    """Tests that the app loads with the expected initial widgets."""
    # Arrange
    mock_instance = mock_bigquery_client.return_value
    # Return an empty DataFrame to simulate no data on startup
    mock_instance.query.return_value.to_dataframe.return_value = pd.DataFrame()

    app = NewBookImporterTUI()

    # Act & Assert
    async with app.run_test() as pilot:
        assert pilot.app.query_one("Header")
        assert pilot.app.query_one("Footer")
        assert pilot.app.query_one("#tabs")
        assert len(pilot.app.query("Tab")) == 4
        assert pilot.app.query_one("#process_books_button")
        assert pilot.app.query_one("#data_table")
        assert pilot.app.query_one("#marc_tree")
        assert pilot.app.query_one("#generate_marc_button")
        # Check that the DataTable is empty
        assert pilot.app.query_one(DataTable).row_count == 0

@pytest.mark.asyncio
@patch('new_book_importer_tui.bigquery.Client')
async def test_process_books_handler(mock_bigquery_client):
    """Tests that calling the on_button_pressed handler with the
    'Process Books' button triggers the import process."""
    # Arrange
    app = NewBookImporterTUI()
    mock_instance = mock_bigquery_client.return_value
    mock_instance.query.return_value.to_dataframe.return_value = pd.DataFrame()

    # Mock the process_books method to check if it's called
    with patch.object(app, 'process_books', return_value=None) as mock_process_books:
        # Act
        # Directly call the event handler
        async with app.run_test() as pilot:
            button = pilot.app.query_one("#process_books_button", Button)
            pilot.app.on_button_pressed(Button.Pressed(button))

        # Assert
        mock_process_books.assert_called_once()
