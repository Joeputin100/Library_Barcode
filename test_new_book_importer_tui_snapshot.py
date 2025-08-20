import pytest
from unittest.mock import patch, MagicMock
from new_book_importer_tui import NewBookImporterTUI


@pytest.mark.asyncio
@patch("new_book_importer_tui.bigquery.Client")
async def test_tui_snapshot(mock_bigquery_client, snapshot):
    """Tests the initial state of the TUI with a snapshot."""
    # Arrange
    mock_instance = mock_bigquery_client.return_value
    mock_instance.query.return_value.to_dataframe.return_value = MagicMock()

    app = NewBookImporterTUI(dev_mode=True)

    # Act & Assert
    async with app.run_test() as pilot:
        snapshot.assert_match(pilot.app.screen.tree)
