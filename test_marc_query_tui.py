import re

import pytest
import json
from marc_query_tui import MarcQueryTUI


@pytest.mark.asyncio
async def test_initial_state():
    """Test that the app starts up correctly."""
    app = MarcQueryTUI()
    async with app.run_test() as pilot:
        assert pilot.app.query_one("#query-input").placeholder == "Enter your query"
        assert pilot.app.query_one("#results").visible


@pytest.mark.asyncio
async def test_simple_barcode_query():
    """Test a simple barcode query."""
    app = MarcQueryTUI()
    async with app.run_test() as pilot:
        query_input = pilot.app.query_one("#query-input")
        query_input.value = "b123"
        await pilot.press("enter")
        await pilot.pause(0.1)
        await pilot.pause(0.1)
        assert "You entered: 'b123'" in "".join(
            [line.text for line in pilot.app.query_one("#results").lines]
        )
        assert "I understood this as: {'type': 'barcode', 'value': '123'}" in "".join(
            [line.text for line in pilot.app.query_one("#results").lines]
        )


@pytest.mark.asyncio
async def test_unsupported_query():
    """Test an unsupported natural language query."""
    app = MarcQueryTUI()
    async with app.run_test() as pilot:
        query_input = pilot.app.query_one("#query-input")
        query_input.value = "all barcodes by Aveyard."
        await pilot.press("enter")
        await pilot.pause(0.1)
        assert "Unsupported or ambiguous query" in "".join(
            [line.text for line in pilot.app.query_one("#results").lines]
        )


@pytest.mark.asyncio
async def test_complex_query_rejection():
    """Test a complex query that should be rejected by the current parser."""
    app = MarcQueryTUI()
    async with app.run_test() as pilot:
        query_input = pilot.app.query_one("#query-input")
        query_input.value = "b1-b290 but not b100"
        await pilot.press("enter")
        await pilot.pause(0.1)
        assert "Unsupported or ambiguous query" in "".join(
            [line.text for line in pilot.app.query_one("#results").lines]
        )


@pytest.mark.asyncio
async def test_padded_zero_query():
    """Test a query with padded zeros."""
    app = MarcQueryTUI()
    async with app.run_test() as pilot:
        query_input = pilot.app.query_one("#query-input")
        query_input.value = "b1"
        await pilot.press("enter")
        await pilot.pause(0.1)
        assert "You entered: 'b1'" in "".join(
            [line.text for line in pilot.app.query_one("#results").lines]
        )
        assert (
            "I understood this as: {'type': 'barcode', 'value': 'b000001'}"
            in "".join([line.text for line in pilot.app.query_one("#results").lines])
        )


@pytest.mark.asyncio
async def test_and_query():
    """Test a query with 'and'."""
    app = MarcQueryTUI()
    async with app.run_test() as pilot:
        query_input = pilot.app.query_one("#query-input")
        query_input.value = "author: brandon sanderson and series: stormlight archive"
        await pilot.press("enter")
        await pilot.pause(0.1)
        # Get the text from the RichLog
        results_text = ""
        for line in pilot.app.query_one("#results").lines:
            results_text += line.text
        # Extract the JSON part of the text
        json_text = results_text.split("I understood this as: ")[1].split(
            "Is this correct?"
        )[0]
        # Parse the JSON
        parsed_json = json.loads(json_text.replace("'", '"'))
        # The expected JSON
        expected_json = {
            "queries": [
                {
                    "type": "field_query",
                    "field": "author",
                    "value": "brandon sanderson",
                },
                {
                    "type": "field_query",
                    "field": "series",
                    "value": "stormlight archive",
                },
            ]
        }
        assert parsed_json == expected_json




        results_text = "".join(
            [line.text for line in pilot.app.query_one("#results").lines]
        )
        # Extract the JSON part of the text more robustly
        start_index = results_text.find("I understood this as: ") + len(
            "I understood this as: "
        )
        end_index = results_text.find("Is this correct?")
        json_text = results_text[start_index:end_index].strip()
        parsed_json = json.loads(json_text.replace("'", '"'))
        expected_json = {
            "type": "barcode_list",
            "values": [
                "0000100",
                {"type": "barcode_range", "start": "0003957", "end": "0004000"},
            ],
        }
        assert parsed_json == expected_json


@pytest.mark.asyncio
async def test_quit_action():
    """Test the quit action."""
    app = MarcQueryTUI()
    async with app.run_test() as pilot:
        await pilot.press("q")
        app.exit()
        await pilot.pause(0.1)
        assert not app.is_running


@pytest.mark.asyncio
async def test_barcode_range_b100_b105():
    """Test barcode range query b100-b105."""
    app = MarcQueryTUI()
    async with app.run_test() as pilot:
        query_input = pilot.app.query_one("#query-input")
        query_input.value = "b100-b105"
        await pilot.press("enter")
        await pilot.pause(0.1)  # Wait for query parsing and confirmation prompt

        # Confirm the query
        query_input.value = "y"
        await pilot.press("enter")
        await pilot.pause(0.5)  # Wait for query execution and results to appear

        results_text = "".join(
            [line.text for line in pilot.app.query_one("#results").lines]
        )
        assert "Found 6 records." in results_text


@pytest.mark.asyncio
async def test_books_by_aveyard():
    """Test query for books by Aveyard."""
    app = MarcQueryTUI()
    async with app.run_test() as pilot:
        query_input = pilot.app.query_one("#query-input")
        query_input.value = "author: Aveyard"
        await pilot.press("enter")
        await pilot.pause(0.1)  # Wait for query parsing and confirmation prompt

        # Confirm the query
        query_input.value = "y"
        await pilot.press("enter")
        await pilot.pause(0.5)  # Wait for query execution and results to appear

        results_text = "".join(
            [line.text for line in pilot.app.query_one("#results").lines]
        )
        assert "Found 4 records." in results_text


@pytest.mark.asyncio
async def test_barcodes_starting_with_lp():
    """Test query for barcodes starting with lp."""
    app = MarcQueryTUI()
    async with app.run_test() as pilot:
        query_input = pilot.app.query_one("#query-input")
        query_input.value = "barcodes beginning with lp"
        await pilot.press("enter")
        await pilot.pause(0.1)  # Wait for query parsing and confirmation prompt

        # Confirm the query
        query_input.value = "y"
        await pilot.press("enter")
        await pilot.pause(0.5)  # Wait for query execution and results to appear

        results_text = "".join(
            [line.text for line in pilot.app.query_one("#results").lines]
        )
        assert "Found 35 records." in results_text


@pytest.mark.asyncio
async def test_mixed_barcode_query():
    """Test a mixed query with specific barcodes and a range."""
    app = MarcQueryTUI()
    async with app.run_test() as pilot:
        query_input = pilot.app.query_one("#query-input")
        query_input.value = "lp35, b163, 3957-3960"
        await pilot.press("enter")
        await pilot.pause(0.1)  # Wait for query parsing and confirmation prompt

        # Confirm the query
        query_input.value = "y"
        await pilot.press("enter")
        await pilot.pause(0.5)  # Wait for query execution and results to appear

        results_text = "".join(
            [line.text for line in pilot.app.query_one("#results").lines]
        )
        assert "Found 6 records." in results_text


@pytest.mark.asyncio
async def test_barcode_range_B000100_B000105():
    """Test barcode range query B000100 to B000105."""
    app = MarcQueryTUI()
    async with app.run_test() as pilot:
        query_input = pilot.app.query_one("#query-input")
        query_input.value = "B000100 to B000105"
        await pilot.press("enter")
        await pilot.pause(0.1)  # Wait for query parsing and confirmation prompt

        # Confirm the query
        query_input.value = "y"
        await pilot.press("enter")
        await pilot.pause(0.5)  # Wait for query execution and results to appear

        results_text = "".join(
            [line.text for line in pilot.app.query_one("#results").lines]
        )
        assert "Found 6 records." in results_text
