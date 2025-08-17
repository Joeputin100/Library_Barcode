import pytest
import json
from textual.pilot import Pilot
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
        assert "You entered: 'b123'" in "".join([str(line) for line in pilot.app.query_one("#results").lines])
        assert "I understood this as: {'type': 'barcode', 'value': '123'}" in "".join([str(line) for line in pilot.app.query_one("#results").lines])

@pytest.mark.asyncio
async def test_unsupported_query():
    """Test an unsupported natural language query."""
    app = MarcQueryTUI()
    async with app.run_test() as pilot:
        query_input = pilot.app.query_one("#query-input")
        query_input.value = "all barcodes by Aveyard."
        await pilot.press("enter")
        await pilot.pause(0.1)
        assert "Unsupported or ambiguous query" in "".join([str(line) for line in pilot.app.query_one("#results").lines])

@pytest.mark.asyncio
async def test_complex_query_rejection():
    """Test a complex query that should be rejected by the current parser."""
    app = MarcQueryTUI()
    async with app.run_test() as pilot:
        query_input = pilot.app.query_one("#query-input")
        query_input.value = "b1-b290 but not b100"
        await pilot.press("enter")
        await pilot.pause(0.1)
        assert "Unsupported or ambiguous query" in "".join([str(line) for line in pilot.app.query_one("#results").lines])

@pytest.mark.asyncio
async def test_padded_zero_query():
    """Test a query with padded zeros."""
    app = MarcQueryTUI()
    async with app.run_test() as pilot:
        query_input = pilot.app.query_one("#query-input")
        query_input.value = "b1"
        await pilot.press("enter")
        await pilot.pause(0.1)
        assert "You entered: 'b1'" in "".join([str(line) for line in pilot.app.query_one("#results").lines])
        assert "I understood this as: {'type': 'barcode', 'value': 'b000001'}" in "".join([str(line) for line in pilot.app.query_one("#results").lines])

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
        json_text = results_text.split("I understood this as: ")[1].split("Is this correct?")[0]
        # Parse the JSON
        parsed_json = json.loads(json_text.replace("'", '"'))
        # The expected JSON
        expected_json = {'queries': [{'type': 'field_query', 'field': 'author', 'value': 'brandon sanderson'}, {'type': 'field_query', 'field': 'series', 'value': 'stormlight archive'}]}
        assert parsed_json == expected_json

import re

@pytest.mark.asyncio
async def test_barcode_list_query():
    """Test a query with a list of barcodes and ranges."""
    app = MarcQueryTUI()
    async with app.run_test() as pilot:
        query_input = pilot.app.query_one("#query-input")
        query_input.value = "b100 and 3957-4000"
        await pilot.press("enter")
        await pilot.pause(0.1)
        results_text = "".join([str(line) for line in pilot.app.query_one("#results").lines])
        match = re.search(r"{\"?.+?\"?}", results_text)
        json_text = match.group(0)
        parsed_json = json.loads(json_text.replace("'", '"'))
        expected_json = {'type': 'barcode_list', 'values': ['b100', {'type': 'barcode_range', 'start': '3957', 'end': '4000'}]}
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