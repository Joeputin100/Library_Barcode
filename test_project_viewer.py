import pytest

from project_viewer import ProjectViewer


@pytest.mark.asyncio
async def test_project_viewer_initialization():
    """Test that the ProjectViewer app initializes without errors."""
    app = ProjectViewer()
    async with app.run_test() as pilot:
        # Just check if it starts and quits
        await pilot.press("q")
