import pytest
from unittest.mock import patch, MagicMock
from project_viewer import ProjectViewer


@pytest.mark.asyncio
@patch("project_viewer.subprocess.run")
@patch("project_viewer.ProjectViewer.add_task_to_plan")
async def test_add_task_with_real_data(mock_add_task_to_plan, mock_subprocess_run):
    """Tests that action_add_task correctly parses the gemini output and calls add_task_to_plan."""
    # Arrange
    app = ProjectViewer()
    mock_process = MagicMock()
    mock_process.stdout = '{"task_name": "Fix Tree Collapse Bug", "description": "The tree collapses a few seconds after the user expands it.", "model": "Flash", "phase_name": "Phase 9: Code Quality and Maintenance", "dependencies": []}'
    mock_subprocess_run.return_value = mock_process

    # Act
    async with app.run_test() as pilot:
        input_widget = pilot.app.query_one("#task-input")
        input_widget.value = "@project_viewer.css the tree collapses a few seconds after the user expands it.  fix this bug."
        await pilot.press("a")

        # Assert
        mock_add_task_to_plan.assert_called_once_with({
            "task_name": "Fix Tree Collapse Bug",
            "description": "The tree collapses a few seconds after the user expands it.",
            "model": "Flash",
            "phase_name": "Phase 9: Code Quality and Maintenance",
            "dependencies": []
        })
