import pytest
from textual.pilot import Pilot
from project_viewer import ProjectViewer
import json

@pytest.mark.asyncio
async def test_add_task():
    app = ProjectViewer()
    async with app.run_test() as pilot:
        # 1. Get the initial state of the project plan
        with open("project_plan.json", "r") as f:
            initial_plan = json.load(f)
        initial_task_count = len(initial_plan["phases"][0]["tasks"])

        # 2. Simulate user input and button press
        await pilot.press("tab") # Focus the input
        task_description = "Replace horizontal scrolling with line wrapping in the Project Viewer."
        await pilot.press(*task_description)
        await pilot.press("tab") # Focus the button
        await pilot.press("enter") # Press the button

        # 3. Give the app time to process
        await pilot.pause(2) # Revert to original pause time

        # 4. Check if the project plan has been updated
        with open("project_plan.json", "r") as f:
            updated_plan = json.load(f)
        updated_task_count = len(updated_plan["phases"][0]["tasks"])

        assert updated_task_count == initial_task_count + 1, "The task was not added to the project plan."

        # 5. Check the details of the new task
        new_task = updated_plan["phases"][0]["tasks"][-1]
        assert new_task["task_name"] == "Replace horizontal scrolling with line wrapping"
        assert new_task["model"] == "Flash"