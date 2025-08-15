import json
from textual.app import App, ComposeResult
from textual.widgets import Header, Footer, Static, Tree, Tabs, Tab, Button
from textual.containers import Vertical, Container, Horizontal


class ProjectViewer(App):
    """A Textual app to view project information."""

    CSS_PATH = "project_viewer.css"

    BINDINGS = [
        ("q", "quit", "Quit"),
    ]

    def compose(self) -> ComposeResult:
        """Create child widgets for the app."""
        yield Header()
        with Vertical():
            with Tabs(id="tabs"):
                yield Tab("Project Plan")
                yield Tab("Project State")
                yield Tab("Context")
            with Container(id="content-container"):
                yield Tree("Project Plan", id="plan-tree")
                yield Static(id="project-state-view", classes="hidden")
                yield Static(id="context-view", classes="hidden")
            with Horizontal(id="buttons"):
                yield Button("Add Task", id="add_task")
                yield Button("Quit", id="quit_button")
        yield Footer()

    def on_mount(self) -> None:
        """Called when the app is mounted."""
        self.load_project_plan()
        self.set_interval(5, self.load_project_plan)

    def on_tabs_tab_activated(self, event: Tabs.TabActivated) -> None:
        """Handle tab changes."""
        self.query_one("#plan-tree").add_class("hidden")
        self.query_one("#project-state-view").add_class("hidden")
        self.query_one("#context-view").add_class("hidden")

        if event.tab.label == "Project Plan":
            self.query_one("#plan-tree").remove_class("hidden")
            self.load_project_plan()
        elif event.tab.label == "Project State":
            self.query_one("#project-state-view").remove_class("hidden")
            self.load_project_state()
        elif event.tab.label == "Context":
            self.query_one("#context-view").remove_class("hidden")
            self.load_context()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        """Handle button presses."""
        if event.button.id == "quit_button":
            self.exit()
        elif event.button.id == "add_task":
            # Placeholder for adding a task
            pass

    def load_project_plan(self):
        """Load the project plan from the JSON file."""
        tree = self.query_one("#plan-tree", Tree)
        tree.clear()
        last_done_node = None
        try:
            with open("project_plan.json") as f:
                plan = json.load(f)

            root = tree.root
            root.label = plan["project_name"]

            for phase in plan["phases"]:
                phase_node = root.add(phase["phase_name"])
                if phase.get("status") == "DONE":
                    last_done_node = phase_node
                for task in phase["tasks"]:
                    status_emoji = "✅ " if task.get("status") == "DONE" else "⏰ " if task.get("status") == "PROCESSING" else ""
                    task_node = phase_node.add(f"{status_emoji}{task['task_id']}: {task['task_name']} ({task['status']})")
                    if task.get("status") == "DONE":
                        last_done_node = task_node
                    if "sub_tasks" in task:

                        for sub_task in task["sub_tasks"]:
                            sub_task_status_emoji = "✅ " if sub_task.get("status") == "DONE" else "⏰ " if sub_task.get("status") == "PROCESSING" else ""
                            sub_task_node = task_node.add(f"{sub_task_status_emoji}{sub_task['task_id']}: {sub_task['task_name']} ({sub_task['status']})")
                            if sub_task.get("status") == "DONE":
                                last_done_node = sub_task_node

            if last_done_node:
                node = last_done_node
                while node.parent:
                    node.parent.expand()
                    node = node.parent
                last_done_node.expand()

        except (FileNotFoundError, json.JSONDecodeError) as e:
            tree.root.label = f"Could not load project_plan.json: {e}"

    def load_project_state(self):
        """Load the project state from the JSON file."""
        state_view = self.query_one("#project-state-view", Static)
        try:
            with open("project_state.json") as f:
                state = json.load(f)
            state_view.update(json.dumps(state, indent=4))
        except (FileNotFoundError, json.JSONDecodeError) as e:
            state_view.update(f"Could not load project_state.json: {e}")

    def load_context(self):
        """Load the context from the JSON file."""
        context_view = self.query_one("#context-view", Static)
        try:
            with open("context.json") as f:
                context = json.load(f)
            context_view.update(json.dumps(context, indent=4))
        except (FileNotFoundError, json.JSONDecodeError) as e:
            context_view.update(f"Could not load context.json: {e}")

    def action_quit(self):
        """Quit the application."""
        self.exit()


if __name__ == "__main__":
    app = ProjectViewer()
    app.run()
