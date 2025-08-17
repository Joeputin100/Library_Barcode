import json
import subprocess
from textual.app import App, ComposeResult
from textual.widgets import (
    Header,
    Footer,
    Static,
    Tree,
    Tabs,
    Tab,
    Button,
    Input,
    Menu,
    MenuItem,
)
from textual.screen import ModalScreen
from textual.containers import Vertical, Container, Horizontal
from duet_api import parse_query_with_duet


class ProjectViewer(App):
    """A Textual app to view project information."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.current_filter = "all" # "all", "completed", "incomplete"
        self.current_sort = "id" # "id", "add_date", "status_date"
        self.auto_expand_tree = True # True/False

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
                yield Input(placeholder="Enter new task description...")
                yield Button("Add Task", id="add_task")
                yield Button("View Options", id="view_options_button")
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

    def send_notification(self, title, content):
        try:
            subprocess.run(
                ["termux-notification", "--title", title, "--content", content]
            )
        except FileNotFoundError:
            pass

    def add_task_from_nl(self, task_description):
        phases = [p["phase_name"] for p in self.plan["phases"]]
        prompt = f"""You are a JSON-only AI assistant. Your only function is to convert a user\'s request into a structured JSON object.

The user\'s request is: "{task_description}"

You must identify the following information:
1.  `task_name`: A concise title for the task.
2.  `description`: A one-sentence description of the task.
3.  `model`: Classify as "Pro" or "Flash". "Pro" tasks involve
    complex reasoning, planning, or code generation. "Flash" tasks are
    simpler, like edits, running commands, or simple lookups.
4.  `phase_name`: Categorize the task into one of the following existing
    project phases: {json.dumps(phases, indent=2)}
5.  `dependencies`: List any task IDs that this new task depends on. If none,
    provide an empty list.

Return ONLY a single, valid JSON object with these fields.

Example Request: "add a button to the UI to export data to csv"
Example Output:
{{
    "task_name": "Add CSV Export Button",
    "description": "Add a button to the main UI that allows users to export
    the current data view as a CSV file.",
    "model": "Flash",
    "phase_name": "Phase 3: Streamlit Integration and MARC Export",
    "dependencies": []
}}"""

        response_text = parse_query_with_duet(prompt)

        try:
            new_task_data = json.loads(response_text)

            required_keys = ["task_name", "description", "model", "phase_name"]
            if not all(key in new_task_data for key in required_keys):
                self.send_notification(
                    "Error", "AI response was missing required fields."
                )
                return

            if "error" in new_task_data:
                self.send_notification(
                    "Error", f"Could not parse task: {new_task_data['error']}"
                )
                return

            # Find the phase and add the task
            phase_found = False
            for phase in self.plan["phases"]:
                if phase["phase_name"] == new_task_data["phase_name"]:
                    phase_found = True
                    # Generate a new task ID
                    last_task_id = (
                        phase["tasks"][-1]["task_id"]
                        if phase["tasks"]
                        else f"{self.plan['phases'].index(phase) + 1}.0"
                    )
                    major_id = int(last_task_id.split(".")[0])
                    minor_id = int(last_task_id.split(".")[1])
                    new_task_id = f"{major_id}.{minor_id + 1}"

                    new_task = {
                        "task_id": new_task_id,
                        "task_name": new_task_data["task_name"],
                        "status": "TODO",
                        "description": new_task_data["description"],
                        "model": new_task_data["model"],
                        "dependencies": new_task_data.get(
                            "dependencies", []
                        ),
                    }
                    phase["tasks"].append(new_task)
                    break

            if not phase_found:
                self.send_notification(
                    "Error",
                    f"Phase '{new_task_data['phase_name']}' not found.",
                )
                return

            with open("project_plan.json", "w") as f:
                json.dump(self.plan, f, indent=4)

            self.send_notification(
                "Project Plan Updated", f"New task '{new_task_id}' added."
            )
            self.load_project_plan()
            self.app.push_screen(TaskConfirmationModal(new_task_id, new_task_data["description"]))

        except (json.JSONDecodeError, KeyError) as e:
            self.send_notification("Error", f"Could not parse task: {e}")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        """Handle button presses."""
        if event.button.id == "quit_button":
            self.exit()
        elif event.button.id == "add_task":
            input_widget = self.query_one(Input)
            task_description = input_widget.value
            if task_description:
                self.add_task_from_nl(task_description)
                input_widget.value = ""
        elif event.button.id == "modal_ok":
            self.app.pop_screen()
        elif event.button.id == "view_options_button":
            def handle_view_option(selected_id: str):
                if selected_id.startswith("filter_"):
                    self.current_filter = selected_id.replace("filter_", "")
                elif selected_id.startswith("sort_"):
                    self.current_sort = selected_id.replace("sort_", "")
                elif selected_id == "auto_expand_yes":
                    self.auto_expand_tree = True
                elif selected_id == "auto_expand_no":
                    self.auto_expand_tree = False
                elif selected_id == "show_gemini_memory":
                    self.app.push_screen(GeminiMemoryScreen())
                    return
                elif selected_id == "show_git_status": # New block
                    self.app.push_screen(GitStatusScreen())
                    return # Don't reload project plan for this action
                self.log(f"View option selected: {selected_id}")
                self.load_project_plan()

            self.app.push_screen(ViewOptionsScreen(), handle_view_option)

    def load_project_plan(self):
        """Load the project plan from the JSON file."""
        tree = self.query_one("#plan-tree", Tree)
        tree.clear()
        last_done_node = None
        try:
            with open("project_plan.json") as f:
                self.plan = json.load(f)

            root = tree.root
            root.label = self.plan["project_name"]

            for phase in self.plan["phases"]:
                phase_node = root.add(phase["phase_name"])
                if phase.get("status") == "DONE":
                    last_done_node = phase_node

                # Filter tasks
                filtered_tasks = []
                for task in phase["tasks"]:
                    if self.current_filter == "all":
                        filtered_tasks.append(task)
                    elif self.current_filter == "completed" and task.get("status") == "DONE":
                        filtered_tasks.append(task)
                    elif self.current_filter == "incomplete" and task.get("status") != "DONE":
                        filtered_tasks.append(task)

                # Sort tasks (simple sort for now, more complex sorts might need custom keys)
                if self.current_sort == "id":
                    filtered_tasks.sort(key=lambda t: t["task_id"])
                # Add more sorting logic here for 'add_date' and 'status_date' if available in task data

                for task in filtered_tasks: # Iterate over filtered and sorted tasks
                    status_emoji = (
                        "✅ "
                        if task.get("status") == "DONE"
                        else (
                            "⏰ "
                            if task.get("status") == "PROCESSING"
                            else "🚧 "
                            if task.get("status") == "IN PROGRESS"
                            else ""
                        )
                    )
                    task_node = phase_node.add(
                        f"{status_emoji}{task['task_id']}: "
                        f"{task['task_name']} ({task['status']})"
                    )
                    if "dependencies" in task and task["dependencies"]:
                        task_node.add(
                            f"Dependencies: "
                            f"{', '.join(task['dependencies'])}"
                        )
                    if task.get("status") == "DONE":
                        last_done_node = task_node
                    if "sub_tasks" in task:
                        for sub_task in task["sub_tasks"]:
                            sub_task_status_emoji = (
                                "✅ "
                                if sub_task.get("status") == "DONE"
                                else (
                                    "⏰ "
                                    if sub_task.get("status")
                                    == "PROCESSING"
                                    else (
                                        "🚧 "
                                        if sub_task.get("status")
                                        == "IN PROGRESS"
                                        else ""
                                    )
                                )
                            )
                            sub_task_node = task_node.add(
                                f"{sub_task_status_emoji}{sub_task['task_id']}: "
                                f"{sub_task['task_name']} "
                                f"({sub_task['status']})"
                            )
                            if (
                                "dependencies" in sub_task
                                and sub_task["dependencies"]
                            ):
                                sub_task_node.add(
                                    f"Dependencies: "
                                    f"{', '.join(sub_task['dependencies'])}"
                                )
                            if sub_task.get("status") == "DONE":
                                last_done_node = sub_task_node

            if self.auto_expand_tree and last_done_node: # Apply auto-expand based on attribute
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


class TaskConfirmationModal(ModalScreen):
    def __init__(self, task_id: str, task_description: str) -> None:
        super().__init__()
        self.task_id = task_id
        self.task_description = task_description

    def compose(self) -> ComposeResult:
        yield Vertical(
            Static(f"Task Added Successfully!", classes="modal-title"),
            Static(f"Task ID: {self.task_id}", classes="modal-content"),
            Static(f"Description: {self.task_description}", classes="modal-content"),
            Button("OK", id="modal_ok", classes="modal-button"),
            classes="modal-dialog"
        )


class ViewOptionsScreen(ModalScreen):
    def compose(self) -> ComposeResult:
        yield Vertical(
            Static("View Options", classes="modal-title"),
            Menu(
                MenuItem("All Tasks", id="filter_all"),
                MenuItem("Completed Tasks", id="filter_completed"),
                MenuItem("Incomplete Tasks", id="filter_incomplete"),
                MenuItem("---"), # Separator
                MenuItem("Sort by Task ID", id="sort_id"),
                MenuItem("Sort by Task Add Date", id="sort_add_date"),
                MenuItem("Sort by Task Status Last Modified Date", id="sort_status_date"),
                MenuItem("---"), # Separator
                MenuItem("Auto-expand Tree: Yes", id="auto_expand_yes"),
                MenuItem("Auto-expand Tree: No", id="auto_expand_no"),
                MenuItem("---"), # Separator
                MenuItem("Gemini Memory/Preferences", id="show_gemini_memory"), # New MenuItem
                MenuItem("Git Status and History", id="show_git_status"), # New MenuItem
                id="view_options_menu"
            ),
            Button("Close", id="close_view_options", classes="modal-button"),
            classes="modal-dialog"
        )

    def on_menu_item_selected(self, event: Menu.ItemSelected) -> None:
        # This will send a message to the parent app
        self.dismiss(event.item.id)

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "close_view_options":
            self.dismiss()


class GeminiMemoryScreen(ModalScreen):
    def compose(self) -> ComposeResult:
        gemini_md_content = ""
        try:
            with open("GEMINI.md", "r") as f:
                gemini_md_content = f.read()
        except FileNotFoundError:
            gemini_md_content = "GEMINI.md not found."

        project_state_content = ""
        try:
            with open("project_state.json", "r") as f:
                project_state_content = json.dumps(json.load(f), indent=4)
        except FileNotFoundError:
            project_state_content = "project_state.json not found."
        except json.JSONDecodeError:
            project_state_content = "Error decoding project_state.json."

        yield Vertical(
            Static("Gemini Memory and Preferences", classes="modal-title"),
            Static("--- GEMINI.md ---", classes="modal-content"),
            Static(gemini_md_content, classes="modal-content"),
            Static("--- project_state.json ---", classes="modal-content"),
            Static(project_state_content, classes="modal-content"),
            Button("Close", id="close_gemini_memory", classes="modal-button"),
            classes="modal-dialog"
        )

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "close_gemini_memory":
            self.dismiss()


class GitStatusScreen(ModalScreen):
    def compose(self) -> ComposeResult:
        git_status_output = ""
        try:
            result = subprocess.run(["git", "status"], capture_output=True, text=True, check=True)
            git_status_output = result.stdout
        except subprocess.CalledProcessError as e:
            git_status_output = f"Error getting git status: {e.stderr}"
        except FileNotFoundError:
            git_status_output = "Git command not found. Is Git installed and in PATH?"

        git_log_output = ""
        try:
            result = subprocess.run(["git", "log", "-n", "10", "--pretty=format:%h - %an, %ar : %s"], capture_output=True, text=True, check=True)
            git_log_output = result.stdout
        except subprocess.CalledProcessError as e:
            git_log_output = f"Error getting git log: {e.stderr}"
        except FileNotFoundError:
            git_log_output = "Git command not found. Is Git installed and in PATH?"

        yield Vertical(
            Static("Git Status and History", classes="modal-title"),
            Static("--- Git Status ---", classes="modal-content"),
            Static(git_status_output, classes="modal-content"),
            Static("--- Git Log (last 10) ---", classes="modal-content"),
            Static(git_log_output, classes="modal-content"),
            Button("Close", id="close_git_status", classes="modal-button"),
            classes="modal-dialog"
        )

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "close_git_status":
            self.dismiss()


if __name__ == "__main__":
    print("\033]0;Project Viewer\a", end="")
    app = ProjectViewer()
    app.run()