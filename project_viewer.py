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
    Switch,
    RichLog,
    Rule,
)

from textual.screen import ModalScreen
from textual.containers import Vertical, Container, Horizontal


class TaskConfirmationModal(ModalScreen):
    def __init__(
        self, new_task_data: dict, new_task_id: str, *args, **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.new_task_data = new_task_data
        self.new_task_id = new_task_id

    def compose(self) -> ComposeResult:
        yield Vertical(
            Static("Task Added Successfully!", classes="modal-title"),
            Static(f"Task ID: {self.new_task_id}", classes="modal-content"),
            Static(
                f"Name: {self.new_task_data.get('task_name', 'N/A')}",
                classes="modal-content",
            ),
            Static(
                f"Description: {self.new_task_data.get('description', 'N/A')}",
                classes="modal-content",
            ),
            Static(
                f"Model: {self.new_task_data.get('model', 'N/A')}",
                classes="modal-content",
            ),
            Static(
                f"Phase: {self.new_task_data.get('phase_name', 'N/A')}",
                classes="modal-content",
            ),
            Static(
                f"Dependencies: {', '.join(self.new_task_data.get('dependencies', []))}",
                classes="modal-content",
            ),
            Button("OK", id="modal_ok", classes="modal-button"),
            classes="modal-dialog",
        )


class ProjectViewer(App):
    """A Textual app to view project information."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.current_filter = "all"  # "all", "completed", "incomplete"
        self.current_sort = "id"  # "id", "add_date", "status_date"
        self.auto_expand_tree = True  # True/False

    CSS_PATH = "project_viewer.css"

    BINDINGS = [
        ("q", "quit", "Quit"),
        ("a", "add_task", "Add Task"),
    ]

    def compose(self) -> ComposeResult:
        """Create child widgets for the app."""
        yield Header()
        with Vertical():
            with Tabs(id="tabs"):
                yield Tab("Project Plan")
                yield Tab("Project State")
                yield Tab("Context")
                yield Tab("New Book Importer Plan")
            with Container(id="content-container"):
                yield Tree("Project Plan", id="plan-tree")
                yield Static(id="project-state-view", classes="hidden")
                yield Static(id="context-view", classes="hidden")
                yield Tree("New Book Importer Plan", id="new-importer-plan-tree", classes="hidden")

        with Horizontal(id="buttons"):
            yield Input(
                placeholder="Enter new task description...", id="task-input"
            )
            yield Button("Add Task", id="add_task_button")
            yield Button("Refresh", id="refresh_button")
            yield Button("View Options", id="view_options_button")
            yield Button("Quit", id="quit_button")
        yield Footer()

    def on_mount(self) -> None:
        """Called when the app is mounted."""
        self.load_project_plan()

    def on_tabs_tab_activated(self, event: Tabs.TabActivated) -> None:
        """Handle tab changes."""
        self.query_one("#plan-tree").add_class("hidden")
        self.query_one("#project-state-view").add_class("hidden")
        self.query_one("#context-view").add_class("hidden")
        self.query_one("#new-importer-plan-tree").add_class("hidden") # Hide new tree

        if event.tab.label == "Project Plan":
            self.query_one("#plan-tree").remove_class("hidden")
            self.load_project_plan()
        elif event.tab.label == "Project State":
            self.query_one("#project-state-view").remove_class("hidden")
            self.load_project_state()
        elif event.tab.label == "Context":
            self.query_one("#context-view").remove_class("hidden")
            self.load_context()
        elif event.tab.label == "New Book Importer Plan": # New condition
            self.query_one("#new-importer-plan-tree").remove_class("hidden")
            self.load_new_book_importer_plan()

    def send_notification(self, title, content):
        try:
            subprocess.run(
                ["termux-notification", "--title", title, "--content", content]
            )
        except FileNotFoundError:
            pass

    def add_task_to_plan(self, new_task_data):
        try:
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
                        "dependencies": new_task_data.get("dependencies", []),
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
            self.app.push_screen(
                TaskConfirmationModal(new_task_data, new_task_id)
            )

        except (json.JSONDecodeError, KeyError) as e:
            self.send_notification("Error", f"Could not parse task: {e}")

    def action_add_task(self) -> None:
        input_widget = self.query_one("#task-input", Input)
        task_description = input_widget.value
        if task_description:
            prompt = "You are a JSON-only AI assistant. Your only function is to convert a user's request into a structured JSON object.\n\nThe user's request is: " + task_description + "\n\nYou must identiy the following inormation:\n1.  `task_name`: A concise title for the task.\n2.  `description`: A one-sentence description of the task.\n3.  `model`: Classify as \"Pro\" or \"Flash\". \"Pro\" tasks involve\n    complex reasoning, planning, or code generation. \"Flash\" tasks are\n    simpler, like edits, running commands, or simple lookups.\n4.  `phase_name`: Categorize the task into one of the following existing\n    project phases: " + json.dumps([p['phase_name'] for p in self.plan['phases']], indent=2) + "\n5.  `dependencies`: List any task IDs that this new task depends on. If none,\n    provide an empty list.\n\nReturn ONLY a single, valid JSON object with these fields."
            try:
                result = subprocess.run(
                    ["gemini", "-m", "gemini-2.5-flash", prompt],
                    capture_output=True,
                    text=True,
                    check=True,
                )
                response_text = result.stdout
                new_task_data = json.loads(response_text)
                self.add_task_to_plan(new_task_data)
            except (
                subprocess.CalledProcessError,
                FileNotFoundError,
                json.JSONDecodeError,
            ) as e:
                self.send_notification("Error adding task", str(e))
            input_widget.value = ""

    def on_button_pressed(self, event: Button.Pressed) -> None:
        print(f"[DEBUG] Button pressed: {event.button.id}")
        """Handle button presses."""
        if event.button.id == "quit_button":
            self.exit()
        elif event.button.id == "add_task_button":
            self.action_add_task()
        elif event.button.id == "refresh_button":
            self.load_project_plan()
        elif event.button.id == "modal_ok":
            self.app.pop_screen()
        elif event.button.id == "view_options_button":
            print("[DEBUG] View Options button pressed, pushing screen.")

            def handle_view_option(selected_id: str):
                if selected_id is None:
                    return
                # Remove '_button' sufix if present
                if selected_id.endswith("_button"):
                    selected_id = selected_id.replace("_button", "")

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
                elif selected_id == "show_git_status":  # New block
                    self.app.push_screen(GitStatusScreen())
                    return  # Don't reload project plan for this action
                self.log(f"View option selected: {selected_id}")
                self.load_project_plan()

            self.app.push_screen(ViewOptionsScreen(), handle_view_option)

    def load_project_plan(self):
        print(
            f"[DEBUG] Loading project plan. auto_expand_tree: {self.auto_expand_tree}"
        )
        """Load the project plan from the JSON ile."""
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
                    elif (
                        self.current_filter == "completed"
                        and task.get("status") == "DONE"
                    ):
                        filtered_tasks.append(task)
                    elif (
                        self.current_filter == "incomplete"
                        and task.get("status") != "DONE"
                    ):
                        filtered_tasks.append(task)

                # Sort tasks (simple sort for now, more complex sorts might need custom keys)
                if self.current_sort == "id":
                    filtered_tasks.sort(key=lambda t: t["task_id"])
                # Add more sorting logic here for 'add_date' and 'status_date' if available in task data

                for (
                    task
                ) in filtered_tasks:  # Iterate over filtered and sorted tasks
                    status_emoji = (
                        "✅ "
                        if task.get("status") == "DONE"
                        else (
                            "⏰ "
                            if task.get("status") == "PROCESSING"
                            else (
                                "🚧 "
                                if task.get("status") == "IN PROGRESS"
                                else ""
                            )
                        )
                    )
                    task_node = phase_node.add(
                        f"{status_emoji}{task['task_id']}: "
                        f"{task['task_name']} ({task['status']})"
                    )
                    if "dependencies" in task and task["dependencies"]:
                        task_node.add(
                            f"Dependencies: {', '.join(task['dependencies'])}"
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
                                    if sub_task.get("status") == "PROCESSING"
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
                                    "Dependencies: "
                                    f"{', '.join(sub_task['dependencies'])}"
                                )
                            if sub_task.get("status") == "DONE":
                                last_done_node = sub_task_node

            if (
                self.auto_expand_tree and last_done_node
            ):  # Apply auto-expand based on attribute
                node = last_done_node
                while node.parent:
                    node.parent.expand()
                    node = node.parent
                last_done_node.expand()

        except (FileNotFoundError, json.JSONDecodeError) as e:
            print(f"Error loading project_plan.json: {e}")
            tree.root.label = f"Could not load project_plan.json: {e}"

    def load_project_state(self):
        """Load the project state from the JSON ile."""
        state_view = self.query_one("#project-state-view", Static)
        try:
            with open("project_state.json") as f:
                state = json.load(f)
            state_view.update(json.dumps(state, indent=4))
        except (FileNotFoundError, json.JSONDecodeError) as e:
            state_view.update(f"Could not load project_state.json: {e}")

    def load_context(self):
        """Load the context from the JSON ile."""
        context_view = self.query_one("#context-view", Static)
        try:
            with open("context.json") as f:
                context = json.load(f)
            context_view.update(json.dumps(context, indent=4))
        except (FileNotFoundError, json.JSONDecodeError) as e:
            context_view.update(f"Could not load context.json: {e}")

    def load_context(self):
        """Load the context from the JSON ile."""
        context_view = self.query_one("#context-view", Static)
        try:
            with open("context.json") as f:
                context = json.load(f)
            context_view.update(json.dumps(context, indent=4))
        except (FileNotFoundError, json.JSONDecodeError) as e:
            context_view.update(f"Could not load context.json: {e}")

    def load_new_book_importer_plan(self):
        print(
            f"[DEBUG] Loading new book importer plan. auto_expand_tree: {self.auto_expand_tree}"
        )
        """Load the new book importer plan from the JSON file."""
        tree = self.query_one("#new-importer-plan-tree", Tree)
        tree.clear()
        last_done_node = None
        try:
            with open("new_book_importer_plan.json") as f:
                self.new_importer_plan = json.load(f)

            root = tree.root
            root.label = self.new_importer_plan["project_name"]

            for phase in self.new_importer_plan["phases"]:
                phase_node = root.add(phase["phase_name"])
                if phase.get("status") == "DONE":
                    last_done_node = phase_node

                # Filter tasks
                filtered_tasks = []
                for task in phase["tasks"]:
                    if self.current_filter == "all":
                        filtered_tasks.append(task)
                    elif (
                        self.current_filter == "completed"
                        and task.get("status") == "DONE"
                    ):
                        filtered_tasks.append(task)
                    elif (
                        self.current_filter == "incomplete"
                        and task.get("status") != "DONE"
                    ):
                        filtered_tasks.append(task)

                # Sort tasks (simple sort for now, more complex sorts might need custom keys)
                if self.current_sort == "id":
                    filtered_tasks.sort(key=lambda t: t["task_id"])
                # Add more sorting logic here for 'add_date' and 'status_date' if available in task data

                for (
                    task
                ) in filtered_tasks:  # Iterate over filtered and sorted tasks
                    status_emoji = (
                        "✅ "
                        if task.get("status") == "DONE"
                        else (
                            "⏰ "
                            if task.get("status") == "PROCESSING"
                            else (
                                "🚧 "
                                if task.get("status") == "IN PROGRESS"
                                else ""
                            )
                        )
                    )
                    task_node = phase_node.add(
                        f"{status_emoji}{task['task_id']}: "
                        f"{task['task_name']} ({task['status']})"
                    )
                    if "dependencies" in task and task["dependencies"]:
                        task_node.add(
                            f"Dependencies: {', '.join(task['dependencies'])}"
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
                                    if sub_task.get("status") == "PROCESSING"
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
                                    "Dependencies: "
                                    f"{', '.join(sub_task['dependencies'])}"
                                )
                            if sub_task.get("status") == "DONE":
                                last_done_node = sub_task_node

            if (
                self.auto_expand_tree and last_done_node
            ):  # Apply auto-expand based on attribute
                node = last_done_node
                while node.parent:
                    node.parent.expand()
                    node = node.parent
                last_done_node.expand()

        except (FileNotFoundError, json.JSONDecodeError) as e:
            print(f"Error loading new_book_importer_plan.json: {e}")
            tree.root.label = f"Could not load new_book_importer_plan.json: {e}"

    def action_quit(self):
        """Quit the application."""
        self.exit()


class ViewOptionsScreen(ModalScreen):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._initialized = False
        print("[DEBUG] ViewOptionsScreen __init__ called.")

    def compose(self) -> ComposeResult:
        print("[DEBUG] ViewOptionsScreen compose called.")
        yield Vertical(
            Static("View Options", classes="modal-title"),
            Button(
                "All Tasks", id="filter_all_button", classes="modal-button"
            ),
            Button(
                "Completed Tasks",
                id="filter_completed_button",
                classes="modal-button",
            ),
            Button(
                "Incomplete Tasks",
                id="filter_incomplete_button",
                classes="modal-button",
            ),
            Static("---", classes="modal-content"),  # Separator
            Button(
                "Sort by Task ID", id="sort_id_button", classes="modal-button"
            ),
            Button(
                "Sort by Task Add Date",
                id="sort_add_date_button",
                classes="modal-button",
            ),
            Button(
                "Sort by Task Status Last Modiied Date",
                id="sort_status_date_button",
                classes="modal-button",
            ),
            Static("---", classes="modal-content"),  # Separator
            Horizontal(
                Static(
                    "Auto-expand Tree:", classes="modal-content static-label"
                ),
                Switch(id="auto_expand_switch", classes="auto-expand-switch"),
                classes="switch-container",
            ),
            Static("---", classes="modal-content"),  # Separator
            Button(
                "Gemini Memory/Preerences",
                id="show_gemini_memory_button",
                classes="modal-button",
            ),
            Button(
                "Git Status and History",
                id="show_git_status_button",
                classes="modal-button",
            ),
            Button("Close", id="close_view_options", classes="modal-button"),
            classes="modal-dialog",
        )

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "close_view_options":
            self.dismiss()
        elif event.button.id.startswith("filter_"):
            self.dismiss(event.button.id.replace("_button", ""))
        elif event.button.id.startswith("sort_"):
            self.dismiss(event.button.id.replace("_button", ""))
        elif event.button.id == "show_gemini_memory_button":
            self.dismiss("show_gemini_memory")
        elif event.button.id == "show_git_status_button":
            self.dismiss("show_git_status")

    def on_mount(self) -> None:
        print("[DEBUG] ViewOptionsScreen on_mount called.")
        # Set initial state of the switch based on current_filter
        self.query_one("#auto_expand_switch", Switch).value = (
            self.app.auto_expand_tree
        )
        # Set _initialized to True *after* the next refresh cycle
        self.call_after_refresh(lambda: setattr(self, "_initialized", True))

    def on_switch_changed(self, event: Switch.Changed) -> None:
        print(
            f"[DEBUG] ViewOptionsScreen on_switch_changed called. Value: {event.value}"
        )
        # Dismiss with the new value of the switch
        if self._initialized:
            self.dismiss(f"auto_expand_{'yes' if event.value else 'no'}")


class GeminiMemoryScreen(ModalScreen):
    def compose(self) -> ComposeResult:
        project_state_content = ""
        try:
            with open("project_state.json", "r") as f:
                project_state_content = json.dumps(json.load(f), indent=4)
        except FileNotFoundError:
            project_state_content = "project_state.json not found."
        except json.JSONDecodeError:
            project_state_content = "Error decoding project_state.json."

        # Summaries of GEMINI.md sections
        textual_testing_summary = "When testing Textual full-screen applications, always use headless mode with App.run_test() and the Pilot class for programmatic interaction, avoiding app.run(). Use pytest with pytest-asyncio for asynchronous test support, and optionally pytest-textual-snapshot for visual regression testing."
        project_config_summary = "This project configuration, for cliVersion 1.0, specifies automated checks for the streamlit_app.py file. Whenever gemini.md is edited or before a Git commit, streamlit_app.py will be linted using flake8 (with a max line length of 120 and selecting errors, fatal errors, and warnings) and checked for black formatting compliance. Both of these checks must pass, otherwise the edit or commit operation will fail."
        python_preferences_summary = """The Python preferences are configured as follows:
- Maximum Script Lines: Python scripts should not exceed 500 lines.
- Modularization: Enabled, with a recommendation to split distinct functional concerns into separate modules to isolate changes and improve the success rate of replacements and edits."""

        yield Vertical(
            Button("Close", id="close_gemini_memory", classes="modal-button"),
            Static("Gemini Memory and Preferences", classes="modal-title"),
            Static(
                "--- Textual Testing Preferences ---", classes="modal-content"
            ),
            Static(
                textual_testing_summary, classes="modal-content", markup=False
            ),
            Rule(line_style="thick"),
            Static("--- Project Configuration ---", classes="modal-content"),
            Static(
                project_config_summary, classes="modal-content", markup=False
            ),
            Rule(line_style="thick"),
            Static("--- Python Preferences ---", classes="modal-content"),
            Static(
                python_preferences_summary,
                classes="modal-content",
                markup=False,
            ),
            Rule(line_style="thick"),
            Static("--- Full GEMINI.md Content ---", classes="modal-content"),
            RichLog(id="gemini_md_log", classes="modal-content"),
            Static("--- project_state.json ---", classes="modal-content"),
            Static(project_state_content, classes="modal-content"),
            classes="modal-dialog",
        )

    def on_mount(self) -> None:
        gemini_md_content = ""
        try:
            with open("GEMINI.md", "r") as f:
                gemini_md_content = f.read()
        except FileNotFoundError:
            gemini_md_content = "GEMINI.md not found."
        self.query_one("#gemini_md_log", RichLog).write(gemini_md_content)

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "close_gemini_memory":
            self.dismiss()


class GitStatusScreen(ModalScreen):
    def compose(self) -> ComposeResult:
        git_status_output = ""
        try:
            result = subprocess.run(
                ["git", "status"],
                capture_output=True,
                text=True,
                check=True,
            )
            git_status_output = result.stdout
        except subprocess.CalledProcessError as e:
            git_status_output = f"Error getting git status: {e.stderr}"
        except FileNotFoundError:
            git_status_output = (
                "Git command not found. Is Git installed and in PATH?"
            )

        git_log_output = ""
        try:
            result = subprocess.run(
                [
                    "git",
                    "log",
                    "-n",
                    "10",
                    "--pretty=format:%h - %an, %ar : %s",
                ],
                capture_output=True,
                text=True,
                check=True,
            )
            git_log_output = result.stdout
        except subprocess.CalledProcessError as e:
            git_log_output = f"Error getting git log: {e.stderr}"
        except FileNotFoundError:
            git_log_output = (
                "Git command not found. Is Git installed and in PATH?"
            )

        yield Vertical(
            Static("Git Status and History", classes="modal-title"),
            Static("--- Git Status ---", classes="modal-content"),
            Static(git_status_output, classes="modal-content"),
            Static("--- Git Log (last 10) ---", classes="modal-content"),
            Static(git_log_output, classes="modal-content"),
            Button("Close", id="close_git_status", classes="modal-button"),
            classes="modal-dialog",
        )

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "close_git_status":
            self.dismiss()


if __name__ == "__main__":
    print("\033]0;Project Viewer\007", end="")
    app = ProjectViewer()
    app.run()
