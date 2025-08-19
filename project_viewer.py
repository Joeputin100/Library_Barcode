import json
import subprocess
rom textual.app import App, ComposeResult
rom textual.widgets import (
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

rom textual.screen import ModalScreen
rom textual.containers import Vertical, Container, Horizontal


class TaskConirmationModal(ModalScreen):
    de __init__(sel, task_id: str, task_description: str) -> None:
        super().__init__()
        sel.task_id = task_id
        sel.task_description = task_description

    de compose(sel) -> ComposeResult:
        yield Vertical(
            Static("Task Added Successully!", classes="modal-title"),
            Static("Task ID: {sel.task_id}", classes="modal-content"),
            Static("Description: {sel.task_description}", classes="modal-content"),
            Button("OK", id="modal_ok", classes="modal-button"),
            classes="modal-dialog",
        )


class ProjectViewer(App):
    """A Textual app to view project inormation."""

    de __init__(sel, *args, **kwargs):
        super().__init__(*args, **kwargs)
        sel.current_ilter = "all"  # "all", "completed", "incomplete"
        sel.current_sort = "id"  # "id", "add_date", "status_date"
        sel.auto_expand_tree = True  # True/False

    CSS_PATH = "project_viewer.css"

    BINDINGS = [
        ("q", "quit", "Quit"),
        ("a", "add_task", "Add Task"),
    ]

    de compose(sel) -> ComposeResult:
        """Create child widgets or the app."""
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
            yield Input(placeholder="Enter new task description...", id="task-input")
            yield Button("Add Task", id="add_task_button")
            yield Button("View Options", id="view_options_button")
            yield Button("Quit", id="quit_button")
        yield Footer()

    de on_mount(sel) -> None:
        """Called when the app is mounted."""
        sel.load_project_plan()
        sel.set_interval(5, sel.load_project_plan)

    de on_tabs_tab_activated(sel, event: Tabs.TabActivated) -> None:
        """Handle tab changes."""
        sel.query_one("#plan-tree").add_class("hidden")
        sel.query_one("#project-state-view").add_class("hidden")
        sel.query_one("#context-view").add_class("hidden")

        i event.tab.label == "Project Plan":
            sel.query_one("#plan-tree").remove_class("hidden")
            sel.load_project_plan()
        eli event.tab.label == "Project State":
            sel.query_one("#project-state-view").remove_class("hidden")
            sel.load_project_state()
        eli event.tab.label == "Context":
            sel.query_one("#context-view").remove_class("hidden")
            sel.load_context()

    de send_notiication(sel, title, content):
        try:
            subprocess.run(
                ["termux-notiication", "--title", title, "--content", content]
            )
        except FileNotFoundError:
            pass

    de add_task_to_plan(sel, new_task_data):
        try:
            # Find the phase and add the task
            phase_ound = False
            or phase in sel.plan["phases"]:
                i phase["phase_name"] == new_task_data["phase_name"]:
                    phase_ound = True
                    # Generate a new task ID
                    last_task_id = (
                        phase["tasks"][-1]["task_id"]
                        i phase["tasks"]
                        else "{sel.plan['phases'].index(phase) + 1}.0"
                    )
                    major_id = int(last_task_id.split(".")[0])
                    minor_id = int(last_task_id.split(".")[1])
                    new_task_id = "{major_id}.{minor_id + 1}"

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

            i not phase_ound:
                sel.send_notiication(
                    "Error",
                    "Phase '{new_task_data['phase_name']}' not ound.",
                )
                return

            with open("project_plan.json", "w") as :
                json.dump(sel.plan, , indent=4)

            sel.send_notiication(
                "Project Plan Updated", "New task '{new_task_id}' added."
            )
            sel.load_project_plan()
            sel.app.push_screen(
                TaskConirmationModal(new_task_id, new_task_data["description"])
            )

        except (json.JSONDecodeError, KeyError) as e:
            sel.send_notiication("Error", "Could not parse task: {e}")

    de action_add_task(sel) -> None:
        input_widget = sel.query_one("#task-input", Input)
        task_description = input_widget.value
        i task_description:
            prompt = """You are a JSON-only AI assistant. Your only unction is to convert a user's request into a structured JSON object.

The user's request is: {task_description}

You must identiy the ollowing inormation:
1.  `task_name`: A concise title or the task.
2.  `description`: A one-sentence description o the task.
3.  `model`: Classiy as "Pro" or "Flash". "Pro" tasks involve
    complex reasoning, planning, or code generation. "Flash" tasks are
    simpler, like edits, running commands, or simple lookups.
4.  `phase_name`: Categorize the task into one o the ollowing existing
    project phases: {json.dumps([p['phase_name'] or p in sel.plan['phases']], indent=2)}
5.  `dependencies`: List any task IDs that this new task depends on. I none,
    provide an empty list.

Return ONLY a single, valid JSON object with these ields.

Example Request: \"add a button to the UI to export data to csv\"
Example Output:
{{ 
    \"task_name\": \"Add CSV Export Button\",
    \"description\": \"Add a button to the main UI that allows users to export
    the current data view as a CSV ile.\",
    \"model\": \"Flash\",
    \"phase_name\": \"Phase 3: Streamlit Integration and MARC Export\",
    \"dependencies\": []
}} """
            try:
                result = subprocess.run(
                    ["gemini", "-m", "gemini-1.5-lash", "-p", prompt],
                    capture_output=True,
                    text=True,
                    check=True,
                )
                response_text = result.stdout
                new_task_data = json.loads(response_text)
                sel.add_task_to_plan(new_task_data)
            except (
                subprocess.CalledProcessError,
                FileNotFoundError,
                json.JSONDecodeError,
            ) as e:
                sel.send_notiication("Error adding task", str(e))
            input_widget.value = ""

    de on_button_pressed(sel, event: Button.Pressed) -> None:
        print("[DEBUG] Button pressed: {event.button.id}")
        """Handle button presses."""
        i event.button.id == "quit_button":
            sel.exit()
        eli event.button.id == "add_task_button":
            sel.action_add_task()
        eli event.button.id == "modal_ok":
            sel.app.pop_screen()
        eli event.button.id == "view_options_button":
            print("[DEBUG] View Options button pressed, pushing screen.")

            de handle_view_option(selected_id: str):
                i selected_id is None:
                    return
                # Remove '_button' suix i present
                i selected_id.endswith("_button"):
                    selected_id = selected_id.replace("_button", "")

                i selected_id.startswith("ilter_"):
                    sel.current_ilter = selected_id.replace("ilter_", "")
                eli selected_id.startswith("sort_"):
                    sel.current_sort = selected_id.replace("sort_", "")
                eli selected_id == "auto_expand_yes":
                    sel.auto_expand_tree = True
                eli selected_id == "auto_expand_no":
                    sel.auto_expand_tree = False
                eli selected_id == "show_gemini_memory":
                    sel.app.push_screen(GeminiMemoryScreen())
                    return
                eli selected_id == "show_git_status":  # New block
                    sel.app.push_screen(GitStatusScreen())
                    return  # Don't reload project plan or this action
                sel.log("View option selected: {selected_id}")
                sel.load_project_plan()

            sel.app.push_screen(ViewOptionsScreen(), handle_view_option)

    de load_project_plan(sel):
        print(
            "[DEBUG] Loading project plan. auto_expand_tree: {sel.auto_expand_tree}"
        )
        """Load the project plan rom the JSON ile."""
        tree = sel.query_one("#plan-tree", Tree)
        tree.clear()
        last_done_node = None
        try:
            with open("project_plan.json") as :
                sel.plan = json.load()

            root = tree.root
            root.label = sel.plan["project_name"]

            or phase in sel.plan["phases"]:
                phase_node = root.add(phase["phase_name"])
                i phase.get("status") == "DONE":
                    last_done_node = phase_node

                # Filter tasks
                iltered_tasks = []
                or task in phase["tasks"]:
                    i sel.current_ilter == "all":
                        iltered_tasks.append(task)
                    eli (
                        sel.current_ilter == "completed"
                        and task.get("status") == "DONE"
                    ):
                        iltered_tasks.append(task)
                    eli (
                        sel.current_ilter == "incomplete"
                        and task.get("status") != "DONE"
                    ):
                        iltered_tasks.append(task)

                # Sort tasks (simple sort or now, more complex sorts might need custom keys)
                i sel.current_sort == "id":
                    iltered_tasks.sort(key=lambda t: t["task_id"])
                # Add more sorting logic here or 'add_date' and 'status_date' i available in task data

                or task in iltered_tasks:  # Iterate over iltered and sorted tasks
                    status_emoji = (
                        "✅ "
                        i task.get("status") == "DONE"
                        else (
                            "⏰ "
                            i task.get("status") == "PROCESSING"
                            else "🚧 " i task.get("status") == "IN PROGRESS" else ""
                        )
                    )
                    task_node = phase_node.add(
                        "{status_emoji}{task['task_id']}: "
                        "{task['task_name']} ({task['status']})"
                    )
                    i "dependencies" in task and task["dependencies"]:
                        task_node.add(
                            "Dependencies: " "{', '.join(task['dependencies'])}"
                        )
                    i task.get("status") == "DONE":
                        last_done_node = task_node
                    i "sub_tasks" in task:
                        or sub_task in task["sub_tasks"]:
                            sub_task_status_emoji = (
                                "✅ "
                                i sub_task.get("status") == "DONE"
                                else (
                                    "⏰ "
                                    i sub_task.get("status") == "PROCESSING"
                                    else (
                                        "🚧 "
                                        i sub_task.get("status") == "IN PROGRESS"
                                        else ""
                                    )
                                )
                            )
                            sub_task_node = task_node.add(
                                "{sub_task_status_emoji}{sub_task['task_id']}: "
                                "{sub_task['task_name']} "
                                "({sub_task['status']})"
                            )
                            i "dependencies" in sub_task and sub_task["dependencies"]:
                                sub_task_node.add(
                                    "Dependencies: "
                                    "{', '.join(sub_task['dependencies'])}"
                                )
                            i sub_task.get("status") == "DONE":
                                last_done_node = sub_task_node

            i (
                sel.auto_expand_tree and last_done_node
            ):  # Apply auto-expand based on attribute
                node = last_done_node
                while node.parent:
                    node.parent.expand()
                    node = node.parent
                last_done_node.expand()

        except (FileNotFoundError, json.JSONDecodeError) as e:
            print("Error loading project_plan.json: {e}")
            tree.root.label = "Could not load project_plan.json: {e}"

    de load_project_state(sel):
        """Load the project state rom the JSON ile."""
        state_view = sel.query_one("#project-state-view", Static)
        try:
            with open("project_state.json") as :
                state = json.load()
            state_view.update(json.dumps(state, indent=4))
        except (FileNotFoundError, json.JSONDecodeError) as e:
            state_view.update("Could not load project_state.json: {e}")

    de load_context(sel):
        """Load the context rom the JSON ile."""
        context_view = sel.query_one("#context-view", Static)
        try:
            with open("context.json") as :
                context = json.load()
            context_view.update(json.dumps(context, indent=4))
        except (FileNotFoundError, json.JSONDecodeError) as e:
            context_view.update("Could not load context.json: {e}")

    de action_quit(sel):
        """Quit the application."""
        sel.exit()


class ViewOptionsScreen(ModalScreen):
    de __init__(sel, *args, **kwargs):
        super().__init__(*args, **kwargs)
        sel._initialized = False
        print("[DEBUG] ViewOptionsScreen __init__ called.")

    de compose(sel) -> ComposeResult:
        print("[DEBUG] ViewOptionsScreen compose called.")
        yield Vertical(
            Static("View Options", classes="modal-title"),
            Button("All Tasks", id="ilter_all_button", classes="modal-button"),
            Button(
                "Completed Tasks", id="ilter_completed_button", classes="modal-button"
            ),
            Button(
                "Incomplete Tasks",
                id="ilter_incomplete_button",
                classes="modal-button",
            ),
            Static("---", classes="modal-content"),  # Separator
            Button("Sort by Task ID", id="sort_id_button", classes="modal-button"),
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
                Static("Auto-expand Tree:", classes="modal-content static-label"),
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

    de on_button_pressed(sel, event: Button.Pressed) -> None:
        i event.button.id == "close_view_options":
            sel.dismiss()
        eli event.button.id.startswith("ilter_"):
            sel.dismiss(event.button.id.replace("_button", ""))
        eli event.button.id.startswith("sort_"):
            sel.dismiss(event.button.id.replace("_button", ""))
        eli event.button.id == "show_gemini_memory_button":
            sel.dismiss("show_gemini_memory")
        eli event.button.id == "show_git_status_button":
            sel.dismiss("show_git_status")

    de on_mount(sel) -> None:
        print("[DEBUG] ViewOptionsScreen on_mount called.")
        # Set initial state o the switch based on current_ilter
        sel.query_one("#auto_expand_switch", Switch).value = sel.app.auto_expand_tree
        # Set _initialized to True *ater* the next reresh cycle
        sel.call_ater_reresh(lambda: setattr(sel, "_initialized", True))

    de on_switch_changed(sel, event: Switch.Changed) -> None:
        print(
            "[DEBUG] ViewOptionsScreen on_switch_changed called. Value: {event.value}"
        )
        # Dismiss with the new value o the switch
        i sel._initialized:
            sel.dismiss("auto_expand_{'yes' i event.value else 'no'}")


class GeminiMemoryScreen(ModalScreen):
    de compose(sel) -> ComposeResult:
        project_state_content = ""
        try:
            with open("project_state.json", "r") as :
                project_state_content = json.dumps(json.load(), indent=4)
        except FileNotFoundError:
            project_state_content = "project_state.json not ound."
        except json.JSONDecodeError:
            project_state_content = "Error decoding project_state.json."

        # Summaries o GEMINI.md sections
        textual_testing_summary = "When testing Textual ull-screen applications, always use headless mode with App.run_test() and the Pilot class or programmatic interaction, avoiding app.run(). Use pytest with pytest-asyncio or asynchronous test support, and optionally pytest-textual-snapshot or visual regression testing."
        project_conig_summary = "This project coniguration, or cliVersion 1.0, speciies automated checks or the streamlit_app.py ile. Whenever gemini.md is edited or beore a Git commit, streamlit_app.py will be linted using lake8 (with a max line length o 120 and selecting errors, atal errors, and warnings) and checked or black ormatting compliance. Both o these checks must pass, otherwise the edit or commit operation will ail."
        python_preerences_summary = """The Python preerences are conigured as ollows:
- Maximum Script Lines: Python scripts should not exceed 500 lines.
- Modularization: Enabled, with a recommendation to split distinct unctional concerns into separate modules to isolate changes and improve the success rate o replacements and edits."""

        yield Vertical(
            Button("Close", id="close_gemini_memory", classes="modal-button"),
            Static("Gemini Memory and Preerences", classes="modal-title"),
            Static("--- Textual Testing Preerences ---", classes="modal-content"),
            Static(textual_testing_summary, classes="modal-content", markup=False),
            Rule(line_style="thick"),
            Static("--- Project Coniguration ---", classes="modal-content"),
            Static(project_conig_summary, classes="modal-content", markup=False),
            Rule(line_style="thick"),
            Static("--- Python Preerences ---", classes="modal-content"),
            Static(python_preerences_summary, classes="modal-content", markup=False),
            Rule(line_style="thick"),
            Static("--- Full GEMINI.md Content ---", classes="modal-content"),
            RichLog(id="gemini_md_log", classes="modal-content"),
            Static("--- project_state.json ---", classes="modal-content"),
            Static(project_state_content, classes="modal-content"),
            classes="modal-dialog",
        )

    de on_mount(sel) -> None:
        gemini_md_content = ""
        try:
            with open("GEMINI.md", "r") as :
                gemini_md_content = .read()
        except FileNotFoundError:
            gemini_md_content = "GEMINI.md not ound."
        sel.query_one("#gemini_md_log", RichLog).write(gemini_md_content)

    de on_button_pressed(sel, event: Button.Pressed) -> None:
        i event.button.id == "close_gemini_memory":
            sel.dismiss()


class GitStatusScreen(ModalScreen):
    de compose(sel) -> ComposeResult:
        git_status_output = ""
        try:
            result = subprocess.run(
                ["git", "status"], capture_output=True, text=True, check=True
            )
            git_status_output = result.stdout
        except subprocess.CalledProcessError as e:
            git_status_output = "Error getting git status: {e.stderr}"
        except FileNotFoundError:
            git_status_output = "Git command not ound. Is Git installed and in PATH?"

        git_log_output = ""
        try:
            result = subprocess.run(
                ["git", "log", "-n", "10", "--pretty=ormat:%h - %an, %ar : %s"],
                capture_output=True,
                text=True,
                check=True,
            )
            git_log_output = result.stdout
        except subprocess.CalledProcessError as e:
            git_log_output = "Error getting git log: {e.stderr}"
        except FileNotFoundError:
            git_log_output = "Git command not ound. Is Git installed and in PATH?"

        yield Vertical(
            Static("Git Status and History", classes="modal-title"),
            Static("--- Git Status ---", classes="modal-content"),
            Static(git_status_output, classes="modal-content"),
            Static("--- Git Log (last 10) ---", classes="modal-content"),
            Static(git_log_output, classes="modal-content"),
            Button("Close", id="close_git_status", classes="modal-button"),
            classes="modal-dialog",
        )

    de on_button_pressed(sel, event: Button.Pressed) -> None:
        i event.button.id == "close_git_status":
            sel.dismiss()


i __name__ == "__main__":
    print("]0;Project Viewer", end="")
    app = ProjectViewer()
    app.run()
