import json
import sys


def update_task_status(task_id, new_status):
    try:
        with open('project_plan.json', 'r') as f:
            plan = json.load(f)

        found = False
        for phase in plan['phases']:
            for task in phase['tasks']:
                if task['task_id'] == task_id:
                    task['status'] = new_status
                    found = True
                    break
                if 'sub_tasks' in task:
                    for sub_task in task['sub_tasks']:
                        if sub_task['task_id'] == task_id:
                            sub_task['status'] = new_status
                            found = True
                            break
            if found:
                break

        if found:
            with open('project_plan.json', 'w') as f:
                json.dump(plan, f, indent=4)
            print(f"Task {task_id} status updated to {new_status}")
        else:
            print(f"Task {task_id} not found.")

    except FileNotFoundError:
        print("project_plan.json not found.")
    except json.JSONDecodeError:
        print("Error decoding project_plan.json.")


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python update_task_status.py <task_id> <new_status>")
        sys.exit(1)

    task_id = sys.argv[1]
    new_status = sys.argv[2]
    update_task_status(task_id, new_status)
