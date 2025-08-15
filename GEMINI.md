# Textual Testing Preferences

When testing Textual full-screen applications, always use headless mode with App.run_test() and the Pilot class to simulate interactions programmatically. This prevents the CLI from being blocked by the full-screen TUI. Avoid calling app.run() in tests. Example test structure:
```python
# test_my_app.py
import pytest
from textual.app import App
from textual.pilot import Pilot
from my_app import MyTextualApp

@pytest.mark.asyncio
async def test_button_press():
    app = MyTextualApp()
    async with app.run_test() as pilot:
        await pilot.press('enter')
        assert app.some_state == 'expected_value'
```
Use pytest with pytest-asyncio for async support. Optionally, integrate pytest-textual-snapshot for visual regression testing.

```json
{
  "cliVersion": "1.0",
  "files": [
    {
      "path": "gemini.md",
      "runOnEdit": {
        "commands": [
          "python -m flake8 streamlit_app.py --max-line-length=120 --select=E,F,W",
          "black --check streamlit_app.py"
        ],
        "failOnError": true,
        "workDir": "${projectRoot}"
      },
      "hooks": {
        "preCommit": {
          "commands": [
            "python -m flake8 streamlit_app.py --max-line-length=120 --select=E,F,W",
            "black --check streamlit_app.py"
          ],
          "failOnError": true
        }
      }
    }
  ]
}
```

```json
{
  "cliVersion": "1.0",
  "preferences": {
    "python": {
      "maxScriptLines": 500,
      "modularization": {
        "enabled": true,
        "recommendation": "Split distinct functional concerns into separate modules to isolate changes and boost replace/edit success."
      }
    }
  }
}
```