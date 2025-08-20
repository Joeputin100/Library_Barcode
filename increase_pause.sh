#!/bin/bash
find . -name "test_marc_query_tui.py" -print0 | xargs -0 sed -i 's/await pilot.pause(0.5)/await pilot.pause(1)/g'
