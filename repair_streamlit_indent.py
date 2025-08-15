#!/usr/bin/env python3
"""
repair_streamlit_indent.py

Dedents the wrongly indented `if st.button("Apply Manual Classifications...")` block
in your Streamlit script by removing 4 spaces from its start and from every
subsequent line in that block.
"""

import sys

def repair_file(path):
    with open(path, 'r') as f:
        lines = f.readlines()

    repaired = []
    in_button_block = False

    for line in lines:
        # Detect the over-indented button-click block
        stripped = line.lstrip()
        if stripped.startswith('if st.button("Apply Manual Classifications') and line.startswith(' ' * 8):
            # Remove 4 spaces of indent
            repaired.append(line[4:])
            in_button_block = True
            continue

        if in_button_block:
            # If we hit a line that's no longer part of the 8-space indent,
            # we’ve left the block
            if not line.startswith(' ' * 8) or line.strip() == "":
                in_button_block = False
                repaired.append(line)
            else:
                repaired.append(line[4:])
        else:
            repaired.append(line)

    with open(path, 'w') as f:
        f.writelines(repaired)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} path/to/streamlit_app.py")
        sys.exit(1)
    repair_file(sys.argv[1])
    print("Indentation repair complete.")
