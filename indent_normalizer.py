#!/usr/bin/env python3
"""
indent_normalizer.py

Heuristic re-indenter for Python files using a fixed 4-space step.
Backs up your original to streamlit_app.py.bak and rewrites the file in place.
"""

import sys
import shutil

INDENT_STEP = 4


def normalize(path):
    # make a backup in case it goes sideways
    shutil.copy2(path, path + ".bak")

    with open(path, "r") as f:
        raw_lines = f.readlines()

    new_lines = []
    prev_indent = 0

    for line in raw_lines:
        # expand tabs → spaces
        expanded = line.expandtabs(INDENT_STEP)
        stripped = expanded.lstrip(" ")
        # preserve blank lines verbatim
        if not stripped.strip():
            new_lines.append(expanded)
            continue

        # count leading spaces
        curr_indent = len(expanded) - len(stripped)

        # if we jumped more than one step deeper, pull back
        if curr_indent > prev_indent + INDENT_STEP:
            curr_indent = prev_indent + INDENT_STEP

        new_lines.append(" " * curr_indent + stripped)
        prev_indent = curr_indent

    with open(path, "w") as f:
        f.writelines(new_lines)
    print(f"✔ Normalized indentation in {path}  (backup at {path}.bak)")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python indent_normalizer.py path/to/streamlit_app.py")
        sys.exit(1)
    normalize(sys.argv[1])
