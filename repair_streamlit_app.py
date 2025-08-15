#!/usr/bin/env python3
"""
repair_streamlit_app.py

1. Normalize indentation in 4-space steps.
2. Remove any stray code after the final
   if __name__ == "__main__": main() block.
Backs up the original file to streamlit_app.py.bak.
"""

import re
import shutil
import sys

INDENT_STEP = 4


def repair(path):
    # Backup
    shutil.copy2(path, path + ".bak")

    lines = open(path, "r").read().expandtabs(INDENT_STEP).splitlines(True)
    out = []
    prev_indent = 0
    seen_main = False

    for ln in lines:
        # If we hit the main entrypoint, keep it and its call, then stop.
        if re.match(r"^if __name__\s*==\s*['\"]__main__['\"]\s*:", ln):
            out.append(ln)
            seen_main = True
            continue
        if seen_main:
            if re.search(r"\bmain\s*\(\s*\)", ln):
                out.append(" " * INDENT_STEP + "main()\n")
            # skip everything else
            continue

        # Preserve blank lines
        if not ln.strip():
            out.append(ln)
            continue

        # Re-indent at most one level deeper than previous
        stripped = ln.lstrip(" ")
        curr_indent = len(ln) - len(stripped)
        if curr_indent > prev_indent + INDENT_STEP:
            curr_indent = prev_indent + INDENT_STEP

        out.append(" " * curr_indent + stripped)
        prev_indent = curr_indent

    with open(path, "w") as f:
        f.writelines(out)

    print(f"✔ Repaired and normalized {path} (backup at {path}.bak)")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} path/to/streamlit_app.py")
        sys.exit(1)
    repair(sys.argv[1])
