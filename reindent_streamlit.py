#!/usr/bin/env python3
"""
reindent_streamlit.py

Run this to apply the lib2to3 “reindent” fixer to your Streamlit script.
It will rewrite streamlit_app.py in place, fixing mixed or unexpected
indent levels across the entire file.
"""

import sys
from lib2to3.refactor import RefactoringTool, get_fixers_from_package

def main(path):
    # Only load the reindent fixer
    all_fixers = get_fixers_from_package("lib2to3.fixes")
    reindent_fixer = [f for f in all_fixers if f.endswith("fix_reindent")]
    if not reindent_fixer:
        print("Could not find fix_reindent in lib2to3.fixes")
        sys.exit(1)

    tool = RefactoringTool(reindent_fixer)
    tool.refactor_file(path, write=True)
    print(f"Re-indent complete: {path}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} path/to/streamlit_app.py")
        sys.exit(1)
    main(sys.argv[1])
