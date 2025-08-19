#!/data/data/com.termux/files/usr/bin/bash

# Fix F401: Unused imports
sed -i "/from reportlab.platypus import SimpleDocTemplate, Spacer/d" label_generator.py
sed -i "/from reportlab.platypus.flowables import KeepInFrame/d" label_generator.py
sed -i "/import shutil/d" label_generator.py
sed -i "/import csv/d" label_generator.py
sed -i "/import json/d" marc_query_tui.py
sed -i "s/from textual.widgets import (Header, Footer, Input, Static, RichLog, Markdown, LoadingIndicator,)/from textual.widgets import (Header, Footer, Input, RichLog, Markdown, LoadingIndicator,)/" marc_query_tui.py
sed -i "s/from textual.containers import Vertical, Container/from textual.containers import Vertical/" marc_query_tui.py
sed -i "s/from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer/from reportlab.platypus import Paragraph/" pdf_generation.py
sed -i "/from reportlab.platypus.flowables import KeepInFrame/d" pdf_generation.py
sed -i "/import os/d" project_viewer.py
sed -i "/from textual.pilot import Pilot/d" test_marc_query_tui.py
sed -i "/import re/d" test_marc_query_tui.py
sed -i "/from textual.pilot import Pilot/d" test_project_viewer.py
sed -i "/import shutil/d" indent_normalizer.py
sed -i "/import csv/d" loc_query_debugger.py

# Fix F841: local variable assigned to but never used
sed -i "s/vertical_offset = 0//g" label_generator.py
sed -i "s/operator = parsed_query['operator']//g" marc_query_tui.py
sed -i "s/vertical_offset = 0//g" pdf_generation.py

# Fix F821: undefined name
sed -i "s/except Exception as e:/except requests.exceptions.RequestException as e:/" loc_query_debugger.py
sed -i "s/from textual.app import App, ComposeResult/from textual.app import App, ComposeResult, events/" marc_query_tui.py
sed -i "s/from marc_exporter import convert_df_to_marc, write_marc_file/from marc_exporter import convert_df_to_marc, write_marc_file\nfrom pdf_generation import generate_pdf_labels/" streamlit_app.py

# Fix E203: whitespace before ":"
sed -i "s/ : /:/g" data_cleaner.py
sed -i "s/ : /:/g" data_cleaning.py
sed -i "s/ : /:/g" external_enricher.py
sed -i "s/ : /:/g" loc_enricher.py

# Fix F541: f-string is missing placeholders
sed -i "s/f"""/"""/g" project_viewer.py

# Fix E402: module level import not at top of file
sed -i "/import re/d" test_marc_query_tui.py
sed -i "1s/^/import re\n/" test_marc_query_tui.py
