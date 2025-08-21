#!/data/data/com.termux/files/usr/bin/bash

echo "Starting New Book Importer... Please wait."

# Get the directory where the script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# Run the python script from that directory
python "$SCRIPT_DIR/new_book_importer_tui.py" "$@"