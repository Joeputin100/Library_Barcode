#!/bin/bash
# Start the Flask review application

echo "🚀 Starting Barcode Project Review Application"
echo "============================================="

# Change to the review app directory
cd /data/data/com.termux/files/home/projects/barcode/review_app

# Check if Python and required packages are available
echo "Checking Python environment..."
if ! command -v python &> /dev/null; then
    echo "❌ Python is not installed. Please install Python first."
    exit 1
fi

# Install required packages if not already installed
echo "Installing required packages..."
pip install -r requirements.txt

# Initialize the database
echo "Initializing database..."
python -c "
from app import init_db
init_db()
print('Database initialized successfully')
"

# Start the Flask application
echo "Starting Flask application on port 31337..."
echo "📱 Access the review interface at: http://localhost:31337"
echo ""
echo "Note: This will run in the foreground. Press Ctrl+C to stop."
echo "The application will automatically:"
echo "  - Load all current records from current_review_state.json"
echo "  - Fetch cover images from Google Books"
echo "  - Persist all decisions in SQLite database"
echo ""

# Start the application
python app.py

# After the app stops (if it does)
echo ""
echo "Application stopped."
echo "Review decisions are saved in: ../review_decisions.json"
echo "You can resume CLI processing with the exported decisions."