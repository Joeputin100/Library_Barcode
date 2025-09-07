#!/bin/bash
# Start the Enhanced Batch Review Application

echo "🚀 Starting Enhanced Batch Review Application"
echo "==============================================="

# Change to the review app directory
cd /data/data/com.termux/files/home/projects/barcode

# Check if Python and required packages are available
echo "Checking Python environment..."
if ! command -v python &> /dev/null; then
    echo "❌ Python is not installed. Please install Python first."
    exit 1
fi

# Install required packages if not already installed
echo "Installing required packages..."
pip install -r review_app/requirements.txt

# Send initial notification
termux-notification --title "Batch Review Starting" --content "Processing 440 records..."

# Start the enhanced batch application
echo "Starting Enhanced Batch Application on port 31338..."
echo "📱 Access the review interface at: http://localhost:31338"
echo ""
echo "FEATURES:"
echo "• Single-record display with progress tracking"
echo "• Cover image safety filter (only verified books)"
echo "• Alternate edition fallback system"
echo "• MSRP-aware price calculation"
echo "• 440 total records to process"
echo ""
echo "The application will automatically:"
echo "• Parse and pre-process all 440 records"
echo "• Apply alternate edition fallbacks"
echo "• Calculate MSRP-aware prices"
echo "• Filter cover images for safety"
echo ""

# Start the application
python review_app_batch.py

# After the app stops
termux-notification --title "Batch Review Complete" --content "All 440 records processed!"
echo ""
echo "✅ Batch processing complete!"
echo "All 440 records have been processed."
echo "Review decisions are saved in the database."