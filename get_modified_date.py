import os
from datetime import datetime
from zoneinfo import ZoneInfo  # Python 3.9+

file_path = "/data/data/com.termux/files/home/projects/barcode/streamlit_app.py"
timestamp = os.path.getmtime(file_path)
dt_object = datetime.fromtimestamp(timestamp)

# Define Pacific Time (PT) timezone
pacific_time = ZoneInfo("America/Los_Angeles")
dt_pacific = dt_object.astimezone(pacific_time)

formatted_date = dt_pacific.strftime("%Y-%m-%d %H:%M:%S %Z%z")
print(f"Last modified date (PT): {formatted_date}")
