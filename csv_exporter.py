from collections import defaultdict
import os
import csv
from datetime import datetime, timedelta
import pandas as pd

def export_monthly_csv(city_time, output_dir, group_by, log_func):
    if group_by == "by_city":
        output_file = os.path.join(output_dir, "by_city_location_days.csv")
        header = ["Location", "Fractional Days"]
    else:
        output_file = os.path.join(output_dir, "by_state_location_days.csv")
        header = ["Location", "Fractional Days"]

    # Convert city_time to list of tuples for sorting
    sorted_items = sorted(city_time.items(), key=lambda x: (-x[1], x[0]))

    log_func(f"💾 Writing: {output_file}")
    try:
        with open(output_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(header)
            for location, days in sorted_items:
                writer.writerow([location, f"{days:.1f}"])
    except Exception as e:
        log_func(f"❌ Error writing {output_file}: {e}")
