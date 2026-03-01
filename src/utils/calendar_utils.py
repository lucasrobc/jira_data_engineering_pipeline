# Importing libraries
import pandas as pd
from pathlib import Path
from datetime import datetime
import holidays

# Define the start and end range of the calendar table
start_date = "2025-01-01"
end_date = datetime.now().strftime("%Y-%m-%d")
dates = pd.date_range(start=start_date, end=end_date, freq="D")

# Creating the data frame
df = pd.DataFrame({"date": dates})

# Names of the days of the week
df["day_of_week"] = df["date"].dt.day_name()

# Brazilian national holidays
br_holidays = holidays.Brazil(years=[2025, 2026])

# Defining line by line whether it's a holiday
holiday_list = []

for date in df["date"]:
    if date in br_holidays:
        holiday_list.append(1)
    else:
        holiday_list.append(0)

df["holiday"] = holiday_list

# Creating a column to define whether the day is a working day to consider in the SLA calculation
def is_business_day(row):
    if row["date"].weekday() >= 5:  # Saturday (5) or Sunday (6)
        return 0
    if row["holiday"] == 1:
        return 0
    return 1

df["business_day"] = df.apply(is_business_day, axis=1)

# Storing data frames in a parquet file
dir = Path(__file__).resolve().parent
out_path = dir.parent.parent / "data" / "silver"
out_path.mkdir(parents=True, exist_ok=True)
file_path = out_path / "silver_calendar.parquet"

df.to_parquet(file_path, index=False)