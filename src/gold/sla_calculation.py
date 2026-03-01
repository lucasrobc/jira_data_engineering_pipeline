import pandas as pd
from datetime import time

# SLA rules
def get_sla_expected_hours(priority: str) -> int:
    
    # Return expected SLA hours based on priority
    if priority == "High":
        return 24
    elif priority == "Medium":
        return 72
    elif priority == "Low":
        return 120
    else:
        return None

# Load Business calendar from silver layer
def load_business_calendar(calendar_path: str) -> pd.DataFrame:
    
    # Load silver calendar table
    calendar_df = pd.read_parquet(calendar_path)
    calendar_df["date"] = pd.to_datetime(calendar_df["date"])
    return calendar_df

# Business hours calculation using calendar table
def calculate_business_hours(
    created_at,
    resolved_at,
    calendar_df: pd.DataFrame,
    business_start_hour: int = 0,
    business_end_hour: int = 24,            # 24 means midnight of the following day
) -> float:

    # Calculate business hours between created_at and resolved_at using silver calendar table
    if pd.isna(created_at) or pd.isna(resolved_at):
        return 0.0

    start_dt = pd.to_datetime(created_at, utc=True)
    end_dt = pd.to_datetime(resolved_at, utc=True)

    if end_dt <= start_dt:
        return 0.0

    start_date = start_dt.date()
    end_date = end_dt.date()

    calendar_df = calendar_df.copy()
    calendar_df["date"] = pd.to_datetime(calendar_df["date"]).dt.date

    # Filter relevant dates from the calendar
    mask = (
        (calendar_df["date"] >= start_date) &
        (calendar_df["date"] <= end_date) &
        (calendar_df["business_day"] == 1)
    )

    business_days = calendar_df.loc[mask]
    total_hours = 0.0

    for _, row in business_days.iterrows():
        current_date = row["date"]
 
        # Create timestamps with UTC timezone for the start and end of the workday
        if business_end_hour == 24:
            business_end = pd.Timestamp(current_date, tz='UTC') + pd.Timedelta(days=1)
        else:
            business_end = pd.Timestamp(
                year=current_date.year,
                month=current_date.month,
                day=current_date.day,
                hour=business_end_hour,
                minute=0,
                second=0,
                tz='UTC'
            )
        
        business_start = pd.Timestamp(
            year=current_date.year,
            month=current_date.month,
            day=current_date.day,
            hour=business_start_hour,
            minute=0,
            second=0,
            tz='UTC'
        )

        # Calculate the overlapping interval
        interval_start = max(start_dt, business_start)
        interval_end = min(end_dt, business_end)

        if interval_end > interval_start:
            diff = interval_end - interval_start
            total_hours += diff.total_seconds() / 3600

    return round(total_hours, 2)

# SLA compliance using calendar table
def check_sla_compliance(
    created_at: str,
    resolved_at: str,
    priority: str,
    calendar_df: pd.DataFrame,
) -> dict:
    
    #Evaluate SLA compliance using calendar table.
    resolution_hours = calculate_business_hours(
        created_at=created_at,
        resolved_at=resolved_at,
        calendar_df=calendar_df,
    )

    sla_expected_hours = get_sla_expected_hours(priority)

    if sla_expected_hours is None:
        is_sla_met = None
    else:
        is_sla_met = resolution_hours <= sla_expected_hours

    return {
        "resolution_hours": resolution_hours,
        "sla_expected_hours": sla_expected_hours,
        "is_sla_met": is_sla_met,
    }