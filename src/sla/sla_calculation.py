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
    created_at: str,
    resolved_at: str,
    calendar_df: pd.DataFrame,
    business_start_hour: int = 8,
    business_end_hour: int = 18,
) -> float:
    
    #Calculate business hours using pre-generated calendar table
    start_dt = pd.to_datetime(created_at, utc=True)
    end_dt = pd.to_datetime(resolved_at, utc=True)

    if pd.isna(start_dt) or pd.isna(end_dt):
        return 0.0
    if end_dt <= start_dt:
        return 0.0

    start_date = start_dt.normalize()
    end_date = end_dt.normalize()

    # Filter relevant dates from calendar
    mask = (
        (calendar_df["date"] >= start_date) &
        (calendar_df["date"] <= end_date) &
        (calendar_df["is_business_day"] == 1)
    )

    business_days = calendar_df.loc[mask]
    total_hours = 0.0

    for _, row in business_days.iterrows():
        current_date = row["date"]

        business_start = pd.Timestamp.combine(
            current_date.date(),
            time(business_start_hour, 0),
        ).tz_localize("UTC")
        business_end = pd.Timestamp.combine(
            current_date.date(),
            time(business_end_hour, 0),
        ).tz_localize("UTC")

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