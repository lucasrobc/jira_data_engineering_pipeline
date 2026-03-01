# Import libraries and modules for SLA calculation
import pandas as pd
from pathlib import Path
from sla_calculation import (
    load_business_calendar,
    check_sla_compliance,
)

# Read the files for each silver
dir = Path(__file__).resolve().parent
silver_issues_path = dir.parent.parent / "data" / "silver" / "silver_issues.parquet"
silver_calendar_path = dir.parent.parent / "data" / "silver" / "silver_calendar.parquet"

df = pd.read_parquet(silver_issues_path)
calendar_df = load_business_calendar(silver_calendar_path)

# Filter only service requests with statuses marked as "done" and "resolved"
df = df[df["status"].isin(["Done", "Resolved"])].copy()

# Read the data frame and call the function for SLA calculations
sla_results = []

for _, row in df.iterrows():
    result = check_sla_compliance(
        created_at=row["created_at"],
        resolved_at=row["resolved_at"],
        priority=row["priority"],
        calendar_df=calendar_df,
    )
    sla_results.append(result)

sla_df = pd.DataFrame(sla_results)

# Concatenate new columns from the calculation into the main table
df = pd.concat([df.reset_index(drop=True), sla_df], axis=1)

# Selection of fields for the final table – SLA per service
gold_sla_issues = df[
    [
        "issue_id",
        "issue_type",
        "assignee_name",
        "priority",
        "created_at",
        "resolved_at",
        "resolution_hours",
        "sla_expected_hours",
        "is_sla_met",
    ]
]

# Creation of the average SLA table per analyst
gold_sla_by_analyst = (
    gold_sla_issues
    .groupby("assignee_name")
    .agg(
        issue_count=("issue_id", "count"),
        avg_sla_hours=("resolution_hours", "mean"),
    )
    .reset_index()
)

# Creating an average SLA table by service type
gold_sla_by_issue_type = (
    gold_sla_issues
    .groupby("issue_type")
    .agg(
        issue_count=("issue_id", "count"),
        avg_sla_hours=("resolution_hours", "mean"),
    )
    .reset_index()
)

#
out_path = dir.parent.parent / "data" / "gold"
out_path.mkdir(parents=True, exist_ok=True)

fp_sla_issues = out_path / "gold_sla_issues.csv"
fp_sla_by_analyst = out_path / "gold_sla_by_analyst.csv"
fp_sla_by_issue_type = out_path / "gold_sla_by_issue_type.csv"

gold_sla_issues.to_csv(fp_sla_issues, index=False)
gold_sla_by_analyst.to_csv(fp_sla_by_analyst, index=False)
gold_sla_by_issue_type.to_csv(fp_sla_by_issue_type, index=False)