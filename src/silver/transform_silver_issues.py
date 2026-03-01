# Import libraries and set the file location for reading
import pandas as pd
from pathlib import Path

dir = Path(__file__).resolve().parent
file = dir.parent.parent / "data" / "bronze" / "bronze_issues.parquet"

# Read file and store bronze table
bronze_archive = pd.read_parquet(file)
df = pd.DataFrame(bronze_archive)

# Explode the columns that are the assignee list and timestamps, and expand the dictionaries into columns
df_silver = df.copy()

df_silver = df_silver.explode('assignee')
df_silver = df_silver.explode('timestamps')

assignee_colunms = pd.json_normalize(df_silver['assignee'])
timestamps_colunms = pd.json_normalize(df_silver['timestamps'])

# Merge all columns
df = pd.concat(
    [
        df_silver.drop(columns=['assignee', 'timestamps']),
        assignee_colunms,
        timestamps_colunms
    ],
    axis=1
)

# Handling non-standard records to enable string conversion to date and time
df['created_at'] = df['created_at'].replace('2026-02-30T25:61:00Z','2026-02-28T23:59:59Z')
df['resolved_at'] = df['resolved_at'].replace('not_a_date',None)

# String conversion to UTC date and time
df['created_at'] = pd.to_datetime(df['created_at'], utc=True)
df['resolved_at'] = pd.to_datetime(df['resolved_at'], utc=True)

# Rename the column names
df.columns = ['issue_id','issue_type','status','priority','assignee_email','assignee_id','assignee_name','created_at','resolved_at']

# Deleting the invalid creation date is something we discussed, but after re-evaluating, 
# I understand it's just unnecessary information that should be removed from the data frame
is_invalid_value = pd.Timestamp("2026-02-28 23:59:59+00:00")
df = df[df["created_at"] != is_invalid_value]

# Save silver table
out_path = dir.parent.parent / "data" / "silver"
out_path.mkdir(parents=True, exist_ok=True)
file_path = out_path / "silver_issues.parquet"

df.to_parquet(file_path, index=False)
