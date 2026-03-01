# Import library and define file source
import pandas as pd
from pathlib import Path

dir = Path(__file__).resolve().parent
file = dir.parent.parent / "data" / "bronze" / "bronze_issues.json"

# Read the JSON file. It was necessary to set the read type to Series, because setting it to Frame was returning an error
read_series = pd.read_json(file, typ='series')

# Get list of key issues
df = pd.json_normalize(read_series.loc['issues'])

# Save bronze table
out_path = dir.parent.parent / "data" / "bronze"
out_path.mkdir(parents=True, exist_ok=True)
file_path = out_path / "bronze_issues.parquet"

df.to_parquet(file_path, index=False)