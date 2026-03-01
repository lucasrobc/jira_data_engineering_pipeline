# Python Data Engineering Challenge – JIRA SLA Pipeline

This project implements a data pipeline to process and analyze Service Level Agreement (SLA) compliance for JIRA issues. The pipeline follows a medallion architecture (Bronze, Silver, Gold) and calculates SLA based on a business day calendar.

## Project Execution Instructions

Follow the steps below to set up the environment and run the pipeline.

### Prerequisites

- **Python 3.8+** installed.
- **Git** (for cloning the repository, if applicable).
- Using a virtual environment (`venv`) is recommended.

### Step-by-Step Guide

1.  **Clone the repository**
    ```bash
    git clone <your-repository-url>
    cd <repository-name>
2.  **Create and activate a virtual environment**
# Linux/macOS
    ```bash
    python3 -m venv venv
    source venv/bin/activate 
    ```
# Windows
    ```bash
    python -m venv venv
    venv\Scripts\activate
    ```
#  **Install dependencies**
    ```bash
    pip install -r requirements.txt
    ```
#  **Prepare the input data**
Place the raw JSON file containing the JIRA issue data in the data/bronze/ directory.
Ensure the file is named bronze_issues.json, as expected by the ingestion script.
#  **Execute the pipeline**
Navigate to the src/ folder and run the scripts in the correct order:
- cd src/
    - 5.1. Ingest raw data (Bronze layer)
        - bronze/ingest_bronze.py
    - 5.2. Transform data (Silver layer)
        - silver/transform_silver_issues.py
        - silver/calendar_utils.py
    - 5.3. Build analytical tables (Gold layer)
        - gold/build_gold_sla.py
#  **Execute the pipeline**
The final results (.csv files) will be available in the data/gold/ directory.

### Pipeline Architecture
The pipeline is built following the Medallion Architecture pattern (Bronze, Silver, Gold), ensuring a clear separation of responsibilities in each layer.
- **Bronze Layer** (ingest_bronze.py):
    - Function: Ingestion of raw data.
    - Input: data/bronze/bronze_issues.json file.
    - Processing: Reads the JSON file, normalizes the nested structure of the 'issues' list, and persists the data in Parquet format at data/bronze/bronze_issues.parquet. The Parquet format is more efficient for subsequent steps.
- **Silver Layer** (transform_silver_issues.py and calendar_utils.py):
    - Function: Data cleansing, standardization, and enrichment.
    - Processing (transform_silver_issues.py):
        1. Reads data from the Bronze layer.
        2. Explodes the nested assignee and timestamps columns.
        3. Standardizes column names to snake_case (e.g., assignee_email).
        4. Converts date/time strings to datetime type with UTC timezone.
        5. Handles invalid or out-of-spec values (e.g., 2026-02-30T25:61:00Z) and removes records with invalid dates.
        6. Saves the result to data/silver/silver_issues.parquet.
- **Processing** (calendar_utils.py):
    1. Generates a calendar table with all days from 2025-01-01 to the current date.
    2. Identifies weekends and Brazilian national holidays (using the holidays library).
    3. Creates the business_day column to mark working days.
    4. Saves the table to data/silver/silver_calendar.parquet.
- **Gold Layer** (build_gold_sla.py and sla_calculation.py):
    - Function: Aggregation and calculation of business metrics (SLA).
    - Processing (build_gold_sla.py):
        1. Reads the Silver layer tables (silver_issues and silver_calendar).
        2. Filters issues with status "Done" or "Resolved".
        3. For each issue, calls the check_sla_compliance function from the sla_calculation.py module to calculate resolution hours and check SLA compliance.
        4. Generates three analytical reports in CSV format inside the data/gold/ directory:
            - gold_sla_issues.csv: Per-issue details.
            - gold_sla_by_analyst.csv: Summary by analyst.
            - gold_sla_by_issue_type.csv: Summary by issue type.

### SLA Calculation Logic
The SLA calculation is performed by the sla_calculation.py module and considers only business days and hours, as defined in the silver_calendar table.
- **Defining Expected SLA** (get_sla_expected_hours):
    - Based on the issue's priority, the maximum number of business hours allowed for resolution is defined:
        High: 24 hours
        Medium: 72 hours
        Low: 120 hours
- **Calculating Resolution Hours** (calculate_business_hours):
    - The function iterates day by day between the issue's creation (created_at) and resolution (resolved_at) dates.
    - For each day that is a business day (Monday to Friday, not a holiday), it calculates the intersection between the working period (00:00 to 24:00 UTC) and the time the issue was open on that day.
    - The sum of these hours, rounded to two decimal places, results in the total resolution_hours.
- **Compliance Check** (check_sla_compliance):
    - Compares the calculated resolution hours with the expected hours.
    - If resolution_hours <= sla_expected_hours, the SLA was met (is_sla_met is True).

Claro! Aqui está o dicionário de dados em um formato markdown mais limpo e compreensível:

---

## Data Dictionary

### `gold_sla_issues.csv`
Per-issue SLA details.

| Column | Description | Type |
| :--- | :--- | :--- |
| `issue_id` | Unique issue identifier. | String |
| `issue_type` | Type of the issue (e.g., Bug, Task). | String |
| `assignee_name` | Name of the responsible analyst. | String |
| `priority` | Issue priority (High, Medium, Low). | String |
| `created_at` | Issue creation timestamp (UTC). | Datetime |
| `resolved_at` | Issue resolution timestamp (UTC). | Datetime |
| `resolution_hours` | Total business hours to resolution. | Float |
| `sla_expected_hours` | Expected business hours based on priority. | Integer |
| `is_sla_met` | Indicates if the SLA was met (True/False). | Boolean |

---

### `gold_sla_by_analyst.csv`
Performance summary by analyst.

| Column | Description | Type |
| :--- | :--- | :--- |
| `assignee_name` | Analyst name. | String |
| `issue_count` | Total number of issues resolved by the analyst. | Integer |
| `avg_sla_hours` | Average business hours to resolution for the analyst. | Float |

---

### `gold_sla_by_issue_type.csv`
Performance summary by issue type.

| Column | Description | Type |
| :--- | :--- | :--- |
| `issue_type` | Issue type. | String |
| `issue_count` | Total number of issues of that type. | Integer |
| `avg_sla_hours` | Average business hours to resolution for that type. | Float |

---