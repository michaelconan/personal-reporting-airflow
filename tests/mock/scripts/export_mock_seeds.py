# scripts/export_mock_seeds.py
"""
Utility script to export a sample of rows from each dbt source table
into the mock seed files used for local DuckDB testing.

The script reads dbt source definitions from the staging models, queries the
corresponding BigQuery tables (the raw schema populated by dlt), and writes the
first 5 rows to `dbt/seeds/mock_sources/<source>__<table>.csv`.

Prerequisites
------------
- Google Cloud credentials with access to the BigQuery project containing the
  raw dataset.
- `pip install google-cloud-bigquery pyyaml` (or add to your Pipfile).
- Environment variables:
    * `GCP_PROJECT_ID` – BigQuery project ID.
    * `DBT_RAW_DATASET` – Dataset name where the raw tables live (e.g.
      `raw_reporting`).

Usage
-----
```bash
python scripts/export_mock_seeds.py
```
"""

import csv
import datetime
import json
import os
from pathlib import Path
import sys
from typing import List, Tuple

from google.cloud import bigquery
import yaml

# Ensure project root is in sys.path for script imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

try:
    from tests.mock.scripts.scrub_data import apply_fakes_to_rows
except Exception:  # pragma: no cover - direct script execution fallback
    from .scrub_data import apply_fakes_to_rows

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
RAW_DATASET = os.getenv("DBT_RAW_DATASET")  # e.g. "raw_reporting"
OUTPUT_DIR = PROJECT_ROOT / "dbt" / "seeds" / "mock_sources"

if not PROJECT_ID or not RAW_DATASET:
    raise RuntimeError("Environment variables GCP_PROJECT_ID and DBT_RAW_DATASET must be set.")

# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


def load_source_definitions() -> List[Tuple[str, str, str]]:
    """Parse all *_sources.yml files under `dbt/models/staging`.

    Returns:
        A list of (source_name, table_name, identifier) tuples. If an identifier
        is not provided in the yaml, the table name is used as the identifier.
    """
    source_defs: List[Tuple[str, str, str]] = []
    staging_dir = PROJECT_ROOT / "dbt" / "models" / "staging"
    for yaml_path in staging_dir.rglob("*_sources.yml"):
        with open(yaml_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        # Example structure:
        # sources:
        #   - name: hubspot
        #     tables:
        #       - name: contacts
        #       - name: companies
        for source in data.get("sources", []):
            source_name = source.get("name")
            for table in source.get("tables", []):
                table_name = table.get("name")
                identifier = table.get("identifier") or f"{source_name}__{table_name}"
                if source_name and table_name:
                    source_defs.append((source_name, table_name, identifier))
    return source_defs


def export_table(client: bigquery.Client, source_name: str, identifier: str) -> None:
    """Export the first 5 rows of a BigQuery table to a CSV seed file.

    Args:
        client: Authenticated BigQuery client.
        source_name: Name of the source (e.g. "hubspot", "notion").
        identifier: The dbt identifier for the table (e.g. "hubspot__contacts").
    """
    bq_table = f"{PROJECT_ID}.{RAW_DATASET}.{identifier}"
    query = f"SELECT * FROM `{bq_table}` ORDER BY RAND() LIMIT 5"
    print(f"Exporting {bq_table} → {identifier}.csv")
    query_job = client.query(query)
    results = [dict(row) for row in query_job.result()]

    if not results:
        print(f"No data found for {identifier}")
        return

    apply_fakes_to_rows(results)

    def json_serial(obj):
        if isinstance(obj, (datetime.datetime, datetime.date)):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, bytes):
            return obj.decode("utf-8", errors="ignore")
        raise TypeError(f"Type {type(obj)} not serializable")

    # Ensure JSON columns are exported as valid JSON strings
    for row in results:
        for col, val in row.items():
            if isinstance(val, (dict, list)):
                row[col] = json.dumps(val, default=json_serial, ensure_ascii=False)
            elif isinstance(val, str) and (val.startswith("{") or val.startswith("[")):
                # Normalize repeated quotes in stringified JSON fields
                row[col] = val.replace('""', '"')

    out_dir = OUTPUT_DIR / source_name
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"{identifier}.csv"
    with open(out_file, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=results[0].keys())
        writer.writeheader()
        writer.writerows(results)



def main() -> None:
    client = bigquery.Client(project=PROJECT_ID)
    definitions = load_source_definitions()
    if not definitions:
        print("No source definitions found.")
        return
    for src, tbl, identifier in definitions:
        try:
            export_table(client, src, identifier)
        except Exception as e:
            print(f"Failed to export {identifier}: {e}")


if __name__ == "__main__":
    main()
