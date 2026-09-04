# scripts/export_mock_responses.py
"""
Script to capture, scrub, and structure raw API response bodies from dlt sources
into unit test mock data files under tests/mock_data.

Output structures follow the 3-file pattern defined in tests/TESTS.md:
  - run1-page1: 3 records with pagination indicators (e.g. next_cursor, paging.next)
  - run1-page2: 2 records with pagination indicators removed/cleared
  - run2: 1 record with advanced timestamps/cursors for incremental testing

Prerequisites
-------------
- API credentials for the respective sources configured in environment variables or dlt secrets.
- Sources with missing credentials are automatically skipped with a log warning.

Usage
-----
```bash
# Export mock responses for all configured sources
python scripts/export_mock_responses.py

# Export mock responses for a specific source
python scripts/export_mock_responses.py --source hubspot

# Perform a dry run without writing files to disk
python scripts/export_mock_responses.py --dry-run
```
"""

import argparse
import copy
import json
import logging
import os
import sys

from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
import dlt

sys.path.insert(0, str(Path(__file__).parents[1]))

from scripts.scrub_data import scrub_api_response

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

MOCK_DATA_DIR = Path(__file__).parents[1] / "tests" / "mock_data"


class ResponseCaptureSession(requests.Session):
    """Custom requests.Session that captures raw JSON response bodies per endpoint URL path."""

    def __init__(self) -> None:
        super().__init__()
        self.captured_responses: List[Tuple[str, Any, Dict[str, Any]]] = []

        def capture_hook(response: requests.Response, *args: Any, **kwargs: Any) -> None:
            if response.status_code == 200:
                try:
                    payload = response.json()
                    url = response.config.get("url") if hasattr(response, "config") else response.url
                    self.captured_responses.append((url, payload, getattr(response, "headers", {})))
                except Exception:
                    pass

        self.hooks["response"].append(capture_hook)


def detect_data_field(payload: Dict[str, Any]) -> Optional[str]:
    """Detect the key containing record lists in an API response payload."""
    for field in ["results", "sleep", "activities", "dataPoints"]:
        if field in payload and isinstance(payload[field], list):
            return field
    return None


def pad_records(records: List[Dict[str, Any]], target_count: int = 6) -> List[Dict[str, Any]]:
    """Ensure records list has at least target_count items by deep-copying and updating IDs."""
    if not records:
        return []
    result = copy.deepcopy(records)
    original_len = len(records)
    while len(result) < target_count:
        idx = len(result)
        sample = copy.deepcopy(records[idx % original_len])
        if "id" in sample:
            sample["id"] = f"{sample['id']}_{idx + 1}"
        if "logId" in sample:
            sample["logId"] = sample["logId"] + idx + 1
        result.append(sample)
    return result


def advance_timestamps(record: Dict[str, Any]) -> Dict[str, Any]:
    """Advance timestamp/date values in a record for incremental run2 testing."""
    rec = copy.deepcopy(record)
    date_fields = [
        "updatedAt",
        "last_edited_time",
        "lastModified",
        "dateOfSleep",
        "startTime",
        "endTime",
        "createdAt",
        "created_time",
    ]
    for key, val in rec.items():
        if key in date_fields and isinstance(val, str):
            if "202" in val or "201" in val:
                rec[key] = val.replace("2024", "2025").replace("2023", "2025")
        elif isinstance(val, dict):
            rec[key] = advance_timestamps(val)
    return rec


def process_and_split_payload(
    payload: Dict[str, Any], endpoint_name: str
) -> Dict[str, Dict[str, Any]]:
    """Split captured API response payload into run1-page1, run1-page2, and run2 structures."""
    data_field = detect_data_field(payload)
    if not data_field:
        # Single non-list response (e.g. schemas)
        scrubbed = scrub_api_response(payload)
        return {f"{endpoint_name}.json": scrubbed}

    records = payload.get(data_field, [])
    records = pad_records(records, target_count=6)

    # 1. run1-page1 (3 records + pagination indicators)
    page1_payload = copy.deepcopy(payload)
    page1_payload[data_field] = scrub_api_response(records[0:3])

    if "paging" in page1_payload and isinstance(page1_payload["paging"], dict):
        page1_payload["paging"]["next"] = {
            "link": "?after=NTI1Cg%3D%3D",
            "after": "NTI1Cg%3D%3D",
        }
    if "next_cursor" in page1_payload or "has_more" in page1_payload:
        page1_payload["next_cursor"] = "cursor_page2_token"
        page1_payload["has_more"] = True
    if "nextPageToken" in page1_payload:
        page1_payload["nextPageToken"] = "token_page2"

    # 2. run1-page2 (2 records + cleared pagination indicators)
    page2_payload = copy.deepcopy(payload)
    page2_payload[data_field] = scrub_api_response(records[3:5])

    if "paging" in page2_payload:
        page2_payload.pop("paging", None)
    if "next_cursor" in page2_payload or "has_more" in page2_payload:
        page2_payload["next_cursor"] = None
        page2_payload["has_more"] = False
    if "nextPageToken" in page2_payload:
        page2_payload.pop("nextPageToken", None)

    # 3. run2 (1 record with updated incremental timestamp/cursor)
    run2_record = advance_timestamps(records[5])
    run2_payload = copy.deepcopy(payload)
    run2_payload[data_field] = scrub_api_response([run2_record])

    if "paging" in run2_payload:
        run2_payload.pop("paging", None)
    if "next_cursor" in run2_payload or "has_more" in run2_payload:
        run2_payload["next_cursor"] = None
        run2_payload["has_more"] = False
    if "nextPageToken" in run2_payload:
        run2_payload.pop("nextPageToken", None)

    return {
        f"{endpoint_name}_run1-page1.json": page1_payload,
        f"{endpoint_name}_run1-page2.json": page2_payload,
        f"{endpoint_name}_run2.json": run2_payload,
    }


def run_hubspot_export(session: requests.Session, dry_run: bool) -> None:
    """Run HubSpot source extraction and process response bodies."""
    if not dlt.secrets.get("sources.hubspot.api_key"):
        logger.warning("Skipping HubSpot: sources.hubspot.api_key secret is not configured.")
        return

    from pipelines.sources.hubspot import hubspot_source

    logger.info("Executing HubSpot source to capture API responses...")
    pipeline = dlt.pipeline(pipeline_name="mock_export_hs", destination="duckdb")
    source = hubspot_source(session=session)
    try:
        pipeline.extract(source)
    except Exception as e:
        logger.warning(f"HubSpot extraction completed with exception: {e}")


def run_notion_export(session: requests.Session, dry_run: bool) -> None:
    """Run Notion source extraction and process response bodies."""
    if not dlt.secrets.get("sources.notion.api_key"):
        logger.warning("Skipping Notion: sources.notion.api_key secret is not configured.")
        return

    from pipelines.sources.notion import notion_source

    logger.info("Executing Notion source to capture API responses...")
    pipeline = dlt.pipeline(pipeline_name="mock_export_notion", destination="duckdb")
    source = notion_source(db_name="Habits", session=session)
    try:
        pipeline.extract(source)
    except Exception as e:
        logger.warning(f"Notion extraction completed with exception: {e}")


def run_fitbit_export(session: requests.Session, dry_run: bool) -> None:
    """Run Fitbit source extraction and process response bodies."""
    token = dlt.secrets.get("sources.fitbit.refresh_token")
    if not token:
        logger.warning("Skipping Fitbit: sources.fitbit.refresh_token secret is not configured.")
        return

    from pipelines.sources.fitbit import fitbit_source, get_fitbit_token

    try:
        access_token = get_fitbit_token()
    except Exception as e:
        logger.warning(f"Skipping Fitbit: Failed to refresh token ({e}).")
        return

    logger.info("Executing Fitbit source to capture API responses...")
    pipeline = dlt.pipeline(pipeline_name="mock_export_fitbit", destination="duckdb")
    source = fitbit_source(api_key=access_token, session=session)
    try:
        pipeline.extract(source)
    except Exception as e:
        logger.warning(f"Fitbit extraction completed with exception: {e}")


def run_google_health_export(session: requests.Session, dry_run: bool) -> None:
    """Run Google Health source extraction and process response bodies."""
    token = dlt.secrets.get("sources.google_health.refresh_token")
    if not token:
        logger.warning(
            "Skipping Google Health: sources.google_health.refresh_token is not configured."
        )
        return

    from pipelines.sources.google_health import google_health_source, get_google_health_token

    try:
        access_token = get_google_health_token()
    except Exception as e:
        logger.warning(f"Skipping Google Health: Failed to refresh token ({e}).")
        return

    logger.info("Executing Google Health source to capture API responses...")
    pipeline = dlt.pipeline(pipeline_name="mock_export_gh", destination="duckdb")
    source = google_health_source(access_token=access_token, session=session)
    try:
        pipeline.extract(source)
    except Exception as e:
        logger.warning(f"Google Health extraction completed with exception: {e}")


def save_captured_responses(
    captured: List[Tuple[str, Any, Dict[str, Any]]], dry_run: bool
) -> None:
    """Save captured response bodies to tests/mock_data in standardized format."""
    MOCK_DATA_DIR.mkdir(parents=True, exist_ok=True)
    processed_count = 0

    for url, payload, headers in captured:
        if not isinstance(payload, dict):
            continue

        endpoint_name = None
        if "hubapi.com/crm/v3/objects/" in url:
            parts = url.split("/crm/v3/objects/")[1].split("/")
            endpoint_name = f"hubspot_{parts[0]}"
        elif "api.notion.com" in url:
            endpoint_name = "notion_data_source_rows"
        elif "fitbit.com" in url:
            if "sleep" in url:
                endpoint_name = "fitbit_sleep"
            elif "activities" in url:
                endpoint_name = "fitbit_activities"
        elif "health.googleapis.com" in url:
            if "sleep" in url:
                endpoint_name = "google_health_sleep"
            elif "steps" in url:
                endpoint_name = "google_health_steps"
            elif "exercise" in url:
                endpoint_name = "google_health_exercise"

        if not endpoint_name:
            continue

        files_map = process_and_split_payload(payload, endpoint_name)
        for filename, file_payload in files_map.items():
            file_path = MOCK_DATA_DIR / filename
            if dry_run:
                logger.info(f"[DRY-RUN] Would write: {file_path}")
            else:
                logger.info(f"Writing mock response file: {file_path}")
                with open(file_path, "w", encoding="utf-8") as f:
                    json.dump(file_payload, f, indent=2, ensure_ascii=False)
            processed_count += 1

    logger.info(f"Successfully processed {processed_count} mock files.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Extract live API responses via dlt sources, scrub PII, and generate test mocks."
    )
    parser.add_argument(
        "--source",
        choices=["hubspot", "notion", "fitbit", "google_health", "all"],
        default="all",
        help="Source API to capture responses from (default: all)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Process and scrub captured responses without writing to disk",
    )
    args = parser.parse_args()

    session = ResponseCaptureSession()

    if args.source in ("hubspot", "all"):
        run_hubspot_export(session, args.dry_run)
    if args.source in ("notion", "all"):
        run_notion_export(session, args.dry_run)
    if args.source in ("fitbit", "all"):
        run_fitbit_export(session, args.dry_run)
    if args.source in ("google_health", "all"):
        run_google_health_export(session, args.dry_run)

    save_captured_responses(session.captured_responses, args.dry_run)


if __name__ == "__main__":
    main()
