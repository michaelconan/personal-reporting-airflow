# scripts/export_mock_responses.py
"""
Script to capture, scrub, and structure raw API response bodies from dlt sources
into unit test mock data files under tests/mock/data.

For paginated/incremental resources (determined via dlt resource configuration):
  Output structures follow the 3-file pattern defined in tests/TESTS.md:
    - run1_page1: 3 records with the resource's pagination indicators
    - run1_page2: 2 records with pagination indicators removed/cleared
    - run2: 1 record with advanced timestamps/cursors for incremental testing

For non-paginated/non-incremental resources (e.g. schemas, metadata):
  Output is exported as a single scrubbed JSON file (<endpoint_name>.json).

Prerequisites
-------------
- API credentials for the respective sources configured in environment variables or dlt secrets.
- Sources with missing credentials are automatically skipped with a log warning.

Usage
-----
```bash
# Export mock responses for all configured sources
python tests/mock/scripts/export_mock_responses.py

# Export mock responses for a specific source
python tests/mock/scripts/export_mock_responses.py --source hubspot

# Perform a dry run without writing files to disk
python tests/mock/scripts/export_mock_responses.py --dry-run
```
"""

import argparse
import copy
import json
import logging
import os
import re
import sys

from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

import dlt
from dlt.sources.helpers.rest_client.paginators import SinglePagePaginator


PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

try:
    from tests.mock.scripts.scrub_data import scrub_api_response
except Exception:  # pragma: no cover - direct script execution fallback
    from .scrub_data import scrub_api_response

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

MOCK_DATA_DIR = PROJECT_ROOT / "tests" / "mock" / "data"
HUBSPOT_EXPORT_INITIAL_DATE = "1970-01-01"
HUBSPOT_EXPORT_END_DATE = "2100-01-01"
NOTION_EXPORT_INITIAL_DATE = "1970-01-01"
NOTION_EXPORT_END_DATE = "2100-01-01"
NOTION_EXPORT_DATABASE_NAME = "Disciplines"


def get_secret(secret_path: str) -> Optional[str]:
    """Resolve a dlt secret from either dlt config or the equivalent environment variable."""
    try:
        if hasattr(dlt, "secrets") and dlt.secrets is not None:
            value = dlt.secrets.get(secret_path)
            if value:
                return value
    except Exception:
        pass

    env_name = secret_path.upper().replace(".", "__").replace("-", "_")
    return os.getenv(env_name) or os.getenv(env_name.replace("__", "_"))


class ResponseCaptureSession(requests.Session):
    """Capture raw JSON responses at the actual network boundary used by requests.

    dlt's REST client may call Session.send directly rather than only firing the
    response hook callbacks, so recording in send() is the reliable capture point.
    """

    def __init__(self) -> None:
        super().__init__()
        self.captured_responses: List[Tuple[str, Any, Dict[str, Any]]] = []

    def send(self, request: requests.PreparedRequest, **kwargs: Any) -> requests.Response:
        """Call the parent implementation and record any successful JSON payloads."""
        response = super().send(request, **kwargs)
        if 200 <= response.status_code < 300:
            try:
                payload = response.json()
                self.captured_responses.append(
                    (response.url, payload, dict(getattr(response, "headers", {})))
                )
            except Exception:
                pass
        return response


def get_resource_config_dicts(resource: Any) -> List[Dict[str, Any]]:
    """Collect config dictionaries attached to a dlt resource's pipe steps."""
    configs: List[Dict[str, Any]] = []
    try:
        pipe = getattr(resource, "_pipe", None)
        if not pipe:
            return configs

        for step in pipe.steps:
            if hasattr(step, "__closure__") and step.__closure__:
                for cell in step.__closure__:
                    value = getattr(cell, "cell_contents", None)
                    if isinstance(value, dict):
                        configs.append(value)
            for value in getattr(step, "__dict__", {}).values():
                if isinstance(value, dict):
                    configs.append(value)
    except Exception:
        pass
    return configs


def get_resource_pagination_metadata(resource: Any) -> Dict[str, Any]:
    """Read dlt paginator/incremental metadata from the resource rather than hardcoded field names."""
    metadata = {
        "is_paginated_or_incremental": False,
        "incremental_cursor_path": None,
        "incremental_cursor_transform": None,
        "paginator_cursor_path": None,
        "paginator_cursor_param": None,
        "paginator_cursor_body_path": None,
        "paginator_has_more_path": None,
    }

    has_incremental_config = False
    try:
        incremental = getattr(resource, "incremental", None)
        if incremental is not None:
            has_incremental_config = True
            metadata["is_paginated_or_incremental"] = True
            cursor_path = getattr(incremental, "cursor_path", None)
            if cursor_path:
                metadata["incremental_cursor_path"] = cursor_path
            metadata["incremental_cursor_transform"] = getattr(incremental, "cursor_transform", None)
    except Exception:
        pass

    for config in get_resource_config_dicts(resource):
        paginator = config.get("paginator") or getattr(config.get("client"), "paginator", None)
        incremental_obj = config.get("incremental_object")
        if paginator is not None:
            for key in ("cursor_path", "cursor_param", "cursor_body_path", "has_more_path"):
                val = getattr(paginator, key, None)
                if val is not None:
                    metadata[f"paginator_{key}"] = str(val)

        if incremental_obj is not None:
            has_incremental_config = True
            cursor_path = getattr(incremental_obj, "cursor_path", None)
            if cursor_path:
                metadata["incremental_cursor_path"] = cursor_path
            metadata["incremental_cursor_transform"] = getattr(
                incremental_obj, "cursor_transform", None
            )

    metadata["is_paginated_or_incremental"] = bool(
        metadata["incremental_cursor_path"]
        or (metadata["paginator_cursor_path"] and has_incremental_config)
    )

    return metadata


def is_resource_paginated_or_incremental(resource: Any) -> bool:
    """Check dlt resource configuration to determine if it is paginated or incremental."""
    return get_resource_pagination_metadata(resource)["is_paginated_or_incremental"]


def collect_source_resource_configs(source: Any, sources_map: Dict[str, Dict[str, Any]]) -> None:
    """Populate sources_map dictionary with resource metadata derived from dlt config."""
    try:
        for res_name, resource in source.resources.items():
            metadata = get_resource_pagination_metadata(resource)
            metadata["resource_name"] = res_name
            metadata["endpoint_paths"] = []
            for config in get_resource_config_dicts(resource):
                path = config.get("path")
                if path:
                    metadata["endpoint_paths"].append(path)
            if not metadata["endpoint_paths"]:
                metadata["endpoint_paths"] = [res_name]
            sources_map[res_name] = metadata
    except Exception as e:
        logger.warning(f"Error inspecting source resource config: {e}")


def override_source_paginators(source: Any) -> None:
    """Use one API request per resource while exporting raw response fixtures."""
    def replace_paginators(value: Any) -> None:
        if isinstance(value, dict):
            if value.get("paginator") is not None:
                value["paginator"] = SinglePagePaginator()
            client = value.get("client")
            if client is not None and getattr(client, "paginator", None) is not None:
                client.paginator = SinglePagePaginator()
            for child in value.values():
                replace_paginators(child)
        elif isinstance(value, list):
            for child in value:
                replace_paginators(child)

    try:
        for resource in source.resources.values():
            for config in get_resource_config_dicts(resource):
                replace_paginators(config)
    except Exception as e:
        raise RuntimeError("Unable to override source paginators for mock export") from e


def override_incremental_resource_property(
    source: Any, property_name: str, value: int = 6
) -> None:
    """Override an endpoint request property on every incremental dlt resource."""
    try:
        for resource in source.resources.values():
            metadata = get_resource_pagination_metadata(resource)
            if not metadata["incremental_cursor_path"]:
                continue
            for config in get_resource_config_dicts(resource):
                for request_key in ("params", "json"):
                    request = config.get(request_key)
                    if isinstance(request, dict) and property_name in request:
                        request[property_name] = value
    except Exception as e:
        raise RuntimeError(
            f"Unable to override incremental resource property {property_name!r}"
        ) from e


def endpoint_name_from_resource_name(resource_name: str) -> str:
    """Return the dlt resource name used as the mock filename prefix."""
    return resource_name


def mock_system_name(endpoint_name: str) -> str:
    """Return the canonical mock-data directory for an exported endpoint."""
    return endpoint_name.split("__", 1)[0] if "__" in endpoint_name else "misc"


def match_resource_for_url(url: str, sources_map: Dict[str, Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Match a response URL to a source resource using the dlt-configured endpoint path patterns."""
    url_path = url.split("?", 1)[0]
    best_match: Optional[Dict[str, Any]] = None
    best_score = -1

    for meta in sources_map.values():
        for endpoint_path in meta.get("endpoint_paths", []):
            path_parts = re.split(r"(\{[^}]+\})", endpoint_path)
            pattern = "".join(
                "[^/]+" if part.startswith("{") and part.endswith("}") else re.escape(part)
                for part in path_parts
                if part
            )
            regex = re.compile(rf"(^|/){pattern}($|/)" )
            if regex.search(url_path):
                score = len(endpoint_path)
                if score > best_score:
                    best_match = meta
                    best_score = score

    return best_match


def detect_data_field(payload: Dict[str, Any]) -> Optional[str]:
    """Detect the key containing record lists in an API response payload."""
    for field in ["results", "sleep", "activities", "dataPoints"]:
        if field in payload and isinstance(payload[field], list):
            return field
    return None


def get_nested_value(data: Any, path: Optional[str]) -> Any:
    """Return a nested value from a dlt-configured dotted path."""
    if not path:
        return None
    current = data
    for part in path.split("."):
        if not isinstance(current, dict):
            return None
        if part not in current:
            return None
        current = current[part]
    return current


def set_nested_value(data: Any, path: Optional[str], value: Any) -> None:
    """Set a value at a dlt-configured dotted path."""
    if not path:
        return
    parts = path.split(".")
    current = data
    for part in parts[:-1]:
        if not isinstance(current, dict):
            return
        current = current.setdefault(part, {})
    if isinstance(current, dict):
        current[parts[-1]] = value


def delete_nested_value(data: Any, path: Optional[str]) -> None:
    """Delete a value at a dlt-configured dotted path."""
    if not path or not isinstance(data, dict):
        return
    parts = path.split(".")
    if len(parts) == 1:
        data.pop(parts[0], None)
        return

    child = data.get(parts[0])
    delete_nested_value(child, ".".join(parts[1:]))
    if isinstance(child, dict) and not child:
        data.pop(parts[0], None)


def update_pagination_fields(
    payload: Dict[str, Any],
    *,
    endpoint_name: str,
    page_1: bool,
    paginator_cursor_path: Optional[str] = None,
    paginator_has_more_path: Optional[str] = None,
) -> None:
    """Validate or remove fields at paths configured by the dlt paginator."""
    if not paginator_cursor_path:
        return

    cursor_value = get_nested_value(payload, paginator_cursor_path)
    if page_1:
        if cursor_value is None:
            raise ValueError(
                f"Captured response is missing the dlt paginator cursor path "
                f"{paginator_cursor_path!r} for resource {endpoint_name!r}"
            )
        return

    delete_nested_value(payload, paginator_cursor_path)
    if paginator_has_more_path:
        set_nested_value(payload, paginator_has_more_path, False)


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


def sort_records_by_cursor(
    records: List[Dict[str, Any]], cursor_path: Optional[str]
) -> List[Dict[str, Any]]:
    """Order captured records chronologically using the dlt incremental cursor."""
    if not cursor_path:
        return records

    keyed_records = [
        (get_nested_value(record, cursor_path), index, record)
        for index, record in enumerate(records)
    ]
    if any(value is None for value, _, _ in keyed_records):
        return records
    try:
        ordered = sorted(keyed_records, key=lambda item: (item[0], item[1]))
    except TypeError:
        return records
    return [record for _, _, record in ordered]


def looks_like_datetime_value(value: Any) -> bool:
    """Heuristic for ISO-date, ISO-datetime, and epoch-like values used by API responses."""
    if not isinstance(value, str):
        return False
    stripped = value.strip()
    if not stripped:
        return False
    if stripped.isdigit() or (stripped.startswith("-") and stripped[1:].isdigit()):
        return True
    return any(token in stripped.lower() for token in ("-", ":", "t", "z", "202", "201"))


def infer_timestamp_keys(value: Any, prefix: str = "") -> List[str]:
    """Recursively discover date/time-like keys in a payload instead of hardcoding a field list."""
    keys: List[str] = []
    if isinstance(value, dict):
        for key, item in value.items():
            full_key = f"{prefix}.{key}" if prefix else key
            if looks_like_datetime_value(item):
                keys.append(full_key)
            keys.extend(infer_timestamp_keys(item, prefix=full_key))
    elif isinstance(value, list):
        for item in value:
            keys.extend(infer_timestamp_keys(item, prefix=prefix))
    return keys


def advance_timestamps(record: Dict[str, Any], cursor_path: Optional[str] = None) -> Dict[str, Any]:
    """Advance timestamp/date values in a record using the resource cursor field when available."""
    rec = copy.deepcopy(record)
    candidate_keys = set(infer_timestamp_keys(rec))

    if cursor_path:
        cursor_leaf = cursor_path.split(".")[-1]
        candidate_keys.add(cursor_leaf)
        candidate_keys.add(cursor_leaf.replace("__", ""))

    def _should_advance(key: str) -> bool:
        key_lower = key.lower()
        if key in candidate_keys:
            return True
        tail = key_lower.split(".")[-1]
        return tail.startswith("date") or tail.startswith("time") or tail.startswith("updated") or tail.startswith("created") or tail.startswith("modified") or tail.startswith("edited") or tail.startswith("start") or tail.startswith("end")

    for key, val in rec.items():
        if isinstance(val, dict):
            rec[key] = advance_timestamps(val, cursor_path=cursor_path)
        elif isinstance(val, str) and _should_advance(key) and looks_like_datetime_value(val):
            if "202" in val or "201" in val:
                rec[key] = re.sub(
                    r"20\d{2}",
                    lambda match: str(int(match.group()) + 1),
                    val,
                    count=1,
                )
    return rec


def process_and_split_payload(
    payload: Dict[str, Any],
    endpoint_name: str,
    is_paginated_or_incremental: bool = True,
    incremental_cursor_path: Optional[str] = None,
    paginator_cursor_path: Optional[str] = None,
    paginator_has_more_path: Optional[str] = None,
) -> Dict[str, Dict[str, Any]]:
    """Process payload. If resource is paginated/incremental, split into 3-file pattern.

    Otherwise export as single scrubbed JSON file.
    """
    scrubbed_payload = scrub_api_response(payload)

    if not is_paginated_or_incremental:
        return {f"{endpoint_name}.json": scrubbed_payload}

    data_field = detect_data_field(payload)
    if not data_field:
        return {f"{endpoint_name}.json": scrubbed_payload}

    records = payload.get(data_field, [])
    records = pad_records(records, target_count=6)
    records = sort_records_by_cursor(records, incremental_cursor_path)
    if len(records) < 6:
        raise ValueError(
            f"Captured paginated resource {endpoint_name!r} contains no records to structure"
        )

    # 1. run1_page1 (3 records + pagination indicators)
    page1_payload = copy.deepcopy(scrubbed_payload)
    page1_payload[data_field] = scrub_api_response(records[0:3])
    update_pagination_fields(
        page1_payload,
        endpoint_name=endpoint_name,
        page_1=True,
        paginator_cursor_path=paginator_cursor_path,
        paginator_has_more_path=paginator_has_more_path,
    )

    # 2. run1_page2 (2 records + cleared pagination indicators)
    page2_payload = copy.deepcopy(scrubbed_payload)
    page2_payload[data_field] = scrub_api_response(records[3:5])
    update_pagination_fields(
        page2_payload,
        endpoint_name=endpoint_name,
        page_1=False,
        paginator_cursor_path=paginator_cursor_path,
        paginator_has_more_path=paginator_has_more_path,
    )

    # 3. run2 (1 record with updated incremental timestamp/cursor)
    run2_record = advance_timestamps(records[5], cursor_path=incremental_cursor_path)
    run2_payload = copy.deepcopy(scrubbed_payload)
    run2_payload[data_field] = scrub_api_response([run2_record])
    update_pagination_fields(
        run2_payload,
        endpoint_name=endpoint_name,
        page_1=False,
        paginator_cursor_path=paginator_cursor_path,
        paginator_has_more_path=paginator_has_more_path,
    )

    return {
        f"{endpoint_name}-run1_page1.json": page1_payload,
        f"{endpoint_name}-run1_page2.json": page2_payload,
        f"{endpoint_name}-run2.json": run2_payload,
    }


def parse_endpoint_info(
    url: str, sources_map: Dict[str, Dict[str, Any]]
) -> Tuple[Optional[str], bool, Optional[str], Optional[str], Optional[str]]:
    """Resolve the response to a dlt resource name and pagination metadata without hardcoded source branches."""
    matched = match_resource_for_url(url, sources_map)
    if not matched:
        return None, True, None, None, None

    resource_name = matched.get("resource_name")
    if not resource_name:
        return None, True, None, None, None

    endpoint_name = endpoint_name_from_resource_name(resource_name)
    return (
        endpoint_name,
        matched.get("is_paginated_or_incremental", True),
        matched.get("incremental_cursor_path"),
        matched.get("paginator_cursor_path"),
        matched.get("paginator_has_more_path"),
    )


def run_hubspot_export(
    session: requests.Session, sources_map: Dict[str, bool], dry_run: bool
) -> None:
    """Run HubSpot source extraction and process response bodies."""
    if not get_secret("sources.hubspot.api_key"):
        logger.warning("Skipping HubSpot: sources.hubspot.api_key secret is not configured.")
        return

    from pipelines.sources.hubspot import hubspot_source

    logger.info("Executing HubSpot source to capture API responses...")
    pipeline = dlt.pipeline(pipeline_name="mock_export_hs", destination="duckdb")
    source = hubspot_source(
        session=session,
        initial_date=HUBSPOT_EXPORT_INITIAL_DATE,
        end_date=HUBSPOT_EXPORT_END_DATE,
    )
    collect_source_resource_configs(source, sources_map)
    override_incremental_resource_property(source, "limit")
    override_source_paginators(source)
    try:
        pipeline.extract(source)
    except Exception as e:
        logger.warning(f"HubSpot extraction completed with exception: {e}")

    try:
        for resource in source.resources.values():
            resource = resource.with_resources(resource.name)
    except Exception:
        pass


def run_notion_export(
    session: requests.Session, sources_map: Dict[str, bool], dry_run: bool
) -> None:
    """Run Notion source extraction and process response bodies."""
    if not get_secret("sources.notion.api_key"):
        logger.warning("Skipping Notion: sources.notion.api_key secret is not configured.")
        return

    from pipelines.sources.notion import notion_source

    logger.info("Executing Notion source to capture API responses...")
    pipeline = dlt.pipeline(pipeline_name="mock_export_notion", destination="duckdb")
    source = notion_source(
        db_name=NOTION_EXPORT_DATABASE_NAME,
        initial_date=NOTION_EXPORT_INITIAL_DATE,
        end_date=NOTION_EXPORT_END_DATE,
        session=session,
    )
    collect_source_resource_configs(source, sources_map)
    override_incremental_resource_property(source, "page_size")
    override_source_paginators(source)
    try:
        pipeline.extract(source)
    except Exception as e:
        logger.warning(f"Notion extraction completed with exception: {e}")

    try:
        sorted(source.resources)
    except Exception:
        pass


def run_fitbit_export(
    session: requests.Session, sources_map: Dict[str, bool], dry_run: bool
) -> None:
    """Run Fitbit source extraction and process response bodies."""
    token = get_secret("sources.fitbit.refresh_token")
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
    collect_source_resource_configs(source, sources_map)
    override_source_paginators(source)
    try:
        pipeline.extract(source)
    except Exception as e:
        logger.warning(f"Fitbit extraction completed with exception: {e}")

    try:
        source.resources
    except Exception:
        pass


def run_google_health_export(
    session: requests.Session, sources_map: Dict[str, bool], dry_run: bool
) -> None:
    """Run Google Health source extraction and process response bodies."""
    token = get_secret("sources.google_health.refresh_token")
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
    collect_source_resource_configs(source, sources_map)
    override_incremental_resource_property(source, "pageSize")
    override_source_paginators(source)
    try:
        pipeline.extract(source)
    except Exception as e:
        logger.warning(f"Google Health extraction completed with exception: {e}")

    try:
        list(source.resources)
    except Exception:
        pass


def save_captured_responses(
    captured: List[Tuple[str, Any, Dict[str, Any]]],
    sources_map: Dict[str, Dict[str, Any]],
    dry_run: bool,
) -> None:
    """Save captured response bodies to tests/mock/data/[system] in standardized format."""
    processed_count = 0

    for url, payload, headers in captured:
        if not isinstance(payload, dict):
            continue

        (
            endpoint_name,
            is_paginated_or_incremental,
            incremental_cursor_path,
            paginator_cursor_path,
            paginator_has_more_path,
        ) = parse_endpoint_info(
            url, sources_map
        )
        if not endpoint_name:
            continue

        system_name = mock_system_name(endpoint_name)
        target_dir = MOCK_DATA_DIR / system_name
        target_dir.mkdir(parents=True, exist_ok=True)

        files_map = process_and_split_payload(
            payload,
            endpoint_name,
            is_paginated_or_incremental=is_paginated_or_incremental,
            incremental_cursor_path=incremental_cursor_path,
            paginator_cursor_path=paginator_cursor_path,
            paginator_has_more_path=paginator_has_more_path,
        )
        for filename, file_payload in files_map.items():
            file_path = target_dir / filename
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
    sources_map: Dict[str, Dict[str, Any]] = {}

    if args.source in ("hubspot", "all"):
        run_hubspot_export(session, sources_map, args.dry_run)
    if args.source in ("notion", "all"):
        run_notion_export(session, sources_map, args.dry_run)
    if args.source in ("fitbit", "all"):
        run_fitbit_export(session, sources_map, args.dry_run)
    if args.source in ("google_health", "all"):
        run_google_health_export(session, sources_map, args.dry_run)

    save_captured_responses(session.captured_responses, sources_map, args.dry_run)


if __name__ == "__main__":
    main()
