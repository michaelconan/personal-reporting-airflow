"""
Notion API source factory fror dlt pipelines.

API Resources:

- `Search Pages <https://developers.notion.com/reference/post-search>`_
- `Query Data Source <https://developers.notion.com/reference/query-a-data-source>`_
"""

from typing import Any, Generator
import requests
from logging import getLogger

import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.helpers.rest_client.paginators import JSONResponseCursorPaginator
from dlt.sources import DltResource

from pipelines.common.utils import filter_fields
from pipelines import BASE_DATE

logger = getLogger(__name__)

# list[str]: JSONPath expressions to exclude attributes from the Notion source records
EXCLUDE_PATHS = [
    # Exclude standard data source metadata fields (object, type)
    # e.g., parent__type ("data_source_id"), created_by__object ("user")
    # and data source property metadata fields (id, type)
    # e.g., properties__name__type ("title"), properties__date__id ("abZq")
    # This does not exclude relation property IDs (e.g., "$.properties.relation[*].id")
    "$..object",
    "$..type",
    "$.properties.*.id",
    # Exclude data source property list paginator flags (boolean)
    # If longer lists were expected, would need transformer for additional API calls
    "$.properties.*.has_more",
    # Exclude data source property annotations (rich text)
    "$.properties..annotations",
]

DATA_SOURCE_MAP = {
    "17b04335-b725-4511-ae86-594ef725706c": "daily_habits",
    "3042e681-b85e-494e-b287-9aa80827cd81": "weekly_habits",
    "18f09eb8-3f76-809f-8140-000bbccd5616": "monthly_habits",
    "2ec09eb8-3f76-80de-85f4-000b2cd39a1f": "habit_reference",
}


def name_db_table(row: dict) -> str:
    """Generate a table name for Notion data source rows based on data source ID.

    Args:
        row (dict): A data source row containing parent data source information.

    Returns:
        str: Formatted table name using the data source mapping or ID.
    """
    db_id = row["parent"]["data_source_id"]
    suffix = DATA_SOURCE_MAP.get(db_id, db_id)
    return f"notion__data_source_{suffix}"


@dlt.source
def notion_source(
    db_name: str,
    initial_date: str | None = BASE_DATE,
    end_date: str | None = None,
    session: requests.Session | None = None,
) -> Generator[DltResource, None, None]:
    """Create a DLT source for Notion data source data.

    This function configures and returns a DLT source for extracting data source
    metadata and row data from the Notion API.

    Args:
        db_name (str): Name of the data source to search for and extract data from.
        initial_date (str, optional): Start date for data extraction in
            YYYY-MM-DD format. Defaults to `BASE_DATE`.
        end_date (str, optional): Optional end date for data extraction in
            YYYY-MM-DD format. Defaults to None.
        session (requests.Session, optional): Optional requests session for
            HTTP calls. Defaults to None.

    Yields:
        Generator[DltResource, None, None]: DLT resources configured for Notion
            data extraction.
    """
    api_key = dlt.secrets["sources.notion.api_key"]

    api_config: dict[str, Any] = {
        "client": {
            "base_url": "https://api.notion.com/v1",
            "auth": {"type": "bearer", "token": api_key},
            "headers": {
                "Notion-Version": "2025-09-03",
                "Content-Type": "application/json",
            },
        },
        "resource_defaults": {
            "write_disposition": "append",
            "endpoint": {"method": "POST"},
        },
        "resources": [
            {
                "name": "notion__data_sources",
                "max_table_nesting": 1,
                "columns": {
                    "title": {"data_type": "json"},
                    "description": {"data_type": "json"},
                },
                "processing_steps": [
                    {"map": lambda r: filter_fields(r, EXCLUDE_PATHS + ["$.properties"])}
                ],
                "endpoint": {
                    "path": "search",
                    "data_selector": "results",
                    "json": {
                        "query": db_name,
                        "filter": {"property": "object", "value": "data_source"},
                    },
                },
            }
        ],
    }

    rows_resource: dict[str, Any] = {
        "name": "notion__data_source_rows",
        "table_name": name_db_table,
        "max_table_nesting": 2,
        "columns": {"title": {"data_type": "json"}},
        "processing_steps": [{"map": lambda r: filter_fields(r, EXCLUDE_PATHS)}],
        "endpoint": {
            "path": "data_sources/{resources.notion__data_sources.id}/query",
            "data_selector": "results",
            "paginator": JSONResponseCursorPaginator(
                cursor_path="next_cursor",
                cursor_body_path="start_cursor",
                has_more_path="has_more",
            ),
            "json": {
                "filter": {
                    "timestamp": "last_edited_time",
                    "last_edited_time": {"after": "{incremental.start_value}"},
                },
                "page_size": 100,
            },
            "incremental": {
                "cursor_path": "last_edited_time",
                "initial_value": initial_date,
                "end_value": end_date,
            },
        },
    }

    if end_date:
        rows_resource["endpoint"]["json"]["filter"] = {
            "and": [
                rows_resource["endpoint"]["json"]["filter"],
                {
                    "timestamp": "last_edited_time",
                    "last_edited_time": {"before": "{incremental.end_value}"},
                },
            ],
        }
        # Add end date to incremental load range
        # rows_resource["endpoint"]["incremental"]["end_value"] = end_date

    api_config["resources"].append(rows_resource)

    if session:
        api_config["client"]["session"] = session

    yield from rest_api_resources(api_config)
