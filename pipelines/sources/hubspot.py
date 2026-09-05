"""
Hubspot CRM data source factory for dlt pipelines.

API Resources:

- `Schemas <https://developers.hubspot.com/docs/reference/api/crm/objects/schemas>`_
- CRM Objects:
    - `Companies <https://developers.hubspot.com/docs/api-reference/crm-companies-v3/guide>`_
    - `Contacts <https://developers.hubspot.com/docs/api-reference/crm-contacts-v3/guide>`_
    - `Deals <https://developers.hubspot.com/docs/api-reference/crm-deals-v3/guide>`_
    - `Tickets <https://developers.hubspot.com/docs/api-reference/crm-tickets-v3/guide>`_
- CRM Engagements:
    - `Calls <https://developers.hubspot.com/docs/api-reference/crm-calls-v3/guide>`_
    - `Meetings <https://developers.hubspot.com/docs/api-reference/crm-meetings-v3/guide>`_
    - `Tasks <https://developers.hubspot.com/docs/api-reference/crm-tasks-v3/guide>`_
    - `Notes <https://developers.hubspot.com/docs/api-reference/crm-notes-v3/guide>`_
    - `Communications <https://developers.hubspot.com/docs/api-reference/crm-communications-v3/guide>`_  # noqa: E501
- `Associations <https://developers.hubspot.com/docs/api-reference/crm-associations-v4/guide>`_
"""

# Base
from typing import Any, Generator
from datetime import datetime, timezone

# PyPI
import yaml
import requests

# dlt
import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.helpers.rest_client.paginators import JSONResponseCursorPaginator, SinglePagePaginator
from dlt.sources import DltResource


def iso_to_unix(iso_date: str) -> int:
    """Convert ISO date string to Unix timestamp in milliseconds."""
    if len(iso_date) == 10:
        dt = datetime.strptime(iso_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    else:
        dt = datetime.fromisoformat(iso_date.replace("Z", "+00:00"))
    return int(dt.timestamp() * 1000)


@dlt.source
def hubspot_source(
    session: requests.Session | None = None,
    initial_date: str = "1970-01-01",
    end_date: str | None = None,
) -> Generator[DltResource, None, None]:
    """Create a DLT source for HubSpot CRM data.

    This function configures and returns a DLT source for extracting contacts,
    companies, engagements, and schema data from the HubSpot CRM API.

    Args:
        session (requests.Session, optional): Optional requests session for
            HTTP calls. Defaults to None.
        initial_date (str, optional): Start date for data extraction in
            YYYY-MM-DD format. Defaults to `BASE_DATE`.
        end_date (str, optional): Optional end date for data extraction in
            YYYY-MM-DD format. Defaults to None.

    Yields:
        Generator[DltResource, None, None]: DLT resources configured for HubSpot
            data extraction.
    """
    api_key = dlt.secrets["sources.hubspot.api_key"]

    with open("pipelines/sources/hs_config.yml", "rb") as fp:
        hs_config = yaml.safe_load(fp)
    crm_objects = hs_config["objects"]

    api_config: dict[str, Any] = {
        "client": {
            "base_url": "https://api.hubapi.com/",
            "auth": {"type": "bearer", "token": api_key},
            "headers": {"Content-Type": "application/json"},
        },
        "resource_defaults": {"write_disposition": "append"},
        "resources": [],
    }
    if session:
        api_config["client"]["session"] = session

    for hs_object in crm_objects:
        object_name = hs_object["name"]
        schema_resource: dict[str, Any] = {
            "name": f"hubspot__schemas_{object_name}",
            "max_table_nesting": 1,
            "columns": {
                "required_properties": {"data_type": "json"},
                "searchable_properties": {"data_type": "json"},
                "secondary_display_properties": {"data_type": "json"},
            },
            "table_name": "hubspot__schemas",
            "endpoint": {
                "path": f"crm-object-schemas/v3/schemas/{object_name}",
                "method": "GET",
                "paginator": {"type": "single_page"},
                "data_selector": "$",
            },
            "write_disposition": "merge",
            "primary_key": "id",
        }
        api_config["resources"].append(schema_resource)

    for hs_object in crm_objects:
        resource_name = f"hubspot__{hs_object['name']}"
        object_resource: dict[str, Any] = {
            "name": resource_name,
            "endpoint": {
                "path": f"crm/v3/objects/{hs_object['name']}/search",
                "method": "POST",
                "data_selector": "results",
                "json": {
                    "limit": 100,
                    "properties": hs_object["properties"],
                    "filterGroups": [
                        {
                            "filters": [
                                {
                                    "propertyName": hs_object["filter"],
                                    "operator": "GTE",
                                    "value": "{incremental.start_value}",
                                }
                            ]
                        }
                    ],
                },
                "paginator": JSONResponseCursorPaginator(
                    cursor_path="paging.next.after", cursor_body_path="after"
                ),
                "incremental": {
                    "cursor_path": "updatedAt",
                    "initial_value": initial_date,
                    "end_value": end_date,
                    "convert": iso_to_unix,
                },
            },
        }

        if end_date:
            object_resource["endpoint"]["json"]["filterGroups"][0]["filters"].append(
                {
                    "propertyName": hs_object["filter"],
                    "operator": "LTE",
                    "value": "{incremental.end_value}",
                }
            )

        api_config["resources"].append(object_resource)

        for hs_association in hs_object.get("associations", []):
            resource_path = f"crm/v4/objects/{hs_object['name']}/{{resources.{resource_name}.id}}/associations/{hs_association}"
            api_config["resources"].append(
                {
                    "name": f"{resource_name}_to_{hs_association}",
                    "max_table_nesting": 1,
                    "columns": {"association_types": {"data_type": "json"}},
                    "endpoint": {
                        "path": resource_path,
                        "method": "GET",
                        "data_selector": "results",
                    },
                    "include_from_parent": ["id", "updatedAt"],
                }
            )

    yield from rest_api_resources(api_config)
