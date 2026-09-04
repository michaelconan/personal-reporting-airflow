"""
Google Health source factory moved to pipelines.sources.google_health

Prerequisite: Obtain refresh token through interactive flow.

- `Setup Health <https://developers.google.com/health/setup>`_
- Scopes: 
    - https://www.googleapis.com/auth/googlehealth.activity_and_fitness.readonly
    - https://www.googleapis.com/auth/googlehealth.sleep.readonly

API Resources:

- `List DataPoints <https://developers.google.com/health/reference/rest/v4/users.dataTypes.dataPoints/list>`_
- `Daily Roll Up DataPoints <https://developers.google.com/health/reference/rest/v4/users.dataTypes.dataPoints/dailyRollUp>`_
"""

import dlt
from dlt.sources.rest_api import rest_api_source
from dlt.sources.helpers.rest_client.paginators import JSONResponseCursorPaginator
from google.cloud import secretmanager_v1
import google.auth
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request


import requests

from pipelines import SECRET_STORE


def get_google_health_token() -> str:
    """Get a fresh Google Health API access token using the stored refresh token.

    This function requests a new access token from Google's OAuth2 endpoint
    using the refresh token stored in DLT secrets. It also updates the
    refresh token in Google Secret Manager or 1Password for future use if
    a new refresh token is returned by the endpoint.

    Returns:
        str: The new access token for Google Health API requests.

    Raises:
        requests.HTTPError: If the token refresh request fails.
    """
    creds = Credentials(
        token=None,
        refresh_token=dlt.secrets["sources.google_health.refresh_token"],
        token_uri="https://oauth2.googleapis.com/token",
        client_id=dlt.secrets["sources.google_health.client_id"],
        client_secret=dlt.secrets["sources.google_health.client_secret"],
    )

    request = Request()
    creds.refresh(request)

    new_refresh = creds.refresh_token
    if new_refresh and new_refresh != dlt.secrets.get("sources.google_health.refresh_token"):
        if SECRET_STORE == "google":
            _, project_id = google.auth.default()
            client = secretmanager_v1.SecretManagerServiceClient()
            parent = client.secret_path(project_id, "sources-google-health-refresh_token")
            client.add_secret_version(
                request={
                    "parent": parent,
                    "payload": {"data": new_refresh.encode("UTF-8")},
                }
            )
        else:
            from pipelines.common.utils import update_onepassword_item

            update_onepassword_item(
                item_name="google-health",
                vault="reporting",
                field_updates={"refresh_token": new_refresh},
            )

    return creds.token


def google_health_source(
    access_token: str,
    initial_date: str = "1970-01-01",
    end_date: str | None = None,
    session: requests.Session | None = None,
):
    initial_ts = f"{initial_date}T00:00:00Z"
    end_ts = f"{end_date}T00:00:00Z" if end_date else None
    api_config = {
        "client": {
            "base_url": "https://health.googleapis.com/v4/",
            "auth": {"type": "bearer", "token": access_token},
            "headers": {"Content-Type": "application/json"},
            "paginator": JSONResponseCursorPaginator(
                cursor_path="nextPageToken", cursor_param="pageToken"
            ),
        },
        "resource_defaults": {
            "write_disposition": "append",
            "endpoint": {"method": "GET", "data_selector": "dataPoints"},
        },
        "resources": [
            {
                "name": "google_health__sleep",
                "max_table_nesting": 2,
                "endpoint": {
                    "path": "users/me/dataTypes/sleep/dataPoints",
                    "params": {
                        "filter": 'sleep.interval.end_time >= "{incremental.start_value}" AND sleep.interval.end_time < "{incremental.end_value}"'
                    },
                    "incremental": {
                        "cursor_path": "sleep.interval.endTime",
                        "initial_value": initial_ts,
                        "end_value": end_ts,
                    },
                },
            },
            {
                "name": "google_health__steps",
                "max_table_nesting": 4,
                "endpoint": {
                    "path": "users/me/dataTypes/steps/dataPoints",
                    "params": {
                        "filter": 'steps.interval.start_time >= "{incremental.start_value}" AND steps.interval.start_time < "{incremental.end_value}"'
                    },
                    "incremental": {
                        "cursor_path": "steps.interval.startTime",
                        "initial_value": initial_ts,
                        "end_value": end_ts,
                    },
                },
            },
            {
                "name": "google_health__exercise",
                "max_table_nesting": 2,
                "endpoint": {
                    "path": "users/me/dataTypes/exercise/dataPoints",
                    "params": {
                        "filter": 'exercise.interval.civil_start_time >= "{incremental.start_value}" AND exercise.interval.civil_start_time < "{incremental.end_value}"'
                    },
                    "incremental": {
                        "cursor_path": "exercise.interval.startTime",
                        "initial_value": initial_ts[:-1],
                        "end_value": end_ts[:-1] if end_ts else None,
                    },
                },
            },
        ],
    }
    if session:
        api_config["client"]["session"] = session

    return rest_api_source(api_config)
