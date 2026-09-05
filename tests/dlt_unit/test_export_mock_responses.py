import json

import pytest
import requests
from dlt.sources.helpers.rest_client.paginators import SinglePagePaginator

import tests.mock.scripts.export_mock_responses as export_mock_responses


class _JsonAdapter(requests.adapters.BaseAdapter):
    def send(self, request, **kwargs):
        response = requests.Response()
        response.status_code = 200
        response.url = request.url
        response.request = request
        response._content = b'{"results": [{"id": 1}]}'
        return response

    def close(self):
        pass


def test_response_capture_session_records_each_response_once():
    """Capture at the requests send boundary without duplicating response hooks."""
    session = export_mock_responses.ResponseCaptureSession()
    session.mount("https://", _JsonAdapter())

    session.get("https://api.example.test/resource")

    assert session.captured_responses == [
        (
            "https://api.example.test/resource",
            {"results": [{"id": 1}]},
            {},
        )
    ]


def test_get_secret_uses_env_fallback(monkeypatch):
    """Secret lookups should work with both dlt and environment-based configuration."""
    monkeypatch.delenv("SOURCES__HUBSPOT__API_KEY", raising=False)
    monkeypatch.setenv("SOURCES__HUBSPOT__API_KEY", "env-token")

    assert export_mock_responses.get_secret("sources.hubspot.api_key") == "env-token"


def test_override_source_paginators_keeps_original_metadata(monkeypatch):
    """Capture uses one request while structuring retains the original dlt paths."""
    monkeypatch.setenv("SOURCES__HUBSPOT__API_KEY", "env-token")
    from pipelines.sources.hubspot import hubspot_source

    source = hubspot_source()
    metadata = {}
    export_mock_responses.collect_source_resource_configs(source, metadata)
    export_mock_responses.override_source_paginators(source)

    resource = source.resources["hubspot__contacts"]
    config = next(
        config
        for config in export_mock_responses.get_resource_config_dicts(resource)
        if config.get("paginator") is not None
    )
    assert isinstance(config["paginator"], SinglePagePaginator)
    assert metadata["hubspot__contacts"]["paginator_cursor_path"] == "paging.next.after"


def test_override_incremental_property_sets_hubspot_limit(monkeypatch):
    """The generic override updates the HubSpot incremental request limit."""
    monkeypatch.setenv("SOURCES__HUBSPOT__API_KEY", "env-token")
    from pipelines.sources.hubspot import hubspot_source

    source = hubspot_source()
    export_mock_responses.override_incremental_resource_property(source, "limit")

    resource = source.resources["hubspot__contacts"]
    config = next(
        config
        for config in export_mock_responses.get_resource_config_dicts(resource)
        if config.get("path", "").endswith("/search")
    )
    assert config["json"]["limit"] == 6


def test_hubspot_export_uses_wide_incremental_date_range(monkeypatch):
    """The live fixture capture includes the full configured historical date range."""
    monkeypatch.setenv("SOURCES__HUBSPOT__API_KEY", "env-token")
    from pipelines.sources.hubspot import hubspot_source

    source = hubspot_source(
        initial_date=export_mock_responses.HUBSPOT_EXPORT_INITIAL_DATE,
        end_date=export_mock_responses.HUBSPOT_EXPORT_END_DATE,
    )
    config = next(
        config
        for config in export_mock_responses.get_resource_config_dicts(
            source.resources["hubspot__contacts"]
        )
        if config.get("path", "").endswith("/search")
    )
    filters = config["json"]["filterGroups"][0]["filters"]
    assert filters[0]["value"] == "{incremental.start_value}"
    assert filters[1]["value"] == "{incremental.end_value}"


def test_notion_export_uses_wide_incremental_date_range(monkeypatch):
    """The live Notion fixture capture includes the full historical date range."""
    monkeypatch.setenv("SOURCES__NOTION__API_KEY", "env-token")
    from pipelines.sources.notion import notion_source

    source = notion_source(
        db_name="Habits",
        initial_date=export_mock_responses.NOTION_EXPORT_INITIAL_DATE,
        end_date=export_mock_responses.NOTION_EXPORT_END_DATE,
    )
    config = next(
        config
        for config in export_mock_responses.get_resource_config_dicts(
            source.resources["notion__data_source_rows"]
        )
        if config.get("path", "").endswith("/query")
    )
    date_filter = config["json"]["filter"]["and"]
    assert date_filter[0]["last_edited_time"]["after"] == "{incremental.start_value}"
    assert date_filter[1]["last_edited_time"]["before"] == "{incremental.end_value}"


def test_notion_export_targets_runner_data_source(monkeypatch):
    """The live export uses the same data-source query as the production runner."""
    monkeypatch.setenv("SOURCES__NOTION__API_KEY", "env-token")
    from pipelines.sources.notion import notion_source

    source = notion_source(
        db_name=export_mock_responses.NOTION_EXPORT_DATABASE_NAME,
        initial_date=export_mock_responses.NOTION_EXPORT_INITIAL_DATE,
        end_date=export_mock_responses.NOTION_EXPORT_END_DATE,
    )
    config = next(
        config
        for config in export_mock_responses.get_resource_config_dicts(
            source.resources["notion__data_sources"]
        )
        if config.get("path") == "search"
    )
    assert config["json"]["query"] == "Disciplines"


def test_override_incremental_property_sets_notion_page_size(monkeypatch):
    """The generic override updates the Notion incremental request page size."""
    monkeypatch.setenv("SOURCES__NOTION__API_KEY", "env-token")
    from pipelines.sources.notion import notion_source

    source = notion_source(db_name="Habits")
    export_mock_responses.override_incremental_resource_property(source, "page_size")

    config = next(
        config
        for config in export_mock_responses.get_resource_config_dicts(
            source.resources["notion__data_source_rows"]
        )
        if config.get("path", "").endswith("/query")
    )
    assert config["json"]["page_size"] == 6


def test_override_incremental_property_sets_google_health_page_size(monkeypatch):
    """The generic override updates the Google Health incremental page size."""
    monkeypatch.setenv("SOURCES__GOOGLE_HEALTH__REFRESH_TOKEN", "env-token")
    from pipelines.sources.google_health import google_health_source

    source = google_health_source(access_token="env-token")
    export_mock_responses.override_incremental_resource_property(source, "pageSize")

    config = next(
        config
        for config in export_mock_responses.get_resource_config_dicts(
            source.resources["google_health__exercise"]
        )
        if config.get("path", "").endswith("/dataPoints")
    )
    assert config["params"]["pageSize"] == 6


def test_notion_rows_paginator_metadata_is_retained(monkeypatch):
    """The Notion rows paginator path is retained before the runtime override."""
    monkeypatch.setenv("SOURCES__NOTION__API_KEY", "env-token")
    from pipelines.sources.notion import notion_source

    source = notion_source(db_name="Habits")
    metadata = {}
    export_mock_responses.collect_source_resource_configs(source, metadata)
    export_mock_responses.override_source_paginators(source)

    config = next(
        config
        for config in export_mock_responses.get_resource_config_dicts(
            source.resources["notion__data_source_rows"]
        )
        if config.get("path", "").endswith("/query")
    )
    assert metadata["notion__data_source_rows"]["paginator_cursor_path"] == "next_cursor"
    assert metadata["notion__data_source_rows"]["paginator_has_more_path"] == "has_more"
    assert isinstance(config["paginator"], SinglePagePaginator)
    assert metadata["notion__data_sources"]["paginator_cursor_path"] is None


def test_resources_without_cursor_metadata_are_not_split():
    """Single-response resources such as schemas stay as one JSON fixture."""
    source_map = {
        "hubspot__schemas_contacts": {
            "resource_name": "hubspot__schemas_contacts",
            "endpoint_paths": ["crm-object-schemas/v3/schemas/contacts"],
            "is_paginated_or_incremental": False,
            "incremental_cursor_path": None,
            "paginator_cursor_path": None,
        }
    }

    endpoint_name, is_paginated, cursor, paginator_path, has_more_path = (
        export_mock_responses.parse_endpoint_info(
            "https://api.hubapi.com/crm-object-schemas/v3/schemas/contacts",
            source_map,
        )
    )

    assert endpoint_name == "hubspot__schemas_contacts"
    assert is_paginated is False
    assert cursor is None
    assert paginator_path is None
    assert has_more_path is None


def test_match_resource_for_url_uses_dlt_metadata():
    """Match requests to a resource via its actual dlt endpoint metadata rather than hardcoded names."""
    sources_map = {
        "hubspot__contacts": {
            "resource_name": "hubspot__contacts",
            "endpoint_paths": ["crm/v3/objects/contacts/search"],
            "is_paginated_or_incremental": True,
            "incremental_cursor_path": "updatedAt",
            "paginator_cursor_path": "paging.next.after",
            "paginator_has_more_path": "hasMore",
        },
        "notion__data_sources": {
            "resource_name": "notion__data_sources",
            "endpoint_paths": ["search"],
            "is_paginated_or_incremental": False,
            "incremental_cursor_path": None,
        },
    }

    match = export_mock_responses.match_resource_for_url(
        "https://api.hubapi.com/crm/v3/objects/contacts/search",
        sources_map,
    )
    assert match is not None
    assert match["resource_name"] == "hubspot__contacts"

    endpoint_name, is_paginated, cursor, paginator_path, has_more_path = (
        export_mock_responses.parse_endpoint_info(
        "https://api.hubapi.com/crm/v3/objects/contacts/search",
        sources_map,
        )
    )
    assert endpoint_name == "hubspot__contacts"
    assert is_paginated is True
    assert cursor == "updatedAt"
    assert paginator_path == "paging.next.after"
    assert has_more_path == "hasMore"


def test_google_health_uses_canonical_mock_directory():
    """Google Health fixtures are grouped under the canonical source directory."""
    assert export_mock_responses.mock_system_name("google_health__sleep") == "google_health"
    assert export_mock_responses.mock_system_name("hubspot__contacts") == "hubspot"


def test_match_resource_for_url_handles_dependent_resource_templates():
    """Concrete dependent-resource URLs match dlt endpoint templates."""
    sources_map = {
        "notion__data_source_rows": {
            "resource_name": "notion__data_source_rows",
            "endpoint_paths": ["data_sources/{resources.notion__data_sources.id}/query"],
            "is_paginated_or_incremental": True,
            "incremental_cursor_path": "last_edited_time",
        }
    }

    match = export_mock_responses.match_resource_for_url(
        "https://api.notion.com/v1/data_sources/abc-123/query",
        sources_map,
    )

    assert match is not None
    assert match["resource_name"] == "notion__data_source_rows"


def test_advance_timestamps_uses_inferred_datetime_fields():
    """Timestamp advancement should derive fields from the payload rather than a fixed allowlist."""
    record = {"custom_last_seen": "2024-01-15T00:00:00Z", "name": "alpha"}

    advanced = export_mock_responses.advance_timestamps(record)

    assert advanced["custom_last_seen"].startswith("2025")
    assert advanced["name"] == "alpha"


def test_process_and_split_payload_sorts_nested_incremental_cursors():
    """Captured records are sorted before the 3/2/1 fixture split."""
    payload = {
        "dataPoints": [
            {"steps": {"interval": {"startTime": "2026-01-03T00:00:00Z"}}},
            {"steps": {"interval": {"startTime": "2026-01-01T00:00:00Z"}}},
            {"steps": {"interval": {"startTime": "2026-01-02T00:00:00Z"}}},
            {"steps": {"interval": {"startTime": "2026-01-04T00:00:00Z"}}},
            {"steps": {"interval": {"startTime": "2026-01-05T00:00:00Z"}}},
            {"steps": {"interval": {"startTime": "2026-01-06T00:00:00Z"}}},
        ]
    }

    split = export_mock_responses.process_and_split_payload(
        payload,
        "google_health__steps",
        incremental_cursor_path="steps.interval.startTime",
    )

    page1 = split["google_health__steps-run1_page1.json"]
    run2 = split["google_health__steps-run2.json"]
    assert page1["dataPoints"][0]["steps"]["interval"]["startTime"] == "2026-01-01T00:00:00Z"
    assert run2["dataPoints"][0]["steps"]["interval"]["startTime"].startswith("2027")


def test_process_and_split_payload_uses_dynamic_cursor_fields():
    """Page-1 pagination should respect the configured dlt cursor path and payload values."""
    payload = {
        "results": [
            {"id": 1, "updatedAt": "2024-03-10T00:00:00Z"},
            {"id": 2, "updatedAt": "2024-03-11T00:00:00Z"},
            {"id": 3, "updatedAt": "2024-03-12T00:00:00Z"},
            {"id": 4, "updatedAt": "2024-03-13T00:00:00Z"},
            {"id": 5, "updatedAt": "2024-03-14T00:00:00Z"},
            {"id": 6, "updatedAt": "2024-03-15T00:00:00Z"},
        ],
        "token": "abc123",
        "hasMore": True,
        "paging": {"next": {"after": "token-2"}},
    }

    split = export_mock_responses.process_and_split_payload(
        payload,
        "hubspot__contacts",
        incremental_cursor_path="updatedAt",
        paginator_cursor_path="paging.next.after",
        paginator_has_more_path="hasMore",
    )

    page1 = split["hubspot__contacts-run1_page1.json"]
    page2 = split["hubspot__contacts-run1_page2.json"]
    run2 = split["hubspot__contacts-run2.json"]

    assert page1["paging"]["next"]["after"] == "token-2"
    assert page1["hasMore"] is True

    assert "paging" not in page2
    assert page2.get("hasMore") is False

    assert run2["results"][0]["updatedAt"].startswith("2025")


def test_process_and_split_payload_scrubs_response_metadata():
    """Every generated fixture must be scrubbed, including non-record response fields."""
    payload = {
        "email": "real-person@example.com",
        "results": [{"id": 1, "name": "private name"}],
    }

    split = export_mock_responses.process_and_split_payload(
        payload,
        "hubspot__contacts",
        incremental_cursor_path="id",
    )

    assert split["hubspot__contacts-run1_page1.json"]["email"] != "real-person@example.com"


def test_save_captured_responses_writes_file_to_disk(tmp_path, monkeypatch):
    """Captured API responses should be persisted as mock files on disk."""
    monkeypatch.setattr(export_mock_responses, "MOCK_DATA_DIR", tmp_path)

    payload = {
        "results": [
            {"id": i, "updatedAt": f"2024-03-{i:02d}T00:00:00Z"} for i in range(1, 7)
        ],
        "token": "abc123",
        "hasMore": True,
        "paging": {"next": {"after": "token-2"}},
    }
    source_map = {
        "hubspot__contacts": {
            "resource_name": "hubspot__contacts",
            "endpoint_paths": ["crm/v3/objects/contacts/search"],
            "is_paginated_or_incremental": True,
            "incremental_cursor_path": "updatedAt",
            "paginator_cursor_path": "paging.next.after",
            "paginator_has_more_path": "hasMore",
        }
    }

    export_mock_responses.save_captured_responses(
        [("https://api.hubapi.com/crm/v3/objects/contacts/search", payload, {})],
        source_map,
        dry_run=False,
    )

    files = [
        tmp_path / "hubspot" / "hubspot__contacts-run1_page1.json",
        tmp_path / "hubspot" / "hubspot__contacts-run1_page2.json",
        tmp_path / "hubspot" / "hubspot__contacts-run2.json",
    ]
    for file_path in files:
        assert file_path.exists()
        assert json.loads(file_path.read_text(encoding="utf-8"))


def test_process_and_split_payload_rejects_missing_configured_pagination():
    """A paginated resource without its configured response cursor is invalid."""
    payload = {
        "total": 1,
        "results": [
            {"id": 1, "updatedAt": "2024-03-10T00:00:00Z"},
            {"id": 2, "updatedAt": "2024-03-11T00:00:00Z"},
            {"id": 3, "updatedAt": "2024-03-12T00:00:00Z"},
            {"id": 4, "updatedAt": "2024-03-13T00:00:00Z"},
            {"id": 5, "updatedAt": "2024-03-14T00:00:00Z"},
            {"id": 6, "updatedAt": "2024-03-15T00:00:00Z"},
        ],
    }

    with pytest.raises(ValueError, match="paginator cursor path"):
        export_mock_responses.process_and_split_payload(
            payload,
            "hubspot__contacts",
            incremental_cursor_path="updatedAt",
            paginator_cursor_path="paging.next.after",
        )


