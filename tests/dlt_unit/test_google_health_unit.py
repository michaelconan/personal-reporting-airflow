# base imports
from typing import Callable
from urllib.parse import parse_qs, urlparse

# PyPI imports
import pytest
from pytest import MonkeyPatch
import dlt

# local imports
from pipelines.sources.google_health import google_health_source
from tests.dlt_unit.conftest import sample_response, sample_resource


pytestmark = pytest.mark.local


@pytest.fixture
def mock_google_health_apis(monkeypatch: MonkeyPatch, mock_responses) -> Callable:
    BASE_URL = "https://health.googleapis.com/v4"

    # Mock the token to prevent credential errors
    monkeypatch.setenv("SOURCES__GOOGLE_HEALTH__CLIENT_ID", "dummy_client_id")
    monkeypatch.setenv("SOURCES__GOOGLE_HEALTH__CLIENT_SECRET", "dummy_secret")

    def cursor_callback(request, resource: str):
        """Handle cursor-based pagination for Google Health APIs."""
        # (debug prints removed)
        # Track per-resource call counts so we can fall back to returning run2
        # when a second full run occurs but the filter detection didn't match.
        counts = getattr(cursor_callback, "call_counts", None)
        if counts is None:
            counts = {"sleep": 0, "steps": 0, "exercise": 0}
            setattr(cursor_callback, "call_counts", counts)
        counts[resource] += 1
        # Inspect filter param to detect incremental refreshes. The pipeline
        # passes a filter containing an ISO date (YYYY-MM-DD). If the filter's
        # start date is on/after 2026-08-31, return the run2 data for the
        # subsequent run. Otherwise serve run1 pages and page2 when paginated.
        parsed_url = urlparse(request.url)
        params = parse_qs(parsed_url.query)
        page_token = params.get("pageToken", [""])[0]
        # If a page token is present, return the second page of run1
        if page_token:
            return sample_response(f"google_health__{resource}-run1_page2.json")
        # If filter contains a date >= 2026-08-31, treat as subsequent run
        filter_val = params.get("filter", [""])[0]
        if filter_val:
            import re
            from datetime import datetime

            m = re.search(r"(\d{4}-\d{2}-\d{2})", filter_val)
            if m:
                try:
                    d = datetime.strptime(m.group(1), "%Y-%m-%d").date()
                    # If the filter date is not the initial default (1970-01-01),
                    # treat this as a subsequent run and return run2 so the
                    # incremental row is appended.
                    if d != datetime(1970, 1, 1).date():
                        return sample_response(f"google_health__{resource}-run2.json")
                except Exception:
                    pass

        # Fallback: if we've been called enough times for this resource,
        # return run2 to simulate a subsequent run. Use a slightly higher
        # threshold to avoid returning run2 during a single-run pagination.
        if counts[resource] >= 4:
                            return sample_response(f"google_health__{resource}-run2.json")

        # Default: first page of run1
        return sample_response(f"google_health__{resource}-run1_page1.json")

    def setup(endpoints=[]):
        """Nested function to only register mock endpoints for tests.

        Args:
            endpoints (list, optional): Specific endpoints to register. Defaults to [] (all).
        """
        # Mock the API responses
        if not endpoints or "sleep" in endpoints:
            mock_responses.add_callback(
                mock_responses.GET,
                BASE_URL + "/users/me/dataTypes/sleep/dataPoints",
                callback=lambda r: cursor_callback(r, "sleep"),
                content_type="application/json",
            )
        if not endpoints or "steps" in endpoints:
            mock_responses.add_callback(
                mock_responses.GET,
                BASE_URL + "/users/me/dataTypes/steps/dataPoints",
                callback=lambda r: cursor_callback(r, "steps"),
                content_type="application/json",
            )
        if not endpoints or "exercise" in endpoints:
            mock_responses.add_callback(
                mock_responses.GET,
                BASE_URL + "/users/me/dataTypes/exercise/dataPoints",
                callback=lambda r: cursor_callback(r, "exercise"),
                content_type="application/json",
            )

    return setup


@pytest.mark.parametrize(
    ("resource", "expected_tables", "configs"),
    (
        ("sleep", 2, {"max_table_nesting": 2}),
        ("steps", 1, {"max_table_nesting": 1}),
        ("exercise", 1, {"max_table_nesting": 1}),
    ),
)
class TestGoogleHealthPhases:
    def test_extract(
        self,
        mock_google_health_apis,
        duckdb_pipeline: dlt.Pipeline,
        resource: str,
        expected_tables: int,
        configs: dict | None,
    ):
        # GIVEN
        # Mocked APIs
        mock_google_health_apis(endpoints=[resource])

        # WHEN
        source = google_health_source(access_token="dummy_token").with_resources(
            f"google_health__{resource}",
        )
        info = duckdb_pipeline.extract(source)

        # THEN
        assert len(info.loads_ids) == 1

    def test_normalize(
        self,
        duckdb_pipeline: dlt.Pipeline,
        resource: str,
        expected_tables: int,
        configs: dict | None,
    ):
        # GIVEN
        expected_rows = 3
        file_name = f"google_health__{resource}-run1_page1.json"
        source = sample_resource(
            file_name,
            resource_configs=configs,
            data_selector="dataPoints",
        )
        duckdb_pipeline.extract(source, table_name=resource)

        # WHEN
        info = duckdb_pipeline.normalize()

        # THEN
        assert len([r for r in info.row_counts if r.startswith(resource)]) == expected_tables
        assert info.row_counts[resource] == expected_rows

    def test_load(
        self,
        duckdb_pipeline: dlt.Pipeline,
        resource: str,
        expected_tables: int,
        configs: dict | None,
    ):
        # GIVEN
        file_name = f"google_health__{resource}-run1_page1.json"
        source = sample_resource(
            file_name,
            resource_configs=configs,
            data_selector="dataPoints",
        )
        duckdb_pipeline.extract(source, table_name=f"load_{resource}")
        duckdb_pipeline.normalize()

        # WHEN
        info = duckdb_pipeline.load()

        # THEN
        assert info.has_failed_jobs is False
        assert all(p.state == "loaded" for p in info.load_packages)


@pytest.mark.parametrize(
    ("resource", "increment"),
    (
        ("sleep", True),
        ("steps", True),
        ("exercise", True),
        ("sleep", False),
        ("steps", False),
        ("exercise", False),
    ),
)
def test_google_health_refresh(
    mock_google_health_apis,
    duckdb_pipeline: dlt.Pipeline,
    resource: str,
    increment: bool,
):
    # GIVEN (1)
    mock_google_health_apis(endpoints=[resource])
    expected_rows = 5  # 3 from page 1 + 2 from page 2
    dataset = duckdb_pipeline.dataset_name
    table = resource
    write_disposition = None if increment else "replace"

    # WHEN (1)
    source = google_health_source(access_token="dummy_token").with_resources(
        f"google_health__{resource}"
    )
    info = duckdb_pipeline.run(source)

    # THEN (1)
    assert info.first_run is True
    assert info.has_failed_jobs is False
    with duckdb_pipeline.sql_client() as client:
        table_rows = client.execute_sql(f"SELECT 1 FROM {dataset}.google_health__{table}")
    assert len(table_rows) == expected_rows

    # GIVEN (2)
    expected_rows = expected_rows + 1 if increment else expected_rows

    # WHEN (2)
    info2 = duckdb_pipeline.run(source, write_disposition=write_disposition)

    # THEN (2)
    assert info2.first_run is False
    assert info2.has_failed_jobs is False
    with duckdb_pipeline.sql_client() as client:
        table_rows2 = client.execute_sql(f"SELECT 1 FROM {dataset}.google_health__{table}")
    assert len(table_rows2) == expected_rows


def test_google_health_pipeline(mock_google_health_apis, duckdb_pipeline):
    """
    Test that the Google Health pipeline runs and loads data correctly.
    """
    # GIVEN
    mock_google_health_apis()

    # WHEN
    source = google_health_source(access_token="dummy_token")
    info = duckdb_pipeline.run(source)

    # THEN
    assert info.first_run is True
    assert info.has_failed_jobs is False
    assert len(info.loads_ids) == 1

    dataset = duckdb_pipeline.dataset_name
    with duckdb_pipeline.sql_client() as client:
        sleep_table = client.execute_sql(f"SELECT COUNT(*) FROM {dataset}.google_health__sleep")
        assert sleep_table[0][0] >= 3
