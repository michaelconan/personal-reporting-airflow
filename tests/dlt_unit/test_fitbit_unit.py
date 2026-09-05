# base imports
import json
from typing import Callable
from urllib.parse import parse_qs, urlparse

# PyPI imports
import pytest
from pytest import MonkeyPatch
import dlt

# local imports
from pipelines.sources.fitbit import fitbit_source
from tests.dlt_unit.conftest import sample_response, sample_resource


pytestmark = pytest.mark.local


@pytest.fixture
def mock_fitbit_apis(monkeypatch: MonkeyPatch, mock_responses) -> Callable:
    BASE_URL = "https://api.fitbit.com"

    # Mock the token to prevent credential errors
    monkeypatch.setenv("SOURCES__FITBIT__CLIENT_ID", "dummy_client_id")
    monkeypatch.setenv("SOURCES__FITBIT__CLIENT_SECRET", "dummy_secret")

    def offset_callback(request, resource: str):
        """Handle offset-based pagination for Fitbit APIs."""
        parsed_url = urlparse(request.url)
        params = parse_qs(parsed_url.query)
        offset = int(params.get("offset", [0])[0])
        limit = int(params.get("limit", [0])[0])
        after_date = params.get("afterDate", [""])[0]

        if "2024-06-05" in after_date and offset == 0:
            # Subsequent run page
            return sample_response(f"fitbit__{resource}-run2.json")
        else:
            if offset == 0:
                # First page
                return sample_response(f"fitbit__{resource}-run1_page1.json")
            elif offset == limit:
                # Second page
                return sample_response(f"fitbit__{resource}-run1_page2.json")
        # No more data
        return (200, {}, json.dumps({resource: []}))

    def setup(endpoints=[]):
        """Nested function to only register mock endpoints for tests.

        Args:
            endpoints (list, optional): Specific endpoints to register. Defaults to [] (all).
        """
        # Mock the API responses
        if not endpoints or "sleep" in endpoints:
            mock_responses.add_callback(
                mock_responses.GET,
                BASE_URL + "/1.2/user/-/sleep/list.json",
                callback=lambda r: offset_callback(r, "sleep"),
                content_type="application/json",
            )
        if not endpoints or "activities" in endpoints:
            mock_responses.add_callback(
                mock_responses.GET,
                BASE_URL + "/1/user/-/activities/list.json",
                callback=lambda r: offset_callback(r, "activities"),
                content_type="application/json",
            )

    return setup


@pytest.mark.parametrize(
    ("resource", "expected_tables", "configs"),
    (
        ("sleep", 1, {"max_table_nesting": 1}),
        ("activities", 1, {"max_table_nesting": 1}),
    ),
)
class TestFitbitPhases:
    def test_extract(
        self,
        mock_fitbit_apis,
        duckdb_pipeline: dlt.Pipeline,
        resource: str,
        expected_tables: int,
        configs: dict | None,
    ):
        # GIVEN
        # Mocked APIs
        mock_fitbit_apis(endpoints=[resource])

        # WHEN
        source = fitbit_source(api_key="dummy_token").with_resources(
            f"fitbit__{resource}",
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
        file_name = f"fitbit__{resource}-run1_page1.json"
        source = sample_resource(
            file_name,
            resource_configs=configs,
            data_selector=resource,
        )
        duckdb_pipeline.extract(source, table_name=resource)

        # WHEN
        info = duckdb_pipeline.normalize()

        # THEN
        # _dlt_state table, source table, and any child tables
        assert len([r for r in info.row_counts if r.startswith(resource)]) == expected_tables
        # first page record count
        assert info.row_counts[resource] == expected_rows

    def test_load(
        self,
        duckdb_pipeline: dlt.Pipeline,
        resource: str,
        expected_tables: int,
        configs: dict | None,
    ):
        # GIVEN
        # Files to load for sample test
        file_name = f"fitbit__{resource}-run1_page1.json"
        source = sample_resource(
            file_name,
            resource_configs=configs,
            data_selector=resource,
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
        ("activities", True),
        ("sleep", False),
        ("activities", False),
    ),
)
def test_fitbit_refresh(
    mock_fitbit_apis,
    duckdb_pipeline: dlt.Pipeline,
    resource: str,
    increment: bool,
):
    # GIVEN (1)
    # Mock APIs
    mock_fitbit_apis(endpoints=[resource])
    expected_rows = 5  # 3 from page 1 + 2 from page 2
    # Dataset from pipeline (dev mode)
    dataset = duckdb_pipeline.dataset_name
    table = resource
    # Defaults to append for most resources
    write_disposition = None if increment else "replace"

    # WHEN (1)
    source = fitbit_source(api_key="dummy_token").with_resources(f"fitbit__{resource}")
    info = duckdb_pipeline.run(source)

    # THEN (1)
    # Run pipeline the first time
    assert info.first_run is True
    assert info.has_failed_jobs is False
    # Validate loaded data from initial run
    with duckdb_pipeline.sql_client() as client:
        table_rows = client.execute_sql(f"SELECT 1 FROM {dataset}.fitbit__{table}")
    assert len(table_rows) == expected_rows

    # GIVEN (2)
    expected_rows += 1 if increment else 0

    # WHEN (2)
    info2 = duckdb_pipeline.run(source, write_disposition=write_disposition)

    # THEN (2)
    # Run pipeline again
    assert info2.first_run is False
    assert info2.has_failed_jobs is False
    # Validate loaded data from incremental run
    with duckdb_pipeline.sql_client() as client:
        table_rows2 = client.execute_sql(f"SELECT 1 FROM {dataset}.fitbit__{table}")
    assert len(table_rows2) == expected_rows


def test_fitbit_pipeline(mock_fitbit_apis, duckdb_pipeline):
    """
    Test that the Fitbit pipeline runs and loads data correctly.
    """
    # GIVEN
    mock_fitbit_apis()

    # WHEN
    # Run the pipeline
    source = fitbit_source(api_key="dummy_token")
    info = duckdb_pipeline.run(source)

    # THEN
    # Assert that the pipeline ran successfully
    assert info.first_run is True
    assert info.has_failed_jobs is False
    assert len(info.loads_ids) == 1

    # Check the loaded data
    dataset = duckdb_pipeline.dataset_name
    with duckdb_pipeline.sql_client() as client:
        # Check sleep table — use DISTINCT log_id to guard against dlt loading
        # records more than once when multiple resources run together (dlt>=1.23)
        sleep_table = client.execute_sql(f"SELECT DISTINCT log_id FROM {dataset}.fitbit__sleep")
        assert len(sleep_table) == 5

        activities_table = client.execute_sql(
            f"SELECT DISTINCT log_id FROM {dataset}.fitbit__activities"
        )
        assert len(activities_table) == 5
