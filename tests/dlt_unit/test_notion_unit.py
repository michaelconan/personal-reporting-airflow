# base imports
import re
import json
from typing import Callable

# PyPI imports
import pytest
from pytest import MonkeyPatch
import dlt

# local imports
from pipelines.sources.notion import notion_source
from tests.dlt_unit.conftest import sample_response, sample_resource


pytestmark = pytest.mark.local


@pytest.fixture
def mock_notion_apis(monkeypatch: MonkeyPatch, mock_responses) -> Callable:
    BASE_URL = "https://api.notion.com/v1"

    # Mock the API key to prevent credential errors
    monkeypatch.setenv("SOURCES__NOTION__API_KEY", "dummy_api_key")

    def cursor_callback(request):
        """Handle cursor-based pagination for Notion data source queries."""
        body = json.loads(request.body)
        start_cursor = body.get("start_cursor")
        after_date = body["filter"]["last_edited_time"]["after"]

        if "2024-06-05" in after_date:
            # Subsequent run page
            return sample_response("notion__data_source_rows-run2.json")
        else:
            if not start_cursor:
                # First page
                return sample_response("notion__data_source_rows-run1_page1.json")
            elif start_cursor == "cursor_page2_token":
                # Second page
                return sample_response("notion__data_source_rows-run1_page2.json")
        # No more data
        return (
            200,
            {},
            json.dumps(
                {
                    "object": "list",
                    "results": [],
                    "next_cursor": None,
                    "has_more": False,
                },
            ),
        )

    def data_sources_callback(request):
        """Handle data source search requests (single page)."""
            return sample_response("notion__data_sources.json")

    def setup(endpoints=[]):
        """Nested function to only register mock endpoints for tests.

        Args:
            endpoints (list, optional): Specific endpoints to register. Defaults to [] (all).
        """
        # Mock the API responses
        if not endpoints or "data_sources" in endpoints:
            mock_responses.add_callback(
                mock_responses.POST,
                BASE_URL + "/search",
                callback=data_sources_callback,
                content_type="application/json",
            )
        if not endpoints or "data_source_rows" in endpoints:
            mock_responses.add_callback(
                mock_responses.POST,
                re.compile(BASE_URL + r"/data_sources/([a-f0-9\-]+)/query"),
                callback=cursor_callback,
                content_type="application/json",
            )

    return setup


class TestNotionPhases:
    TEST_PARAMS = (
        ("resource", "expected_tables", "configs"),
        (
            # columns hint mirrors pipeline config: title is a rich text array stored as JSON
            (
                "data_sources",
                1,
                {"max_table_nesting": 1, "columns": {"title": {"data_type": "json"}}},
            ),
            ("data_source_rows", 1, {"max_table_nesting": 2}),
        ),
    )

    def test_extract(
        self,
        mock_notion_apis,
        duckdb_pipeline: dlt.Pipeline,
    ):
        # GIVEN
        # Mocked APIs
        mock_notion_apis()

        # WHEN
        # For Notion, we need to specify which database to query
        source = notion_source(db_name="daily_habits")
        info = duckdb_pipeline.extract(source)

        # THEN
        assert len(info.loads_ids) == 1

    @pytest.mark.parametrize(*TEST_PARAMS)
    def test_normalize(
        self,
        duckdb_pipeline: dlt.Pipeline,
        resource: str,
        expected_tables: int,
        configs: dict,
    ):
        # GIVEN
        expected_rows = 3 if "rows" in resource else 1
        file_name = f"notion__{resource}-run1_page1.json"
        file_name2 = f"notion__{resource}.json"
        source = sample_resource(
            file_name,
            fallback=file_name2,
            data_selector="results",
            resource_configs=configs,
        )
        duckdb_pipeline.extract(source, table_name=resource)

        # WHEN
        info = duckdb_pipeline.normalize()

        # THEN
        # _dlt_state table, source table, and any child tables
        assert len([r for r in info.row_counts if r.startswith(resource)]) == expected_tables
        # record count from sample data
        assert info.row_counts[resource] == expected_rows

    @pytest.mark.parametrize(*TEST_PARAMS)
    def test_load(
        self,
        duckdb_pipeline: dlt.Pipeline,
        resource: str,
        expected_tables: int,
        configs: dict,
    ):
        # GIVEN
        file_name = f"notion__{resource}-run1_page1.json"
        file_name2 = f"notion__{resource}.json"
        source = sample_resource(
            file_name,
            fallback=file_name2,
            data_selector="results",
            resource_configs=configs,
        )
        duckdb_pipeline.extract(source, table_name=resource)
        duckdb_pipeline.normalize()

        # WHEN
        info = duckdb_pipeline.load()

        # THEN
        assert info.has_failed_jobs is False
        assert all(p.state == "loaded" for p in info.load_packages)


@pytest.mark.parametrize(
    ("increment"),
    (
        True,
        False,
    ),
)
def test_notion_pipeline_refresh(mock_notion_apis, duckdb_pipeline, increment: bool):
    """Test that the Notion pipeline runs and loads data correctly.

    Run incremental refresh testing and all source resources together due to resource
    interdependency in Notion data source.
    """
    # GIVEN (1)
    mock_notion_apis()
    expected_rows = 5
    # Defaults to append for most resources
    write_disposition = None if increment else "replace"

    # WHEN (1)
    # Run the pipeline
    source = notion_source(db_name="daily_habits")
    info = duckdb_pipeline.run(source)

    # THEN (1)
    # Assert that the pipeline ran successfully
    assert info.first_run is True
    assert info.has_failed_jobs is False
    assert len(info.loads_ids) == 1

    # Check the loaded data
    dataset = duckdb_pipeline.dataset_name
    with duckdb_pipeline.sql_client() as client:
        # Check data_sources table (search results)
        databases_table = client.execute_sql(
            f"SELECT 1 FROM {dataset}.notion__data_sources",
        )
        assert len(databases_table) == 1

        # Check data_source_rows table
        rows_table = client.execute_sql(
            f"SELECT 1 FROM {dataset}.notion__data_source_daily_habits",
        )
        assert len(rows_table) == expected_rows

    # GIVEN (2)
    # Only database_rows should increment (databases search results stay same)
    expected_rows += 1 if increment else 0

    # WHEN (2)
    info2 = duckdb_pipeline.run(source, write_disposition=write_disposition)

    # THEN (2)
    # Run pipeline again
    assert info2.first_run is False
    assert info2.has_failed_jobs is False
    # Validate loaded data from incremental run
    with duckdb_pipeline.sql_client() as client:
        # Check data_source_rows table
        rows_table2 = client.execute_sql(
            f"SELECT 1 FROM {dataset}.notion__data_source_daily_habits",
        )
        assert len(rows_table2) == expected_rows
