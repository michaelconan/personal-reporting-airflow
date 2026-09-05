"""Configuration and fixtures for DLT unit tests.

This module provides common fixtures, helper functions, and configuration
for testing DLT (Data Load Tool) pipeline components.
"""

# base imports
import os
import json
from pathlib import Path
from typing import Any

# PyPI imports
import pytest
import dlt


MOCK_ROOT = Path(__file__).resolve().parents[1] / "mock" / "data"
MOCK_FOLDER = str(MOCK_ROOT)


def resolve_mock_path(file_name: str) -> str:
    """Resolve fixture paths under the system-scoped tests/mock/data layout."""
    if os.path.isabs(file_name):
        return file_name

    path = Path(file_name)
    candidate = (MOCK_ROOT / path).resolve()
    if candidate.exists():
        return str(candidate)

    system_prefix = file_name.split("__", 1)[0] if "__" in file_name else file_name.split("_", 1)[0]
    candidate = (MOCK_ROOT / system_prefix / path.name).resolve()
    if candidate.exists():
        return str(candidate)

    return str((MOCK_ROOT / path).resolve())


# This environment variable is set to disable the Google Secrets provider for all dlt unit tests.
# It needs to be set before any dlt modules are imported, which is why it's at the top of this file.
# Only applies to unit tests - E2E tests should not be affected
os.environ["PROVIDERS__ENABLE_GOOGLE_SECRETS"] = "false"
os.environ["DLT_TELEMETRY_DISABLED"] = "1"


@pytest.fixture
def mock_responses():
    """Pytest fixture providing a requests mock for HTTP calls.

    Yields:
        responses.RequestsMock: Mock object for intercepting HTTP requests.
    """
    import responses

    with responses.RequestsMock() as rsps:
        yield rsps


def sample_resource(
    file_name: str,
    fallback: str | None = None,
    data_selector: str | None = None,
    resource_configs: dict | None = None,
) -> Any:
    """Create a DLT resource from sample JSON data for testing.

    Args:
        file_name: Name of the JSON file in the mock data folder.
        fallback: Optional fallback file name if the primary file doesn't exist.
        data_selector: Optional JSON key to select specific data from the file.
        resource_configs: Additional DLT resource configuration options.

    Returns:
        DLT resource configured with the sample data.
    """

    @dlt.resource(**(resource_configs or {}))
    def sample_resource():
        source = sample_data(file_name=file_name, fallback=fallback)
        if data_selector:
            source = source[data_selector]
        return source

    return sample_resource


def sample_data(
    file_name: str,
    fallback: str | None = None,
) -> dict:
    """Load sample JSON data from the mock data folder.

    Args:
        file_name: Name of the JSON file to load.
        fallback: Optional fallback file name if the primary file doesn't exist.

    Returns:
        dict: Parsed JSON data from the file.
    """
    file = resolve_mock_path(file_name)
    if not os.path.exists(file) and fallback is not None:
        file = resolve_mock_path(fallback)
    with open(file, "r") as f:
        return json.load(f)


def sample_response(file_name: str) -> tuple[int, dict, str]:
    """Create a mock HTTP response from a JSON file.

    Args:
        file_name: Name of the JSON file to use as response body.

    Returns:
        tuple: HTTP response tuple (status_code, headers, body).
    """
    with open(resolve_mock_path(file_name), "r") as f:
        return (200, {}, f.read())


@pytest.fixture(scope="class")
def duckdb_pipeline() -> dlt.Pipeline:
    """Pytest fixture providing a DuckDB pipeline for unit testing.

    Creates a DLT pipeline configured to use DuckDB as the destination
    for fast local testing. The pipeline is automatically cleaned up
    after each test class.

    Yields:
        dlt.Pipeline: Configured DLT pipeline for testing.
    """
    import tempfile

    # Use an isolated temporary HOME to avoid touching user's ~/.dlt files
    tmp_home = tempfile.TemporaryDirectory()
    old_home = os.environ.get("HOME")
    old_userprofile = os.environ.get("USERPROFILE")
    try:
        os.environ["HOME"] = tmp_home.name
        os.environ["USERPROFILE"] = tmp_home.name

        # Test pipeline
        pipeline = dlt.pipeline(
            pipeline_name="local_unit_test",
            destination="duckdb",
            dataset_name="local_data",
            dev_mode=True,
        )
        yield pipeline
    finally:
        # Cleanup pipeline and temporary HOME
        try:
            pipeline.drop()
        except Exception:
            pass
        tmp_home.cleanup()
        # restore env
        if old_home is None:
            os.environ.pop("HOME", None)
        else:
            os.environ["HOME"] = old_home
        if old_userprofile is None:
            os.environ.pop("USERPROFILE", None)
        else:
            os.environ["USERPROFILE"] = old_userprofile
