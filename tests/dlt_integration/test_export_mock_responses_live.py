"""Opt-in live API checks for the response exporter."""

import os

import pytest

import tests.mock.scripts.export_mock_responses as export_mock_responses


pytestmark = pytest.mark.live


@pytest.mark.skipif(
    os.getenv("RUN_LIVE_API_TESTS") != "1",
    reason="Set RUN_LIVE_API_TESTS=1 to call live APIs",
)
def test_live_hubspot_capture_can_generate_unit_fixtures(tmp_path, monkeypatch):
    """Run the real dlt source, capture JSON, and validate generated mock structure."""
    if not export_mock_responses.get_secret("sources.hubspot.api_key"):
        pytest.skip("HubSpot credentials are not configured")

    monkeypatch.setattr(export_mock_responses, "MOCK_DATA_DIR", tmp_path)
    session = export_mock_responses.ResponseCaptureSession()
    sources_map = {}

    export_mock_responses.run_hubspot_export(session, sources_map, dry_run=False)

    assert session.captured_responses
    export_mock_responses.save_captured_responses(
        session.captured_responses,
        sources_map,
        dry_run=False,
    )

    generated_files = list(tmp_path.rglob("*.json"))
    assert generated_files
    assert all(file_path.read_text(encoding="utf-8") for file_path in generated_files)