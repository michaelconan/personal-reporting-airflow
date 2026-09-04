# scripts/scrub_data.py
"""
Data scrubbing utilities using Faker to sanitize PII and sensitive text
in mock dbt seeds and mock API response bodies.
"""

from typing import Any, List, Union
from faker import Faker

fake = Faker()

# Common text fields across BigQuery columns and JSON response structures
TEXT_KEYS = {
    "properties__notes__rich_text",
    "properties__description__rich_text",
    "properties__hs_note_body",
    "properties__subject",
    "properties__hs_task_body",
    "properties__hs_call_title",
    "properties__hs_call_body",
    "properties__hs_meeting_title",
    "properties__hs_meeting_body",
    "properties__hs_internal_meeting_notes",
    "properties__hs_communication_body",
    "properties__name",
    "properties__dealname",
    "properties__content",
    "hs_note_body",
    "subject",
    "hs_task_body",
    "hs_call_title",
    "hs_call_body",
    "hs_meeting_title",
    "hs_meeting_body",
    "hs_internal_meeting_notes",
    "hs_communication_body",
    "dealname",
    "content",
    "description",
}


def apply_fakes_to_rows(data_rows: List[dict]) -> None:
    """Replace designated columns in BigQuery/seed rows with realistic fake data.

    Args:
        data_rows: List of dictionary rows to scrub in-place.
    """
    for row in data_rows:
        for col in TEXT_KEYS:
            if col in row and row[col] is not None:
                row[col] = fake.sentence(nb_words=12)
        if "properties__firstname" in row and row["properties__firstname"] is not None:
            row["properties__firstname"] = fake.first_name()
        if "properties__lastname" in row and row["properties__lastname"] is not None:
            row["properties__lastname"] = fake.last_name()
        if "properties__email" in row and row["properties__email"] is not None:
            row["properties__email"] = fake.email()


def scrub_api_response(data: Union[dict, list, Any]) -> Any:
    """Recursively scrub PII and free-text fields in JSON API response objects.

    Args:
        data: The JSON dictionary or list to scrub.

    Returns:
        The scrubbed JSON structure.
    """
    if isinstance(data, list):
        return [scrub_api_response(item) for item in data]
    elif isinstance(data, dict):
        scrubbed = {}
        for key, val in data.items():
            key_lower = key.lower()
            if isinstance(val, (dict, list)):
                scrubbed[key] = scrub_api_response(val)
            elif isinstance(val, str) and val:
                if key_lower in ("email", "user_email"):
                    scrubbed[key] = fake.email()
                elif key_lower in ("firstname", "first_name"):
                    scrubbed[key] = fake.first_name()
                elif key_lower in ("lastname", "last_name"):
                    scrubbed[key] = fake.last_name()
                elif key_lower in (
                    "subject",
                    "title",
                    "hs_note_body",
                    "hs_task_body",
                    "hs_call_title",
                    "hs_call_body",
                    "hs_meeting_title",
                    "hs_meeting_body",
                    "hs_internal_meeting_notes",
                    "hs_communication_body",
                    "dealname",
                    "content",
                    "description",
                ):
                    scrubbed[key] = fake.sentence(nb_words=8)
                else:
                    scrubbed[key] = val
            else:
                scrubbed[key] = val
        return scrubbed
    return data
