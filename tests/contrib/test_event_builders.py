# SPDX-FileCopyrightText: 2017-2018 CERN.
# SPDX-FileCopyrightText: 2025 Graz University of Technology.
# SPDX-License-Identifier: MIT

"""Test event builders."""

from datetime import datetime, timezone
from unittest.mock import patch

from invenio_stats.contrib.event_builders import (
    file_download_event_builder,
    record_view_event_builder,
)
from invenio_stats.utils import get_user


class NewDate(datetime):
    @classmethod
    def now(cls, tzinfo):
        return cls(2017, 1, 1, tzinfo=tzinfo)


headers = {
    "USER_AGENT": "Mozilla/5.0 (Windows NT 6.1; WOW64) "
    "AppleWebKit/537.36 (KHTML, like Gecko)"
    "Chrome/45.0.2454.101 Safari/537.36"
}


def test_file_download_event_builder(app, mock_user_ctx, sequential_ids, objects):
    """Test the file-download event builder."""
    file_obj = objects[0]
    file_obj.bucket_id = sequential_ids[0]

    with app.test_request_context(headers=headers):
        event = {}
        with patch("datetime.datetime", NewDate):
            file_download_event_builder(event, app, file_obj)
        assert event == {
            # When:
            "timestamp": NewDate.now(tzinfo=timezone.utc)
            .replace(tzinfo=None)
            .isoformat(),
            # What:
            "bucket_id": str(file_obj.bucket_id),
            "file_id": str(file_obj.file_id),
            "file_key": file_obj.key,
            "size": file_obj.file.size,
            "referrer": None,
            # Who:
            **get_user(),
        }


def test_record_view_event_builder(app, mock_user_ctx, record, pid):
    """Test the record view event builder."""
    with app.test_request_context(headers=headers):
        event = {}
        with patch("datetime.datetime", NewDate):
            record_view_event_builder(event, app, pid, record)
        assert event == {
            # When:
            "timestamp": NewDate.now(tzinfo=timezone.utc)
            .replace(tzinfo=None)
            .isoformat(),
            # What:
            "record_id": str(record.id),
            "pid_type": pid.pid_type,
            "pid_value": str(pid.pid_value),
            "referrer": None,
            # Who:
            **get_user(),
        }


def test_file_download_event_builder_aware_datetime(
    app, mock_user_ctx, sequential_ids, objects
):
    """Test file-download event builder produces aware UTC datetime when enabled."""
    file_obj = objects[0]
    file_obj.bucket_id = sequential_ids[0]

    app.config["STATS_EVENTS_UTC_DATETIME_ENABLED"] = True
    try:
        with app.test_request_context(headers=headers):
            event = {}
            with patch("datetime.datetime", NewDate):
                file_download_event_builder(event, app, file_obj)
            assert event["timestamp"] == NewDate.now(tzinfo=timezone.utc).isoformat()
            # Aware ISO format must contain timezone offset
            assert "+00:00" in event["timestamp"]
    finally:
        app.config["STATS_EVENTS_UTC_DATETIME_ENABLED"] = False


def test_record_view_event_builder_aware_datetime(app, mock_user_ctx, record, pid):
    """Test record-view event builder produces aware UTC datetime when enabled."""
    app.config["STATS_EVENTS_UTC_DATETIME_ENABLED"] = True
    try:
        with app.test_request_context(headers=headers):
            event = {}
            with patch("datetime.datetime", NewDate):
                record_view_event_builder(event, app, pid, record)
            assert event["timestamp"] == NewDate.now(tzinfo=timezone.utc).isoformat()
            # Aware ISO format must contain timezone offset
            assert "+00:00" in event["timestamp"]
    finally:
        app.config["STATS_EVENTS_UTC_DATETIME_ENABLED"] = False
