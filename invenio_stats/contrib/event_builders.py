# SPDX-FileCopyrightText: 2017-2018 CERN.
# SPDX-FileCopyrightText: 2022 TU Wien.
# SPDX-FileCopyrightText: 2025 Graz University of Technology.
# SPDX-License-Identifier: MIT

"""Signal receivers for certain events."""

import datetime

from flask import request

from ..utils import format_datetime_iso, get_user


def _build_timestamp():
    """Build an event timestamp.

    Uses centralized date formatting that strips microseconds and timezone info
    when STATS_EVENTS_UTC_DATETIME_ENABLED is False (default) to ensure
    compatibility with strict_date_hour_minute_second format.
    """
    ts = datetime.datetime.now(datetime.timezone.utc)
    return format_datetime_iso(ts)


def file_download_event_builder(event, sender_app, obj=None, **kwargs):
    """Build a file-download event."""
    event.update(
        {
            # When:
            "timestamp": _build_timestamp(),
            # What:
            "bucket_id": str(obj.bucket_id),
            "file_id": str(obj.file_id),
            "file_key": obj.key,
            "size": obj.file.size,
            "referrer": request.referrer,
            # Who:
            **get_user(),
        }
    )
    return event


def build_file_unique_id(doc):
    """Build file unique identifier."""
    doc["unique_id"] = "{0}_{1}".format(doc["bucket_id"], doc["file_id"])
    return doc


def build_record_unique_id(doc):
    """Build record unique identifier."""
    doc["unique_id"] = "{0}_{1}".format(doc["pid_type"], doc["pid_value"])
    return doc


def record_view_event_builder(event, sender_app, pid=None, record=None, **kwargs):
    """Build a record-view event."""
    event.update(
        {
            # When:
            "timestamp": _build_timestamp(),
            # What:
            "record_id": str(record.id),
            "pid_type": pid.pid_type,
            "pid_value": str(pid.pid_value),
            "referrer": request.referrer,
            # Who:
            **get_user(),
        }
    )
    return event
