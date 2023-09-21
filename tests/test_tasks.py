# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2016-2018 CERN.
# Copyright (C)      2022 TU Wien.
#
# Invenio is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Test celery tasks."""

from invenio_stats import current_stats
from invenio_stats.tasks import process_events


def test_process_events(app, search_clear, event_queues):
    """Test process event."""
    current_stats.publish(
        "file-download",
        [
            {
                "timestamp": "2017-01-01T00:00:00",
                "visitor_id": "testuser1",
                "unique_id": "2017-01-01T00:00:00-hash",
                "data": "val",
            }
        ],
    )
    process_events.delay(["file-download"])
    # FIXME: no need to publish events. We should just mock "consume" and test
    # that the events are properly received and processed.
