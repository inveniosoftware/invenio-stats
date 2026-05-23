# SPDX-FileCopyrightText: 2018 CERN.
# SPDX-FileCopyrightText: 2025 Graz University of Technology.
# SPDX-License-Identifier: MIT

"""Invenio Stats testing helpers."""

import datetime
import time

from invenio_queues.proxies import current_queues


def get_queue_size(queue_name):
    """Get the current number of messages in a queue."""
    queue = current_queues.queues[queue_name]
    time.sleep(1)  # necessary, queue returning random sizes otherwise
    _, size, _ = queue.queue.queue_declare(passive=True)
    return size


def mock_date(*date_parts):
    """Mocked 'datetime.now()'."""

    class MockDate(datetime.datetime):
        """datetime.datetime mock."""

        @classmethod
        def now(cls, tzinfo=datetime.timezone.utc):
            """Override to return 'current_date'."""
            return cls(*date_parts, tzinfo=tzinfo)

    return MockDate
