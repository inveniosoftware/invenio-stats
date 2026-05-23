# SPDX-FileCopyrightText: 2016-2018 CERN.
# SPDX-License-Identifier: MIT

"""Celery background tasks."""

from .proxies import current_stats


def declare_queues():
    """Index statistics events."""
    return [
        {"name": "stats-{0}".format(event), "exchange": current_stats.exchange}
        for event in current_stats.events_config
    ]
