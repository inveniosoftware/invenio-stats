# SPDX-FileCopyrightText: 2016-2018 CERN.
# SPDX-License-Identifier: MIT

"""Celery background tasks."""

from flask import current_app

from .proxies import current_stats


def _collect_templates():
    """Return event and aggregation templates from config."""
    event_templates = [
        event["templates"] for event in current_stats.events_config.values()
    ]
    aggregation_templates = [
        agg["templates"] for agg in current_stats.aggregations_config.values()
    ]

    return event_templates + aggregation_templates


def register_templates():
    """Register search templates for events."""
    if current_app.config["STATS_REGISTER_INDEX_TEMPLATES"]:
        return []
    return _collect_templates()


def register_index_templates():
    """Register search index templates for events."""
    if not current_app.config["STATS_REGISTER_INDEX_TEMPLATES"]:
        return []
    return _collect_templates()
