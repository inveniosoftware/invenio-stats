# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2016-2018 CERN.
#
# Invenio is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Celery background tasks."""

from .proxies import current_stats


def register_templates():
    """Register elasticsearch templates for events."""
    event_templates = [current_stats.stats_events[event]['templates']
                       for event in current_stats.stats_events]
    aggregation_templates = [
        current_stats.stats_aggregations[agg]['templates']
        for agg in current_stats.stats_aggregations
    ]
    return event_templates + aggregation_templates
