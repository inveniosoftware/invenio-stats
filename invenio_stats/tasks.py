# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2016-2018 CERN.
#
# Invenio is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Celery background tasks."""

from __future__ import absolute_import, print_function

from celery import shared_task
from dateutil.parser import parse as dateutil_parse

from .proxies import current_stats


@shared_task
def process_events(event_types):
    """Index statistics events."""
    results = []
    for event_name in event_types:
        event_cfg = current_stats.events[event_name]
        processor = event_cfg.cls(**event_cfg.params)
        results.append((event_name, processor.run()))
    return results


@shared_task
def aggregate_events(aggregations, start_date=None, end_date=None,
                     update_bookmark=True):
    """Aggregate indexed events."""
    start_date = dateutil_parse(start_date) if start_date else None
    end_date = dateutil_parse(end_date) if end_date else None
    results = []
    for aggr_name in aggregations:
        aggr_cfg = current_stats.aggregations[aggr_name]
        aggregator = aggr_cfg.cls(name=aggr_cfg.name, **aggr_cfg.params)
        results.append(aggregator.run(start_date, end_date, update_bookmark))
    return results
