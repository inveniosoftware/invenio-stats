# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2016 CERN.
#
# Invenio is free software; you can redistribute it
# and/or modify it under the terms of the GNU General Public License as
# published by the Free Software Foundation; either version 2 of the
# License, or (at your option) any later version.
#
# Invenio is distributed in the hope that it will be
# useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Invenio; if not, write to the
# Free Software Foundation, Inc., 59 Temple Place, Suite 330, Boston,
# MA 02111-1307, USA.
#
# In applying this license, CERN does not
# waive the privileges and immunities granted to it by virtue of its status
# as an Intergovernmental Organization or submit itself to any jurisdiction.

"""Celery background tasks."""

from __future__ import absolute_import, print_function

from celery import shared_task

from .proxies import current_stats


@shared_task
def process_events(event_types):
    """Index statistics events."""
    results = []
    for e in event_types:
        processor = current_stats.events[e].processor_class(
            **current_stats.events[e].processor_config)
        results.append((e, processor.run()))
    return results


@shared_task
def aggregate_events(aggregations):
    """Aggregate indexed events."""
    results = []
    for a in aggregations:
        aggregator = current_stats.aggregations[a].aggregator_class(
            **current_stats.aggregations[a].aggregator_config)
        results.append(aggregator.run())
    return results
