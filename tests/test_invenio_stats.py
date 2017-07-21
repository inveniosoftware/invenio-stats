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

"""Module tests."""

from __future__ import absolute_import, print_function

import uuid

import pytest
from elasticsearch_dsl import Search
from flask import Flask
from invenio_queues.proxies import current_queues
from invenio_search import current_search_client

from invenio_stats import InvenioStats
from invenio_stats.proxies import current_stats
from invenio_stats.tasks import StatAggregator, aggregate_events, \
    process_events


def test_version():
    """Test version import."""
    from invenio_stats import __version__
    assert __version__


def test_init():
    """Test extension initialization."""
    app = Flask('testapp')
    ext = InvenioStats(app)
    assert 'invenio-stats' in app.extensions

    app = Flask('testapp')
    ext = InvenioStats()
    assert 'invenio-stats' not in app.extensions
    ext.init_app(app)
    assert 'invenio-stats' in app.extensions


def test_event_queues_declare(app, event_entrypoints):
    """Test that event queues are declared properly."""
    for event in current_stats.events.values():
        assert not event.queue.exists
    current_queues.declare()
    for event in current_stats.events.values():
        assert event.queue.exists


def test_publish_and_consume_events(app, event_entrypoints):
    """Test that events are published and consumed properly."""
    event_type = 'file-download'
    events = [{"payload": "test {}".format(idx)} for idx in range(3)]
    current_queues.declare()
    current_stats.publish(event_type, events)
    assert list(current_stats.consume(event_type)) == events


def test_batch_events(app, event_entrypoints, objects,
                      queued_events, sequential_ids):
    """Test processing of multiple events and checking aggregation counts."""

    process_events(['file-download'])
    aggregate_events.delay(['file-download-agg'])
    current_search_client.indices.flush(index='*')

    query = Search(using=current_search_client,
                   index='stats-file-download').sort('file_id')
    results = query.execute()
    for idx, result in enumerate(results.hits):
        assert uuid.UUID(result.file_id) == sequential_ids[idx]
        if idx < 1000:
            assert result.count == 101
        else:
            assert result.count == 1

    current_search_client.indices.delete(index='events-stats-file-download')
    current_search_client.indices.delete(index='stats-file-download')


def test_wrong_intervals(app):
    with pytest.raises(ValueError):
        StatAggregator(current_search_client, 'test', 'test', 'month', 'day')
