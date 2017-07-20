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

import datetime
import uuid

from elasticsearch_dsl import Search
from flask import Flask
from invenio_queues.proxies import current_queues
from invenio_search import current_search, current_search_client
from mock import MagicMock, Mock, patch

from invenio_stats import InvenioStats
from invenio_stats.indexer import EventsIndexer
from invenio_stats.proxies import current_stats
from invenio_stats.tasks import aggregate_events, process_events


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


def test_batch_events(app, event_entrypoints, objects):
    """Test processing of multiple events and checking aggregation counts."""

    mock_user = Mock()
    mock_user.get_id = lambda: '123'
    mock_user.is_authenticated = True

    current_queues.declare()

    for t in current_search.put_templates(ignore=[400]):
        pass

    with patch('invenio_stats.utils.current_user', mock_user):
        with app.test_request_context(
            headers={'USER_AGENT':
                     'Mozilla/5.0 (Windows NT 6.1; WOW64) '
                     'AppleWebKit/537.36 (KHTML, like Gecko)'
                     'Chrome/45.0.2454.101 Safari/537.36'}):
            ids = [uuid.UUID((
                '0000000000000000000000000000000' + str(i))[-32:])
                for i in range(100000)]
            # ids = [ for i in range(1000 * len(objects))]
            multiple_events = ids[:1000]
            multiple_events = [event for event in multiple_events
                               for _ in range(100)]
            ids = ids + multiple_events
            generator_list = []
            for i in ids:
                file_obj = objects[0]
                file_obj.file_id = i
                file_obj.bucket_id = i
                msg = dict(
                    # When:
                    timestamp=datetime.datetime.utcnow().isoformat(),
                    # What:
                    bucket_id=str(i),
                    file_id=str(i),
                    filename='test.pdf',
                    # labels=record.get('communities', []),
                    # Who:
                    visitor_id=100,
                )
                generator_list.append(msg)

            mock_queue = Mock()
            mock_queue.consume.return_value = (msg for msg in generator_list)
            mock_queue.routing_key = 'stats-file-download'
            mock_processor = MagicMock()
            mock_processor.run = EventsIndexer(mock_queue,
                                               'events',
                                               '%Y-%m-%d',
                                               current_search_client).run
            mock_processor.actionsiter = EventsIndexer.actionsiter
            mock_processor.process_event = EventsIndexer.process_event
            mock_cfg = MagicMock()
            mock_cfg.processor = mock_processor
            mock_events_dict = {'file-download': mock_cfg}
            mock_events = MagicMock()
            mock_events.__getitem__.side_effect = \
                mock_events_dict.__getitem__
        with patch('invenio_stats.ext._InvenioStatsState.events', mock_events):
            process_events(['file-download'])

    aggregate_events.delay(['file-download-agg'])

    current_search_client.indices.flush(index='*')

    query = Search(using=current_search_client,
                   index='stats-file-download').sort('file_id')
    results = query.execute()
    for idx, result in enumerate(results.hits):
        assert uuid.UUID(result.file_id) == ids[idx]
        if idx < 1000:
            assert result.count == 101
        else:
            assert result.count == 1

    current_search_client.indices.delete(index='events-stats-file-download')
    current_search_client.indices.delete(index='stats-file-download')
