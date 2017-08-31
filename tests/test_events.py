# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2017 CERN.
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

"""Events tests."""
from conftest import _create_file_download_event
from invenio_queues.proxies import current_queues
from mock import patch

from invenio_stats.contrib.event_builders import build_file_unique_id, \
    file_download_event_builder
from invenio_stats.processors import EventsIndexer, anonymize_user, \
    flag_robots, hash_id
from invenio_stats.proxies import current_stats
from invenio_stats.tasks import process_events


def test_event_queues_declare(app, event_entrypoints):
    """Test that event queues are declared properly."""
    try:
        for event in current_stats.events.values():
            assert not event.queue.exists
        current_queues.declare()
        for event in current_stats.events.values():
            assert event.queue.exists
    finally:
        current_queues.delete()


def test_publish_and_consume_events(app, event_entrypoints):
    """Test that events are published and consumed properly."""
    try:
        event_type = 'file-download'
        events = [{"payload": "test {}".format(idx)} for idx in range(3)]
        current_queues.declare()
        current_stats.publish(event_type, events)
        assert list(current_stats.consume(event_type)) == events
    finally:
        current_queues.delete()


def test_events_indexer_preprocessors(app, mock_event_queue):
    """Check that EventsIndexer calls properly the preprocessors."""
    def test_preprocessor1(event):
        event['test1'] = 42
        event['visitor_id'] = 'testuser1'
        return event

    def test_preprocessor2(event):
        event['test2'] = 21
        return event

    indexer = EventsIndexer(
        mock_event_queue,
        preprocessors=[build_file_unique_id,
                       test_preprocessor1,
                       test_preprocessor2]
    )

    # Generate the events
    received_docs = []

    def bulk(client, generator, *args, **kwargs):
        received_docs.extend(generator)

    with patch('elasticsearch.helpers.bulk', side_effect=bulk):
        indexer.run()

    # Process the events as we expect them to be
    expected_docs = []
    for event in mock_event_queue.queued_events:
        event = build_file_unique_id(event)
        event = test_preprocessor1(event)
        event = test_preprocessor2(event)
        _id = hash_id('2017-01-01T00:00:00', event)
        expected_docs.append(dict(
            _id=_id,
            _op_type='index',
            _index='events-stats-file-download-2017-01-01',
            _type='stats-file-download',
            _source=event,
        ))

    assert received_docs == expected_docs


def test_events_indexer_id_windowing(app, mock_event_queue):
    """Check that EventsIndexer applies time windows to ids."""

    indexer = EventsIndexer(mock_event_queue, preprocessors=[],
                            double_click_window=180)

    # Generated docs will be registered in this list
    received_docs = []

    def bulk(client, generator, *args, **kwargs):
        received_docs.extend(generator)

    mock_event_queue.consume.return_value = [
        _create_file_download_event(date) for date in
        [
            # Those two events will be in the same window
            (2017, 6, 1, 0, 11, 3), (2017, 6, 1, 0, 9, 1),
            # Those two events will be in the same window
            (2017, 6, 2, 0, 12, 10), (2017, 6, 2, 0, 13, 3),
            (2017, 6, 2, 0, 30, 3)
        ]
    ]

    with patch('elasticsearch.helpers.bulk', side_effect=bulk):
        indexer.run()

    assert len(received_docs) == 5
    ids = set(doc['_id'] for doc in received_docs)
    assert len(ids) == 3


def test_anonymise_user(app, mock_user_ctx, request_headers, objects):
    """Test anonymize_user preprocessor."""
    with app.test_request_context(headers=request_headers['user']):
        event = file_download_event_builder({}, app, objects[0])
    event = anonymize_user(event)
    assert 'user_id' not in event
    assert 'user_agent' not in event
    assert 'ip_address' not in event
    assert event['visitor_id'] == \
        '78d8045d684abd2eece923758f3cd781489df3a48e1278982466017f'


def test_flag_robots(app, mock_user_ctx, request_headers, objects):
    """Test flag_robots preprocessor."""
    def build_event(headers):
        with app.test_request_context(headers=headers):
            event = file_download_event_builder({}, app, objects[0])
        return flag_robots(event)

    assert build_event(request_headers['user'])['is_robot'] is False
    assert build_event(request_headers['robot'])['is_robot'] is True


def test_double_clicks(app, mock_event_queue, es):
    """Test that events occurring within a time window are counted as 1."""
    event_type = 'file-download'
    events = [_create_file_download_event(date) for date in
              [(2000, 6, 1, 10, 0, 10),
               (2000, 6, 1, 10, 0, 11),
               (2000, 6, 1, 10, 0, 19),
               (2000, 6, 1, 10, 0, 22)]]
    current_queues.declare()
    current_stats.publish(event_type, events)
    process_events(['file-download'])
    es.indices.refresh(index='*')
    res = es.search(
        index='events-stats-file-download-2000-06-01',
    )
    assert res['hits']['total'] == 2
