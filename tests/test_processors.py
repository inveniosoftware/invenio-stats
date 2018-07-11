# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2018 CERN.
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

"""Event processor tests."""

import logging
from datetime import datetime

import pytest
from conftest import _create_file_download_event
from elasticsearch_dsl import Search
from helpers import get_queue_size
from invenio_queues.proxies import current_queues
from mock import patch

from invenio_stats.contrib.event_builders import build_file_unique_id, \
    file_download_event_builder
from invenio_stats.processors import EventsIndexer, anonymize_user, \
    flag_machines, flag_robots, hash_id
from invenio_stats.proxies import current_stats
from invenio_stats.tasks import process_events


@pytest.mark.parametrize(
    ['ip_addess', 'user_id', 'session_id', 'user_agent', 'timestamp',
     'exp_country', 'exp_visitor_id', 'exp_unique_session_id'],
    [
        # Minimal
        ('131.169.180.47', None, None, None, datetime(2018, 1, 1, 12),
         'DE',
         'd14a028c2a3a2bc9476102bb288234c415a2b01f828ea62ac5b3e42f',
         'd14a028c2a3a2bc9476102bb288234c415a2b01f828ea62ac5b3e42f'),
        # User id
        ('188.184.37.205', '100', None, None, datetime(2018, 1, 1, 12),
         'CH',
         '66348d03d012c50199bf8b45546ba5b405dcef3a0d4ed4a963c42327',
         '821aa7364dd24d6026b3c3f41b625011dce665025ef43d91369021fb'),
        # User id + session id + user agent, different IP address
        ('23.22.39.120', '100', 'foo', 'bar', datetime(2018, 1, 1, 12),
         'US',
         '66348d03d012c50199bf8b45546ba5b405dcef3a0d4ed4a963c42327',
         '821aa7364dd24d6026b3c3f41b625011dce665025ef43d91369021fb'),
        # User id, different hour
        ('23.22.39.120', '100', None, None, datetime(2018, 1, 1, 15),
         'US',
         '66348d03d012c50199bf8b45546ba5b405dcef3a0d4ed4a963c42327',
         '8ed64a5456e354b94363e61a9c9463cdab3a2a726471d50d3db915b6'),
        # User id, same hour different minute
        ('23.22.39.120', '100', None, None, datetime(2018, 1, 1, 15, 30),
         'US',
         '66348d03d012c50199bf8b45546ba5b405dcef3a0d4ed4a963c42327',
         '8ed64a5456e354b94363e61a9c9463cdab3a2a726471d50d3db915b6'),
        # Session id
        ('131.169.180.47', None, 'foo', None, datetime(2018, 1, 1, 12),
         'DE',
         '0808f64e60d58979fcb676c96ec938270dea42445aeefcd3a4e6f8db',
         '3bfc63b1f73736586b287873f6c40b85189f288afa94814a8888bc33'),
        # Session id + user agent
        ('131.169.180.47', None, 'foo', 'bar', datetime(2018, 1, 1, 12),
         'DE',
         '0808f64e60d58979fcb676c96ec938270dea42445aeefcd3a4e6f8db',
         '3bfc63b1f73736586b287873f6c40b85189f288afa94814a8888bc33'),
        # Session id + user agent + different hour
        ('131.169.180.47', None, 'foo', 'bar', datetime(2018, 1, 1, 15),
         'DE',
         '0808f64e60d58979fcb676c96ec938270dea42445aeefcd3a4e6f8db',
         'c754dec5421b6b766f83efb0d8e9915e9b78d877a16e61c730c3d0b5'),
        # User agent
        ('188.184.37.205', None, None, 'bar', datetime(2018, 1, 1, 12),
         'CH',
         'd9fe787765ddca6931accfa840a7f9fe6081719810f5bc241b5e6670',
         'd9fe787765ddca6931accfa840a7f9fe6081719810f5bc241b5e6670'),
        # Differnet ip address
        ('131.169.180.47', None, None, 'bar', datetime(2018, 1, 1, 12),
         'DE',
         '635e7978322f54cd01654d216712ecc795f0461773314918ac04aaf5',
         '635e7978322f54cd01654d216712ecc795f0461773314918ac04aaf5'),
        # Different hour
        ('131.169.180.47', None, None, 'bar', datetime(2018, 1, 1, 15),
         'DE',
         'fb40d9579e73650f2e91485fad5e35757ca4fd63b1bbcd4837d0efe8',
         'fb40d9579e73650f2e91485fad5e35757ca4fd63b1bbcd4837d0efe8'),
        # No result ip address
        ('0.0.0.0', None, None, None, datetime(2018, 1, 1, 12),
         None,
         'd14a028c2a3a2bc9476102bb288234c415a2b01f828ea62ac5b3e42f',
         'd14a028c2a3a2bc9476102bb288234c415a2b01f828ea62ac5b3e42f'),
    ]
)
def test_anonymize_user(ip_addess, user_id, session_id, user_agent, timestamp,
                        exp_country, exp_visitor_id, exp_unique_session_id):
    """Test anonymize_user preprocessor."""
    event = anonymize_user({
        'ip_address': ip_addess,
        'user_id': user_id,
        'session_id': session_id,
        'user_agent': user_agent,
        'timestamp': timestamp.isoformat(),
    })
    assert 'user_id' not in event
    assert 'user_agent' not in event
    assert 'ip_address' not in event
    assert 'session_id' not in event
    assert event['country'] == exp_country
    assert event['visitor_id'] == exp_visitor_id
    assert event['unique_session_id'] == exp_unique_session_id


def test_flag_robots(app, mock_user_ctx, request_headers, objects):
    """Test flag_robots preprocessor."""
    def build_event(headers):
        with app.test_request_context(headers=headers):
            event = file_download_event_builder({}, app, objects[0])
        return flag_robots(event)

    assert build_event(request_headers['user'])['is_robot'] is False
    assert build_event(request_headers['machine'])['is_robot'] is False
    assert build_event(request_headers['robot'])['is_robot'] is True


def test_flag_machines(app, mock_user_ctx, request_headers, objects):
    """Test machines preprocessor."""
    def build_event(headers):
        with app.test_request_context(headers=headers):
            event = file_download_event_builder({}, app, objects[0])
        return flag_machines(event)

    assert build_event(request_headers['user'])['is_machine'] is False
    assert build_event(request_headers['robot'])['is_machine'] is False
    assert build_event(request_headers['machine'])['is_machine'] is True


def test_referrer(app, mock_user_ctx, request_headers, objects):
    """Test referrer header."""
    request_headers['user']['REFERER'] = 'example.com'
    with app.test_request_context(headers=request_headers['user']):
        event = file_download_event_builder({}, app, objects[0])
    assert event['referrer'] == 'example.com'


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


def test_failing_processors(app, event_queues, es_with_templates, caplog):
    """Test events that raise an exception when processed."""
    es = es_with_templates
    search = Search(using=es)

    current_queues.declare()
    current_stats.publish(
        'file-download',
        [_create_file_download_event(date) for date in
         [(2018, 1, 1), (2018, 1, 2), (2018, 1, 3), (2018, 1, 4)]])

    def _raises_on_second_call(doc):
        if _raises_on_second_call.calls == 1:
            _raises_on_second_call.calls += 1
            raise Exception('mocked-exception')
        _raises_on_second_call.calls += 1
        return doc
    _raises_on_second_call.calls = 0

    queue = current_queues.queues['stats-file-download']
    indexer = EventsIndexer(queue, preprocessors=[_raises_on_second_call])

    assert get_queue_size('stats-file-download') == 4
    assert not es.indices.exists('events-stats-file-download-2018-01-01')
    assert not es.indices.exists('events-stats-file-download-2018-01-02')
    assert not es.indices.exists('events-stats-file-download-2018-01-03')
    assert not es.indices.exists('events-stats-file-download-2018-01-04')
    assert not es.indices.exists_alias(name='events-stats-file-download')

    with caplog.at_level(logging.ERROR):
        indexer.run()  # 2nd event raises exception and is dropped

    # Check that the error was logged
    error_logs = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(error_logs) == 1
    assert error_logs[0].msg == 'Error while processing event'
    assert error_logs[0].exc_info[1].args[0] == 'mocked-exception'

    es.indices.refresh(index='*')
    assert get_queue_size('stats-file-download') == 0
    assert search.index('events-stats-file-download').count() == 3
    assert search.index('events-stats-file-download-2018-01-01').count() == 1
    assert not es.indices.exists('events-stats-file-download-2018-01-02')
    assert search.index('events-stats-file-download-2018-01-03').count() == 1
    assert search.index('events-stats-file-download-2018-01-04').count() == 1
