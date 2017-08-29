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
from uuid import uuid1
from conftest import _create_file_download_event
from invenio_queues.proxies import current_queues
from mock import patch, Mock

from invenio_stats.contrib.event_builders import file_download_event_builder
from invenio_stats.processors import EventsIndexer, anonymize_user, \
    flag_robots, skip_deposit
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
        return event

    def test_preprocessor2(event):
        event['test2'] = 21
        return event

    indexer = EventsIndexer(
        mock_event_queue,
        preprocessors=[test_preprocessor1, test_preprocessor2]
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
        event = test_preprocessor1(event)
        event = test_preprocessor2(event)
        expected_docs.append(dict(
            _id='None-None-2017-01-01T00:00:00',
            _op_type='index',
            _index='events-stats-file-download-2017-01-01',
            _type='stats-file-download',
            _source=event,
        ))

    assert received_docs == expected_docs


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
    es.indices.flush(index='*')
    res = es.search(
        index='events-stats-file-download-2000-06-01',
    )
    assert res['hits']['total'] == 2


def test_deposits_are_skipped(app, mock_user_ctx,
                              request_headers, objects, dummy_location):
    """Test that events coming from deposits are skipped."""
    def build_event(obj):
        with app.test_request_context(headers=request_headers['user']):
            event = file_download_event_builder({}, app, obj)
        return skip_deposit(event)

    from invenio_records_files.models import RecordsBuckets
    from invenio_records.models import RecordMetadata
    from invenio_files_rest.models import Bucket
    from invenio_db import db

    rec_uuid1 = uuid1(1)
    buck_uuid1 = uuid1(2)
    dep_uuid1 = uuid1(3)
    buck_uuid2 = uuid1(4)
    rec_buck1 = RecordsBuckets(record_id=str(rec_uuid1),
                               bucket_id=str(buck_uuid1))
    rec_meta1 = RecordMetadata(
        id=str(rec_uuid1),
        json={'$schema': 'http://localhost:5000/.../json_schema'})
    rec_buck2 = RecordsBuckets(record_id=str(dep_uuid1),
                               bucket_id=str(buck_uuid2))
    rec_meta2 = RecordMetadata(id=str(dep_uuid1),
                               json={'$schema': '#/draft_json_schema'})
    buck1 = Bucket.create(id=buck_uuid1)
    buck2 = Bucket.create(id=buck_uuid2)
    db.session.add(rec_meta1)
    db.session.add(rec_meta2)
    db.session.add(buck1)
    db.session.add(buck2)
    db.session.add(rec_buck1)
    db.session.add(rec_buck2)
    db.session.commit()

    rec = Mock()
    rec.bucket_id = buck_uuid1
    dep = Mock()
    dep.bucket_id = buck_uuid2
    print(build_event(dep))
    assert build_event(dep) is None
    print(build_event(rec))
    assert build_event(rec) is not None
