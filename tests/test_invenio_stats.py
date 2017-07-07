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
import time
import uuid

from elasticsearch_dsl import Search
from flask import Flask
from invenio_queues.proxies import current_queues
from invenio_search import current_search_client
from mock import Mock, patch

from invenio_stats import InvenioStats
from invenio_stats.proxies import current_stats
from invenio_stats.tasks import StatAggregator


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
    event_type = 'event_0'
    events = [{"payload": "test {}".format(idx)} for idx in range(3)]
    current_queues.declare()
    current_stats.publish(event_type, events)
    assert list(current_stats.consume(event_type)) == events


# def test_register_events(app, event_entrypoints, objects):
#     from invenio_files_rest.signals import file_downloaded
#     from invenio_files_rest.models import ObjectVersion

#     mock_user = Mock()
#     mock_user.get_id = lambda: '123'
#     mock_user.is_authenticated = True
#     current_queues.declare()

#     with patch('invenio_stats.utils.current_user', mock_user):
#         with app.test_request_context(
#             headers={'USER_AGENT':
#                      'Mozilla/5.0 (Windows NT 6.1; WOW64) '
#                      'AppleWebKit/537.36 (KHTML, like Gecko)'
#                      'Chrome/45.0.2454.101 Safari/537.36'}):
#             ids = [uuid.uuid1() for i in range(len(objects))]
#             for i in range(len(objects)):
#                 file_obj = objects[i]
#                 file_obj.bucket_id = ids[i]
#                 print("sending bucket id:", file_obj.bucket_id)
#                 file_downloaded.send(app, obj=file_obj)
#     with app.app_context():
#         from invenio_stats.tasks import process_events
#         process_events(['file-download'])
#     time.sleep(10)
#     for _id in ids:
#         query = Search(using=current_search_client,
#                        index='events-stats-file-download').\
#             query('term', bucket_id=_id)
#         assert query.execute().hits.total == 1


# def test_aggregate(app):
#     from invenio_stats.tasks import aggregate_event
#     aggregate_event.delay('file-download')
#     aggregate_event.delay('record-view')


# def test_record_views(app):
#     pass


# def test_querying(app):
#     fds = StatAggregator(current_search_client, 'file-download', 'file_id')
#     print(fds.query('bucket', '8004'))
#     print(fds.query())
#     print(fds.query('bucket', '8004', 'week'))


# def test_get_count_by_day(app):
#     fds = StatAggregator(current_search_client, 'file-download', 'file_id')
#     print(fds.get_count_by_day(datetime.date.today()))


# def test_get_count_in_day_range(app):
#     fds = StatAggregator(current_search_client, 'file-download', 'file_id')
#     print(fds.get_count_in_day_range(
#         [datetime.datetime.strptime('2017-07-10', '%Y-%m-%d').date(),
#          datetime.datetime.strptime('2017-07-13', '%Y-%m-%d').date()]))
# def test_set_last_successful_aggregation_date(app):
#     doc = {
#         'event': 'file_download',
#         'date': '04.07.2017',
#     }
#     res = current_search_client.index(index="daily-file_download-{}".format(
#                                         ,
#                                       doc_type='bookmark', id=1, body=doc)
#     import ipdb
#     ipdb.set_trace()
