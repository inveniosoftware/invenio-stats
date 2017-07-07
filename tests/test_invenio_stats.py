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

from elasticsearch_dsl import Search
from flask import Flask
from invenio_queues.proxies import current_queues
from invenio_search import current_search_client
from invenio_stats import InvenioStats
from invenio_stats.proxies import current_stats
from mock import Mock, patch

import uuid


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


def test_batch_events(app, event_entrypoints):
    from invenio_files_rest.signals import file_downloaded
    from invenio_files_rest.models import ObjectVersion
    # with user_set(app, my_user):
    #     with app.test_client() as c:
    #         resp = c.get('/users/me')
    #         data = json.loads(resp.data)
    #         self.assert_equal(data['username'], my_user.username)
    mock_user = Mock()
    mock_user.get_id = lambda: '123'
    mock_user.is_authenticated = True
    current_queues.declare()
    with app.app_context():
        file_objs = ObjectVersion.query.all()
    import ipdb
    ipdb.set_trace()
    with patch('invenio_stats.utils.current_user', mock_user):
        with app.test_request_context(headers={'USER_AGENT':
            'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko)'
            'Chrome/45.0.2454.101 Safari/537.36'}):
            ids = [uuid.uuid1() for i in range(1024 * len(file_objs))]
            print(ids)
            for i in range(100024):
                for j in range(len(file_objs)):
                    file_obj = file_objs[j]
                    file_obj.bucket_id = ids[i + j]
                    print("sending bucket id:", file_obj.bucket_id)
                    file_downloaded.send(app, obj=file_obj)
    import ipdb
    ipdb.set_trace()


def test_aggregate(app):
    from invenio_stats.tasks import aggregate_event
    aggregate_event.delay('file_download')
    import ipdb
    ipdb.set_trace()

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
