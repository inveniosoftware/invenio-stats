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

"""Pytest configuration."""

from __future__ import absolute_import, print_function

import datetime
import os
import shutil
import sys
import tempfile
import uuid
from contextlib import contextmanager
from copy import deepcopy
from random import randrange

import pytest
from elasticsearch.exceptions import RequestError
from flask import Flask, appcontext_pushed, g
from flask.cli import ScriptInfo
from flask_celeryext import FlaskCeleryExt
from invenio_db import db as db_
from invenio_db import InvenioDB
from invenio_files_rest import InvenioFilesREST
from invenio_files_rest.models import Bucket, Location, ObjectVersion
from invenio_pidstore import InvenioPIDStore
from invenio_pidstore.minters import recid_minter
from invenio_queues import InvenioQueues
from invenio_queues.proxies import current_queues
from invenio_records import InvenioRecords
from invenio_records.api import Record
from invenio_search import InvenioSearch, current_search, current_search_client
from kombu import Exchange
from mock import MagicMock, Mock, patch
from six import BytesIO
from sqlalchemy_utils.functions import create_database, database_exists

from invenio_stats import InvenioStats
from invenio_stats.contrib.event_builders import build_file_unique_id, \
    file_download_event_builder
from invenio_stats.processors import EventsIndexer, anonymize_user, flag_robots
from invenio_stats.tasks import process_events


def mock_iter_entry_points_factory(data, mocked_group):
    """Create a mock iter_entry_points function."""
    from pkg_resources import iter_entry_points

    def entrypoints(group, name=None):
        if group == mocked_group:
            for entrypoint in data:
                yield entrypoint
        else:
            for x in iter_entry_points(group=group, name=name):
                yield x
    return entrypoints


@pytest.yield_fixture()
def event_entrypoints():
    """Declare some events by mocking the invenio_stats.events entrypoint.

    It yields a list like [{event_type: <event_type_name>}, ...].
    """
    data = []
    result = []
    for idx in range(5):
        event_type_name = 'event_{}'.format(idx)
        from pkg_resources import EntryPoint
        entrypoint = EntryPoint(event_type_name, event_type_name)
        conf = dict(event_type=event_type_name, templates='/',
                    processor_class=EventsIndexer)
        entrypoint.load = lambda conf=conf: (lambda: [conf])
        data.append(entrypoint)
        result.append(conf)

    # including file_download
    event_type_name = 'file-download'
    from pkg_resources import EntryPoint
    entrypoint = EntryPoint('invenio_files_rest', 'test_dir')
    conf = dict(event_type=event_type_name, templates='contrib/file-download',
                processor_class=EventsIndexer)
    entrypoint.load = lambda conf=conf: (lambda: [conf])
    data.append(entrypoint)

    entrypoints = mock_iter_entry_points_factory(data, 'invenio_stats.events')

    with patch('invenio_stats.ext.iter_entry_points',
               entrypoints):
        yield result


def date_range(start_date, end_date):
    """Get all dates in a given range."""
    if start_date >= end_date:
        for n in range((start_date - end_date).days + 1):
            yield end_date + datetime.timedelta(n)
    else:
        for n in range((end_date - start_date).days + 1):
            yield start_date + datetime.timedelta(n)


@pytest.yield_fixture()
def event_queues(app, event_entrypoints):
    """Delete and declare test queues."""
    current_queues.delete()
    try:
        current_queues.declare()
        yield
    finally:
        current_queues.delete()


@pytest.yield_fixture()
def base_app():
    """Flask application fixture without InvenioStats."""
    from invenio_stats.config import STATS_EVENTS
    app_ = Flask('testapp')
    stats_events = {'file-download': deepcopy(STATS_EVENTS['file-download'])}
    stats_events.update({'event_{}'.format(idx): {} for idx in range(5)})
    app_.config.update(dict(
        CELERY_ALWAYS_EAGER=True,
        CELERY_TASK_ALWAYS_EAGER=True,
        CELERY_CACHE_BACKEND='memory',
        CELERY_EAGER_PROPAGATES_EXCEPTIONS=True,
        CELERY_TASK_EAGER_PROPAGATES=True,
        CELERY_RESULT_BACKEND='cache',
        SQLALCHEMY_DATABASE_URI=os.environ.get(
            'SQLALCHEMY_DATABASE_URI', 'sqlite://'),
        SQLALCHEMY_TRACK_MODIFICATIONS=True,
        TESTING=True,
        STATS_MQ_EXCHANGE=Exchange(
            'test_events',
            type='direct',
            delivery_mode='transient',  # in-memory queue
            durable=True,
        ),
        STATS_EVENTS=stats_events,
        STATS_AGGREGATIONS={'file-download-agg': {}}
    ))
    FlaskCeleryExt(app_)
    InvenioDB(app_)
    InvenioRecords(app_)
    InvenioPIDStore(app_)
    InvenioQueues(app_)
    InvenioFilesREST(app_)
    InvenioSearch(app_, entry_point_group=None)
    with app_.app_context():
        yield app_


@pytest.yield_fixture()
def app(base_app):
    """Flask application fixture with InvenioStats."""
    InvenioStats(base_app)
    yield base_app


@pytest.yield_fixture()
def db(app):
    """Setup database."""
    if not database_exists(str(db_.engine.url)):
        create_database(str(db_.engine.url))
    db_.create_all()
    yield db_
    db_.session.remove()
    db_.drop_all()


@pytest.yield_fixture()
def es(app):
    """Provide elasticsearch access, create and clean indices."""
    current_search_client.indices.delete(index='*')
    current_search_client.indices.delete_template('*')
    list(current_search.create())
    try:
        yield current_search_client
    finally:
        current_search_client.indices.delete(index='*')
        current_search_client.indices.delete_template('*')


@pytest.fixture()
def celery(app):
    """Get queueobject for testing bulk operations."""
    return app.extensions['flask-celeryext'].celery


@pytest.fixture()
def script_info(app):
    """Get ScriptInfo object for testing CLI."""
    return ScriptInfo(create_app=lambda info: app)


@contextmanager
def user_set(app, user):
    """User set."""
    def handler(sender, **kwargs):
        g.user = user
    with appcontext_pushed.connected_to(handler, app):
        yield


@pytest.yield_fixture()
def dummy_location(db):
    """File system location."""
    tmppath = tempfile.mkdtemp()

    loc = Location(
        name='testloc',
        uri=tmppath,
        default=True
    )
    db.session.add(loc)
    db.session.commit()

    yield loc

    shutil.rmtree(tmppath)


@pytest.fixture()
def record(db):
    """File system location."""
    return Record.create({})


@pytest.fixture()
def pid(db, record):
    """File system location."""
    return recid_minter(record.id, record)


@pytest.fixture()
def bucket(db, dummy_location):
    """File system location."""
    b1 = Bucket.create()
    return b1


@pytest.yield_fixture()
def objects(bucket):
    """File system location."""
    # Create older versions first
    for key, content in [
            ('LICENSE', b'old license'),
            ('README.rst', b'old readme')]:
        ObjectVersion.create(
            bucket, key, stream=BytesIO(content), size=len(content)
        )

    # Create new versions
    objs = []
    for key, content in [
            ('LICENSE', b'license file'),
            ('README.rst', b'readme file')]:
        objs.append(
            ObjectVersion.create(
                bucket, key, stream=BytesIO(content),
                size=len(content)
            )
        )

    yield objs


@pytest.yield_fixture(scope="session")
def sequential_ids():
    """Sequential uuids for files."""
    ids = [uuid.UUID((
        '0000000000000000000000000000000' + str(i))[-32:])
        for i in range(100000)]
    yield ids


@pytest.fixture()
def mock_users():
    """Create mock users."""
    mock_auth_user = Mock()
    mock_auth_user.get_id = lambda: '123'
    mock_auth_user.is_authenticated = True

    mock_anon_user = Mock()
    mock_anon_user.is_authenticated = False
    return {
        'anonymous': mock_anon_user,
        'authenticated': mock_auth_user
    }


@pytest.yield_fixture()
def mock_user_ctx(mock_users):
    """Run in a mock authenticated user context."""
    with patch('invenio_stats.utils.current_user',
               mock_users['authenticated']):
        yield


@pytest.fixture()
def request_headers():
    """Return request headers for normal user and bot."""
    return dict(
        user={'USER_AGENT':
              'Mozilla/5.0 (Windows NT 6.1; WOW64) '
              'AppleWebKit/537.36 (KHTML, like Gecko)'
              'Chrome/45.0.2454.101 Safari/537.36'},
        robot={'USER_AGENT': 'googlebot'}
    )


@pytest.yield_fixture()
def mock_datetime():
    """Mock datetime.datetime.

    Use set_utcnow to set the current utcnow time.
    """
    class NewDate(datetime.datetime):
        _utcnow = (2017, 1, 1)

        @classmethod
        def set_utcnow(cls, value):
            cls._utcnow = value

        @classmethod
        def utcnow(cls):
            return cls(*cls._utcnow)

    yield NewDate


@pytest.yield_fixture()
def mock_event_queue(app, mock_datetime, request_headers, objects,
                     event_entrypoints, mock_user_ctx):
    """Create a mock queue containing a few file download events."""
    mock_queue = Mock()
    mock_queue.routing_key = 'stats-file-download'
    with patch('datetime.datetime', mock_datetime), \
            app.test_request_context(headers=request_headers['user']):
        events = [file_download_event_builder({}, app, objects[0]) for idx
                  in range(100)]
        mock_queue.consume.return_value = iter(events)
    # Save the queued events for later tests
    mock_queue.queued_events = deepcopy(events)
    return mock_queue


def generate_events(app, file_number=5, event_number=100, robot_event_number=0,
                    start_date=datetime.date(2017, 1, 1),
                    end_date=datetime.date(2017, 1, 7)):
    """Queued events for processing tests."""
    current_queues.declare()

    for t in current_search.put_templates(ignore=[400]):
        pass

    def _unique_ts_gen():
        ts = 0
        while True:
            ts += 1
            yield ts

    def generator_list():
        unique_ts = _unique_ts_gen()
        for file_idx in range(file_number):
            for entry_date in date_range(start_date, end_date):
                file_id = '{0}-{1}'.format(entry_date.strftime('%Y-%m-%d'),
                                           file_idx)

                def build_event(is_robot=False):
                    ts = next(unique_ts)
                    return dict(
                        timestamp=datetime.datetime.combine(
                            entry_date,
                            datetime.time(minute=ts % 60,
                                          second=ts % 60)).
                        isoformat(),
                        bucket_id=file_id,
                        file_id=file_id,
                        file_key='test.pdf',
                        visitor_id=100,
                        is_robot=is_robot
                    )

                for event_idx in range(event_number):
                    yield build_event()
                for event_idx in range(robot_event_number):
                    yield build_event(True)

    mock_queue = Mock()
    mock_queue.consume.return_value = generator_list()
    mock_queue.routing_key = 'stats-file-download'

    EventsIndexer(
        mock_queue,
        preprocessors=[
            build_file_unique_id
        ],
        double_click_window=0
    ).run()
    current_search_client.indices.refresh(index='*')


@pytest.yield_fixture()
def indexed_events(app, es, mock_user_ctx, request):
    """Parametrized pre indexed sample events."""
    for t in current_search.put_templates(ignore=[400]):
        pass
    generate_events(app=app, **request.param)
    yield


def get_deleted_docs(index):
    """Get all deleted docs from an ES index."""
    return current_search_client.indices.stats()[
        'indices'][index]['total']['docs'][
        'deleted']


def _create_file_download_event(timestamp,
                                bucket_id='B0000000000000000000000000000001',
                                file_id='F0000000000000000000000000000001',
                                file_key='test.pdf', visitor_id=100):
    """Create a file_download event content."""
    doc = dict(
        timestamp=datetime.datetime(*timestamp).isoformat(),
        # What:
        bucket_id=str(bucket_id),
        file_id=str(file_id),
        file_key=file_key,
        visitor_id=100
    )
    return build_file_unique_id(doc)
