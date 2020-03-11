# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2016-2019 CERN.
#
# Invenio is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Pytest configuration."""

from __future__ import absolute_import, print_function

import datetime
import os
import shutil
import tempfile
import uuid
from contextlib import contextmanager
from copy import deepcopy

# imported to make sure that
# login_oauth2_user(valid, oauth) is included
import invenio_oauth2server.views.server  # noqa
import pytest
from flask import Flask, appcontext_pushed, g
from flask.cli import ScriptInfo
from flask_celeryext import FlaskCeleryExt
from helpers import mock_date
from invenio_accounts import InvenioAccounts, InvenioAccountsREST
from invenio_accounts.testutils import create_test_user
from invenio_cache import InvenioCache
from invenio_db import InvenioDB
from invenio_db import db as db_
from invenio_files_rest import InvenioFilesREST
from invenio_files_rest.models import Bucket, Location, ObjectVersion
from invenio_oauth2server import InvenioOAuth2Server, InvenioOAuth2ServerREST
from invenio_oauth2server.models import Token
from invenio_pidstore import InvenioPIDStore
from invenio_pidstore.minters import recid_minter
from invenio_queues import InvenioQueues
from invenio_queues.proxies import current_queues
from invenio_records import InvenioRecords
from invenio_records.api import Record
from invenio_search import InvenioSearch, current_search, current_search_client
from kombu import Exchange
from mock import Mock, patch
from six import BytesIO
from sqlalchemy_utils.functions import create_database, database_exists

from invenio_stats import InvenioStats
from invenio_stats.contrib.config import AGGREGATIONS_CONFIG, EVENTS_CONFIG, \
    QUERIES_CONFIG
from invenio_stats.contrib.event_builders import build_file_unique_id, \
    build_record_unique_id, file_download_event_builder
from invenio_stats.processors import EventsIndexer, anonymize_user
from invenio_stats.tasks import aggregate_events
from invenio_stats.views import blueprint


@pytest.fixture()
def mock_anonymization_salt():
    """Mock the "get_anonymization_salt" function."""
    with patch('invenio_stats.processors.get_anonymization_salt',
               return_value='test-salt'):
        yield


def date_range(start_date, end_date):
    """Get all dates in a given range."""
    if start_date >= end_date:
        for n in range((start_date - end_date).days + 1):
            yield end_date + datetime.timedelta(n)
    else:
        for n in range((end_date - start_date).days + 1):
            yield start_date + datetime.timedelta(n)


@pytest.fixture()
def event_queues(app):
    """Delete and declare test queues."""
    current_queues.delete()
    try:
        current_queues.declare()
        yield
    finally:
        current_queues.delete()


@pytest.fixture()
def events_config():
    """Events config for the tests."""
    stats_events = deepcopy(EVENTS_CONFIG)
    for idx in range(5):
        event_name = 'event_{}'.format(idx)
        stats_events[event_name] = {
            'cls': EventsIndexer,
            'templates': 'invenio_stats.contrib.record_view',
        }
    return stats_events


@pytest.fixture()
def aggregations_config():
    """Aggregations config for the tests."""
    return deepcopy(AGGREGATIONS_CONFIG)


@pytest.fixture()
def queries_config(app, custom_permission_factory):
    """Queries config for the tests."""
    stats_queries = deepcopy(QUERIES_CONFIG)
    stats_queries.update({
        'test-query': dict(
            cls=CustomQuery,
            params=dict(
                index='stats-file-download',
                copy_fields=dict(
                    bucket_id='bucket_id',
                ),
                required_filters=dict(
                    bucket_id='bucket_id',
                )
            ),
            permission_factory=custom_permission_factory
        ),
        'test-query2': dict(
            cls=CustomQuery,
            params=dict(
                index='stats-file-download',
                copy_fields=dict(
                    bucket_id='bucket_id',
                ),
                required_filters=dict(
                    bucket_id='bucket_id',
                )
            ),
            permission_factory=custom_permission_factory
        )
    })

    # store the original config value
    original_value = app.config.get('STATS_QUERIES')
    app.config['STATS_QUERIES'] = stats_queries
    yield stats_queries
    # set the original value back
    app.config['STATS_QUERIES'] = original_value


@pytest.fixture()
def base_app(events_config, aggregations_config):
    """Flask application fixture without InvenioStats."""
    instance_path = tempfile.mkdtemp()
    app_ = Flask('testapp', instance_path=instance_path)
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
        # Bump the ES client timeout for slower environments (like Travis CI)
        SEARCH_CLIENT_CONFIG={'timeout': 30, 'max_retries': 5},
        TESTING=True,
        OAUTH2SERVER_CLIENT_ID_SALT_LEN=64,
        OAUTH2SERVER_CLIENT_SECRET_SALT_LEN=60,
        OAUTH2SERVER_TOKEN_PERSONAL_SALT_LEN=60,
        STATS_MQ_EXCHANGE=Exchange(
            'test_events',
            type='direct',
            delivery_mode='transient',  # in-memory queue
            durable=True,
        ),
        SECRET_KEY='asecretkey',
        SERVER_NAME='localhost',
        STATS_QUERIES={},
        STATS_EVENTS=events_config,
        STATS_AGGREGATIONS=aggregations_config,
    ))
    FlaskCeleryExt(app_)
    InvenioAccounts(app_)
    InvenioAccountsREST(app_)
    InvenioDB(app_)
    InvenioRecords(app_)
    InvenioFilesREST(app_)
    InvenioPIDStore(app_)
    InvenioCache(app_)
    InvenioQueues(app_)
    InvenioOAuth2Server(app_)
    InvenioOAuth2ServerREST(app_)
    InvenioSearch(app_, entry_point_group=None)
    with app_.app_context():
        yield app_
    shutil.rmtree(instance_path)


@pytest.fixture()
def app(base_app):
    """Flask application fixture with InvenioStats."""
    base_app.register_blueprint(blueprint)
    InvenioStats(base_app)
    yield base_app


@pytest.fixture()
def db(app):
    """Setup database."""
    if not database_exists(str(db_.engine.url)):
        create_database(str(db_.engine.url))
    db_.create_all()
    yield db_
    db_.session.remove()
    db_.drop_all()


@pytest.fixture()
def es(app):
    """Provide elasticsearch access, create and clean indices.

    Don't create template so that the test or another fixture can modify the
    enabled events.
    """
    current_search_client.indices.delete(index='*')
    current_search_client.indices.delete_template('*')
    list(current_search.create())
    list(current_search.put_templates())
    try:
        yield current_search_client
    finally:
        current_search_client.indices.delete(index='*')
        current_search_client.indices.delete_template('*')


@pytest.fixture()
def config_with_index_prefix(app):
    """Add index prefix to the app config."""
    # set the config (store the original value as well just to be sure)
    original_value = app.config.get('SEARCH_INDEX_PREFIX')
    app.config['SEARCH_INDEX_PREFIX'] = 'test-'

    yield app.config

    # set the original value back
    app.config['SEARCH_INDEX_PREFIX'] = original_value


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


@pytest.fixture()
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


@pytest.fixture()
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


@pytest.fixture(scope="session")
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


@pytest.fixture()
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
        robot={'USER_AGENT': 'googlebot'},
        machine={'USER_AGENT': 'Wget/1.14 (linux-gnu)'}
    )


@pytest.fixture()
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


@pytest.fixture()
def mock_event_queue(app, mock_datetime, request_headers, objects,
                     mock_user_ctx):
    """Create a mock queue containing a few file download events."""
    mock_queue = Mock()
    mock_queue.routing_key = 'stats-file-download'
    with patch('datetime.datetime', mock_datetime), \
            app.test_request_context(headers=request_headers['user']):
        events = [
            build_file_unique_id(
                file_download_event_builder({}, app, objects[0])
            ) for idx in range(100)
        ]
        mock_queue.consume.return_value = iter(events)
    # Save the queued events for later tests
    mock_queue.queued_events = deepcopy(events)
    return mock_queue


def generate_events(app, file_number=5, event_number=100, robot_event_number=0,
                    start_date=datetime.date(2017, 1, 1),
                    end_date=datetime.date(2017, 1, 7),
                    **kwargs):
    """Queued events for processing tests."""
    current_queues.declare()

    def _unique_ts_gen():
        ts = 0
        while True:
            ts += 1
            yield ts

    def generator_list():
        unique_ts = _unique_ts_gen()
        for file_idx in range(file_number):
            for entry_date in date_range(start_date, end_date):
                file_id = 'F000000000000000000000000000000{}'.\
                    format(file_idx + 1)
                bucket_id = 'B000000000000000000000000000000{}'.\
                    format(file_idx + 1)

                def build_event(is_robot=False):
                    ts = next(unique_ts)
                    return dict(
                        timestamp=datetime.datetime.combine(
                            entry_date,
                            datetime.time(minute=ts % 60,
                                          second=ts % 60)).
                        isoformat(),
                        bucket_id=bucket_id,
                        file_id=file_id,
                        file_key='test.pdf',
                        size=9000,
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
            build_file_unique_id, anonymize_user
        ],
        double_click_window=0
    ).run()
    current_search.flush_and_refresh(index='*')


@pytest.fixture()
def indexed_events(app, es, mock_user_ctx, request):
    """Parametrized pre indexed sample events."""
    generate_events(app=app, **request.param)
    yield


@pytest.fixture()
def aggregated_events(app, es, mock_user_ctx, request):
    """Parametrized pre indexed sample events."""
    list(current_search.put_templates(ignore=[400]))
    generate_events(app=app, **request.param)
    run_date = request.param.get(
        'run_date', request.param['end_date'].timetuple()[:3])

    with patch('invenio_stats.aggregations.datetime', mock_date(*run_date)):
        aggregate_events(['file-download-agg'])
    current_search.flush_and_refresh(index='*')
    yield


@pytest.fixture()
def users(app, db):
    """Create users."""
    user1 = create_test_user(email='info@inveniosoftware.org',
                             password='tester')
    user2 = create_test_user(email='info2@inveniosoftware.org',
                             password='tester2')

    user1.allowed_token = Token.create_personal(name='allowed_token',
                                                user_id=user1.id,
                                                scopes=[]
                                                ).access_token
    user2.allowed_token = Token.create_personal(name='allowed_token',
                                                user_id=user2.id,
                                                scopes=[]
                                                ).access_token
    return {'authorized': user1, 'unauthorized': user2}


def get_deleted_docs(index):
    """Get all deleted docs from an ES index."""
    return current_search_client.indices.stats()[
        'indices'][index]['total']['docs'][
        'deleted']


def _create_file_download_event(timestamp,
                                bucket_id='B0000000000000000000000000000001',
                                file_id='F0000000000000000000000000000001',
                                size=9000,
                                file_key='test.pdf',
                                visitor_id=100,
                                user_id=None):
    """Create a file_download event content."""
    doc = dict(
        timestamp=datetime.datetime(*timestamp).isoformat(),
        # What:
        bucket_id=str(bucket_id),
        file_id=str(file_id),
        file_key=file_key,
        size=size,
        visitor_id=visitor_id,
        user_id=user_id,
    )
    return build_file_unique_id(doc)


def _create_record_view_event(timestamp,
                              record_id='R0000000000000000000000000000001',
                              pid_type='recid',
                              pid_value='1',
                              visitor_id=100,
                              user_id=None):
    """Create a file_download event content."""
    doc = dict(
        timestamp=datetime.datetime(*timestamp).isoformat(),
        # What:
        record_id=record_id,
        pid_type=pid_type,
        pid_value=pid_value,
        visitor_id=visitor_id,
        user_id=user_id,
    )
    return build_record_unique_id(doc)


@pytest.fixture()
def custom_permission_factory(users):
    """Test denying permission factory."""
    def permission_factory(query_name, params, *args, **kwargs):
        permission_factory.query_name = query_name
        permission_factory.params = params
        from flask_login import current_user
        if current_user.is_authenticated and \
                current_user.id == users['authorized'].id:
            return type('Allow', (), {'can': lambda self: True})()
        return type('Deny', (), {'can': lambda self: False})()
    permission_factory.query_name = None
    permission_factory.params = None
    return permission_factory


@pytest.fixture()
def sample_histogram_query_data():
    """Sample query parameters."""
    yield {"mystat":
           {"stat": "bucket-file-download-histogram",
            "params": {"start_date": "2017-1-1",
                       "end_date": "2017-7-1",
                       "interval": "day",
                       "bucket_id":
                       "B0000000000000000000000000000001",
                       "file_key": "test.pdf"
                       }
            }
           }


class CustomQuery:
    """Mock query class."""

    def __init__(self, *args, **kwargs):
        """Mock constructor."""
        pass

    def run(self, *args, **kwargs):
        """Sample response."""
        return dict(bucket_id='test_bucket',
                    value=100)
