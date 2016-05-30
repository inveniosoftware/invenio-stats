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

import os
import shutil
import tempfile

import pytest
from elasticsearch.exceptions import RequestError
from flask import Flask
from flask_celeryext import FlaskCeleryExt
from flask_cli import FlaskCLI, ScriptInfo
from invenio_db import db as db_
from invenio_db import InvenioDB
from invenio_files_rest import InvenioFilesREST
from invenio_files_rest.models import Location
from invenio_pidstore import InvenioPIDStore
from invenio_records import InvenioRecords
from invenio_records_ui import InvenioRecordsUI
from invenio_search import InvenioSearch, current_search, current_search_client
from sqlalchemy_utils.functions import create_database, database_exists

from invenio_stats import EventQueue, InvenioStats, current_stats


@pytest.yield_fixture(scope='session')
def instance_path():
    """Default instance path."""
    path = tempfile.mkdtemp()

    yield path

    shutil.rmtree(path)


@pytest.fixture(scope='session')
def env_config(instance_path):
    """Default instance path."""
    os.environ.update(
        APP_INSTANCE_PATH=os.environ.get(
            'INSTANCE_PATH', instance_path),
    )

    return os.environ


@pytest.fixture(scope='session')
def config(request):
    """Default configuration."""
    # Parameterize application.
    return dict(
        BROKER_URL=os.environ.get('BROKER_URL', 'memory://'),
        CELERY_ALWAYS_EAGER=True,
        CELERY_CACHE_BACKEND="memory",
        CELERY_EAGER_PROPAGATES_EXCEPTIONS=True,
        CELERY_RESULT_BACKEND="cache",
        SQLALCHEMY_DATABASE_URI=os.environ.get(
            'SQLALCHEMY_DATABASE_URI', 'sqlite:///test.db'),
        SQLALCHEMY_TRACK_MODIFICATIONS=True,
        TESTING=True,
    )


@pytest.yield_fixture()
def app(env_config, config, instance_path):
    """Flask application fixture."""
    app_ = Flask('testapp', instance_path=instance_path)
    app_.config.update(**config)
    FlaskCLI(app_)
    FlaskCeleryExt(app_)
    InvenioDB(app_)
    InvenioRecords(app_)
    InvenioRecordsUI(app_)
    InvenioPIDStore(app_)
    InvenioStats(app_)
    InvenioFilesREST(app_)
    InvenioSearch(app_, entry_point_group=None)
    # search.register_mappings('records', 'data')

    with app_.app_context():
        yield app_


@pytest.yield_fixture()
def db(app):
    """Setup database."""
    if not database_exists(str(db_.engine.url)):
        create_database(str(db_.engine.url))
    db_.create_all()
    yield db_
    db_.session.remove()
    db_.drop_all()


# @pytest.yield_fixture()
# def es(app):
#     """Provide elasticsearch access."""
#     try:
#         list(current_search.create())
#     except RequestError:
#         list(current_search.delete(ignore=[400, 404]))
#         list(current_search.create())
#     current_search_client.indices.refresh()
#     yield current_search_client
#     list(current_search.delete(ignore=[404]))


# @pytest.yield_fixture()
# def location(db):
#     """File system location."""
#     tmppath = tempfile.mkdtemp()

#     loc = Location(
#         name='testloc',
#         uri=tmppath,
#         default=True
#     )
#     db.session.add(loc)
#     db.session.commit()

#     yield loc

#     shutil.rmtree(tmppath)


@pytest.fixture()
def exchange(app):
    """Get queueobject for testing bulk operations."""
    return app.config['STATS_MQ_EXCHANGE']


@pytest.fixture()
def celery(app):
    """Get queueobject for testing bulk operations."""
    return app.extensions['flask-celeryext'].celery


@pytest.fixture()
def event_queue(app, exchange, celery):
    """Get queueobject for testing bulk operations."""
    queue = EventQueue(exchange, 'test-event')
    with celery.pool.acquire(block=True) as conn:
        queue.queue(conn).declare()
        queue.queue(conn).purge()

    return queue


@pytest.fixture()
def script_info(app):
    """Get ScriptInfo object for testing CLI."""
    return ScriptInfo(create_app=lambda info: app)
