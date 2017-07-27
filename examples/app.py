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

"""Minimal Flask application example for development.

Run example development server:

.. code-block:: console

   $ cd examples
   $ flask -a app.py --debug run
"""

from __future__ import absolute_import, print_function

import os.path
import random
from datetime import datetime, timedelta

from flask import Flask
from invenio_queues import InvenioQueues
from invenio_rest import InvenioREST
from invenio_search import InvenioSearch, current_search_client

from invenio_stats import InvenioStats
from invenio_stats.proxies import current_stats
from invenio_stats.tasks import aggregate_events, process_events
from invenio_stats.views import blueprint

# Create Flask application
# TODO
app = Flask(__name__)
app.config.update(dict(
    BROKER_URL='redis://',
    CELERY_RESULT_BACKEND='redis://',
    DATADIR=os.path.join(os.path.dirname(__file__), 'data'),
    FILES_REST_MULTIPART_CHUNKSIZE_MIN=4,
    REST_ENABLE_CORS=True,
    SECRET_KEY='CHANGEME',
    SQLALCHEMY_ECHO=False,
    SQLALCHEMY_DATABASE_URI=os.environ.get(
        'SQLALCHEMY_DATABASE_URI', 'sqlite:///test.db'
    ),
    SQLALCHEMY_TRACK_MODIFICATIONS=True,
))

InvenioREST(app)
InvenioStats(app)
InvenioQueues(app)
InvenioSearch(app)

app.register_blueprint(blueprint)


@app.cli.group()
def fixtures():
    """Command for working with test data."""


def publish_filedownload(nb_events, user_id, filename,
                         file_id, bucket_id, date):
    current_stats.publish('file-download', [dict(
        # When:
        timestamp=date.isoformat(),
        # What:
        bucket_id=str(bucket_id),
        file_id=str(file_id),
        filename=filename,
        # Who:
        user_id=str(user_id)
    )] * nb_events)


@fixtures.command()
def events():
    # Create events
    nb_days = 20
    day = datetime(2016, 12, 1, 10, 11, 12)
    max_events = 10
    random.seed(42)
    for _ in range(nb_days):
        publish_filedownload(random.randrange(1, max_events),
                             1, 'test.txt', 10, 20, day)
        day = day + timedelta(days=1)

    process_events(['file-download'])
    # flush elasticsearch indices so that the events become searchable
    current_search_client.indices.flush(index='*')


@fixtures.command()
def aggregations():
    aggregate_events(['file-download-agg'])
    # flush elasticsearch indices so that the aggregations become searchable
    current_search_client.indices.flush(index='*')
