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

"""Test celery tasks."""

from __future__ import absolute_import, print_function

import datetime
import uuid

from elasticsearch_dsl import Search
from invenio_search import current_search, current_search_client

from invenio_stats import current_stats
from invenio_stats.tasks import aggregate_events, process_events


def test_process_events(app, event_entrypoints):
    """Test process event."""
    current_stats.publish('file-download', [dict(data='val')])
    process_events.delay(['file-download'])


# def test_aggregate_events(app):
#     """Test process event."""
#     for t in current_search.put_templates(ignore=[400]):
#         pass

#     ids = [uuid.UUID((
#            '0000000000000000000000000000000' + str(i))[-32:])
#            for i in range(10)]
#     current_stats.publish('file-download',
#                           [dict(
#                            timestamp=datetime.datetime.utcnow().isoformat(),
#                            bucket_id=str(ids[0]),
#                            file_id=str(ids[1]),
#                            filename='test.pdf',
#                            visitor_id=100)])
#     process_events.delay(['file-download'])
#     current_search_client.indices.flush(index='*')
#     aggregate_events.delay(['file-download-agg'])
#     current_search_client.indices.flush(index='*')
#     query = Search(using=current_search_client,
#                    index='stats-file-download').sort('file_id')
#     result = query.execute()[0]
#     assert result.file_id == str(ids[1])
#     assert result.count == 1

#     current_search_client.indices.delete(index='events-stats-file-download')
#     current_search_client.indices.delete(index='stats-file-download')
