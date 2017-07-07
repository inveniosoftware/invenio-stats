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
# along with Invenio; if not,2 write to the
# Free Software Foundation, Inc., 59 Temple Place, Suite 330, Boston,
# MA 02111-1307, USA.
#
# In applying this license, CERN does not
# waive the privileges and immunities granted to it by virtue of its status
# as an Intergovernmental Organization or submit itself to any jurisdiction.

"""Events indexer."""

from __future__ import absolute_import, print_function

import arrow
from elasticsearch.helpers import bulk
from flask import current_app
from invenio_search import current_search_client
from robot_detection import is_robot

from .utils import anonimize_user


class EventsIndexer(object):
    """Simple events indexer.

    Subclass this class in order to provide custom indexing behaviour.
    """

    def __init__(self, queue, prefix='events', suffix='%Y-%m-%d', client=None):
        """Initialize indexer."""
        self.queue = queue
        self.client = client or current_search_client
        self.doctype = queue.routing_key
        self.index = '{0}-{1}'.format(prefix, self.queue.routing_key)
        self.suffix = suffix

    def process_event(self, data):
        """Process data from a single event."""
        if 'invenio-collections' in current_app.extensions:
            print("add collection")
        if 'invenio-communities' in current_app.extensions:
            print("add community info")
        return anonimize_user(data)

    def actionsiter(self):
        """Iterator."""
        for msg in self.queue.consume():
            if 'user_agent' in msg and is_robot(msg['user_agent']):
                continue

            suffix = arrow.get(msg.get('timestamp')).strftime(self.suffix)
            suffix = '2017-07-14'
            yield dict(
                _op_type='index',
                _index='{0}-{1}'.format(self.index, suffix),
                _type=self.doctype,
                _source=self.process_event(msg),
            )

    def run(self):
        """Process events queue."""
        return bulk(
            self.client,
            self.actionsiter(),
            stats_only=True,
        )
