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

import hashlib

import arrow
import elasticsearch
from flask import current_app
from invenio_search import current_search_client
from robot_detection import is_robot

from .utils import obj_or_import_string


def anonymize_user(doc):
    """Preprocess an event by anonymizing user information."""
    ip = doc.pop('ip_address', None)
    if ip:
        doc.update(get_geoip(ip))

    uid = doc.pop('user_id', '')
    ua = doc.pop('user_agent', '')

    m = hashlib.sha224()
    # TODO: include random salt here, that changes once a day.
    # m.update(random_salt)
    if uid:
        m.update(uid.encode('utf-8'))
    elif ua:
        m.update(ua)
    else:
        # TODO: add random data?
        pass

    doc.update(dict(
        visitor_id=m.hexdigest()
    ))

    return doc


def flag_robots(doc):
    """Flag events which are created by robots."""
    doc['is_robot'] = 'user_agent' in doc and is_robot(doc['user_agent'])
    return doc


class EventsIndexer(object):
    """Simple events indexer.

    Subclass this class in order to provide custom indexing behaviour.
    """

    default_preprocessors = [flag_robots, anonymize_user]
    """Default preprocessors ran on every event."""

    def __init__(self, queue, prefix='events', suffix='%Y-%m-%d', client=None,
                 preprocessors=None):
        """Initialize indexer.

        :param preprocessors: a list of functions which are called on every
            event before it is indexed. Each function should return the
            processed event. If it returns None, the event is filtered and
            won't be indexed.
        """
        self.queue = queue
        self.client = client or current_search_client
        self.doctype = queue.routing_key
        self.index = '{0}-{1}'.format(prefix, self.queue.routing_key)
        self.suffix = suffix
        # load the preprocessors
        self.preprocessors = [
            obj_or_import_string(preproc) for preproc in preprocessors
        ] if preprocessors is not None else self.default_preprocessors

    def actionsiter(self):
        """Iterator."""
        for msg in self.queue.consume():
            for preproc in self.preprocessors:
                msg = preproc(msg)
                if msg is None:
                    break
            if msg is None:
                continue
            suffix = arrow.get(msg.get('timestamp')).strftime(self.suffix)
            yield dict(
                _op_type='index',
                _index='{0}-{1}'.format(self.index, suffix),
                _type=self.doctype,
                _source=msg,
            )

    def run(self):
        """Process events queue."""
        return elasticsearch.helpers.bulk(
            self.client,
            self.actionsiter(),
            stats_only=True,
            chunk_size=50
        )
