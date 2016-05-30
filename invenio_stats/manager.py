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

"""Elasticsearch template manager."""

from __future__ import absolute_import, print_function

import json

from invenio_search import current_search_client
from pkg_resources import resource_string


class IndexManager(object):
    """Index manager."""


class IndexTemplate(object):
    """Index template."""

    def __init__(self, event_type, name, package_name, client=None):
        """"."""
        self.client = client or current_search_client
        self.event_type = event_type
        self.name = name
        self.package_name = package_name

    @property
    def body(self):
        """Read body from file."""
        return json.loads(resource_string(
            self.package_name,
            '{0}.json'.format(self.event_type)
        ).decode('utf8'))

    def create(self, ignore=None):
        """"Create template."""
        ignore = ignore or []

        return self.client.indices.put_template(
            name=self.name,
            body=self.body,
            ignore=ignore,
        )

    def delete(self, ignore=None):
        """"Create template."""
        ignore = ignore or []

        return self.client.indices.delete_template(
            name=self.name,
            ignore=ignore,
        )
