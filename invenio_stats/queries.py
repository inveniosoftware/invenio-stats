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
# along with Invenio; if not, write to the
# Free Software Foundation, Inc., 59 Temple Place, Suite 330, Boston,
# MA 02111-1307, USA.
#
# In applying this license, CERN does not
# waive the privileges and immunities granted to it by virtue of its status
# as an Intergovernmental Organization or submit itself to any jurisdiction.

"""Query processing classes."""

from invenio_search import current_search_client


class ESQuery(object):
    """Elasticsearch query."""

    def __init__(self, query_name, doc_type, index, client=None,
                 *args, **kwargs):
        """Constructor.

        :param doc_type: queried document type.
        :param index: queried index.
        :param client: elasticsearch client used to query.
        """
        super(ESQuery, self).__init__()
        self.index = index
        self.client = client or current_search_client
        self.query_name = query_name
        self.doc_type = doc_type

    def run(self, *args, **kwargs):
        """Run the query."""
        raise NotImplementedError()
