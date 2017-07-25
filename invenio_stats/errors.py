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

"""Errors used in Invenio-Stats."""

from __future__ import absolute_import, print_function

from invenio_rest.errors import RESTException

##
#  Events errors
##


class DuplicateEventError(Exception):
    """Error raised when a duplicate event is detected."""


class UnknownEventError(Exception):
    """Error raised when an unknown event is detected."""


class UnknownAggregationError(Exception):
    """Error raised when an unknown  is detected."""


class DuplicateAggregationError(Exception):
    """Error raised when a duplicate aggregation is detected."""


##
#  Aggregation errors
##

class NotSupportedInterval(Exception):
    """Error raised for an unsupported aggregation interval."""


##
#  Query errors
##

class InvalidRequestInputError(RESTException):
    """Error raised when the request input is invalid."""

    code = 400


class UnknownQueryError(RESTException):
    """Error raised when the request input is invalid."""

    def __init__(self, query_name):
        """Constructor.

        :param query_name: name of the unknown query.
        """
        super(RESTException, self).__init__()
        self.query_name = query_name
        self.description = 'Unknown statistic "{}"'.format(query_name)
    code = 400
