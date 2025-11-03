# SPDX-FileCopyrightText: 2017-2018 CERN.
# SPDX-FileCopyrightText: 2022 TU Wien.
# SPDX-License-Identifier: MIT

"""Errors used in Invenio-Stats."""

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


class DuplicateQueryError(Exception):
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

    def __init__(self, description, **kwargs):
        """Initialize exception."""
        super(RESTException, self).__init__(**kwargs)
        self.description = description


class UnknownQueryError(RESTException):
    """Error raised when the request input is invalid."""

    def __init__(self, query_name):
        """Constructor.

        :param query_name: name of the unknown query.
        """
        super(RESTException, self).__init__()
        self.query_name = query_name
        self.description = f"Unknown statistic '{query_name}'"

    code = 400
