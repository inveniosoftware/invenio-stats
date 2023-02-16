# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2016-2018 CERN.
#
# Invenio is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Utilities for Invenio-Stats."""

from __future__ import absolute_import, print_function

import os
from base64 import b64encode
from math import ceil

import six
from dateutil.parser import parse as dateutil_parse
from elasticsearch import VERSION as ES_VERSION
from elasticsearch_dsl import Search
from flask import current_app, request, session
from flask_login import current_user
from geolite2 import geolite2
from invenio_cache import current_cache
from werkzeug.utils import import_string


def get_anonymization_salt(ts):
    """Get the anonymization salt based on the event timestamp's day."""
    salt_key = 'stats:salt:{}'.format(ts.date().isoformat())
    salt = current_cache.get(salt_key)
    if not salt:
        salt_bytes = os.urandom(32)
        salt = b64encode(salt_bytes).decode('utf-8')
        current_cache.set(salt_key, salt, timeout=60 * 60 * 24)
    return salt


def get_bucket_size(client, index, agg_field, start_date=None, end_date=None):
    """Function to help us define the size for our search query.

    :param client: search client
    :param str index: prefixed search index
    :param str agg_field: aggregation field
    :param str start_date: string containing a formatted start date
    :param str end_date: string containing a formatted end date

    """
    time_range = {}
    if start_date is not None:
        time_range['gte'] = start_date
    if end_date is not None:
        time_range['lte'] = end_date

    search = Search(using=client, index=index)
    if time_range:
        search = search.filter('range', timestamp=time_range)
    search.aggs.metric('unique_values', 'cardinality', field=agg_field)

    result = search.execute()
    unique_values = result.aggregations.unique_values.value

    # NOTE: we increase the count by 10% in order to be safe
    return int(ceil(unique_values + unique_values * 0.1))


def get_geoip(ip):
    """Lookup country for IP address."""
    reader = geolite2.reader()
    ip_data = reader.get(ip) or {}
    return ip_data.get('country', {}).get('iso_code')


def get_user():
    """User information.

    .. note::

       **Privacy note** A users IP address, user agent string, and user id
       (if logged in) is sent to a message queue, where it is stored for about
       5 minutes. The information is used to:

       - Detect robot visits from the user agent string.
       - Generate an anonymized visitor id (using a random salt per day).
       - Detect the users host contry based on the IP address.

       The information is then discarded.
    """
    return dict(
        ip_address=request.remote_addr,
        user_agent=request.user_agent.string,
        user_id=(
            current_user.get_id() if current_user.is_authenticated else None
        ),
        session_id=session.get('sid_s')
    )


def obj_or_import_string(value, default=None):
    """Import string or return object.

    :params value: Import path or class object to instantiate.
    :params default: Default object to return if the import fails.
    :returns: The imported object.
    """
    if isinstance(value, six.string_types):
        return import_string(value)
    elif value:
        return value
    return default


def load_or_import_from_config(key, app=None, default=None):
    """Load or import value from config.

    :returns: The loaded value.
    """
    app = app or current_app
    imp = app.config.get(key)
    return obj_or_import_string(imp, default=default)


AllowAllPermission = type('Allow', (), {
    'can': lambda self: True,
    'allows': lambda *args: True,
})()


def default_permission_factory(query_name, params):
    """Default permission factory.

    It enables by default the statistics if they don't have a dedicated
    permission factory.
    """
    from invenio_stats import current_stats
    if current_stats.queries[query_name].permission_factory is None:
        return AllowAllPermission
    else:
        return current_stats.queries[query_name].permission_factory(
            query_name, params
        )
