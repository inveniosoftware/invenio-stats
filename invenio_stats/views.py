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

"""Invenio extension to syncronize with services."""

# TODO: This is an example file. Remove it if you do not need it, including
# the templates and static folders as well as the test case.

from __future__ import absolute_import, print_function
from flask import Blueprint, make_response, request
from invenio_rest import ContentNegotiatedMethodView
from .serializers import stat_data_to_json_serializer
from .tasks import StatAggregator
from invenio_search import current_search_client
from functools import wraps


def create_blueprint(endpoints):
    """Create Invenio-Records-REST blueprint.

    :params endpoints: Dictionary representing the endpoints configuration.
    :returns: Configured blueprint.
    """
    blueprint = Blueprint(
        'stats',
        __name__,
        url_prefix=''
    )
    for endpoint, options in (endpoints or {}).items():
        for rule in create_url_rules(endpoint, **options):
            blueprint.add_url_rule(**rule)
    return blueprint


def pass_event(f):
    """Decorator to retrieve persistent identifier and record."""
    @wraps(f)
    def inner(self, *args, **kwargs):
        import ipdb
        ipdb.set_trace()
        event = request.view_args['event']
        obj_id = request.view_args['obj_id']
        # event, obj_id = request
        return f(self, event=event, obj_id=obj_id)

    return inner


def create_url_rules(endpoint, event_route=None,
                     event_stats_route=None,
                     event_class=None,
                     stats_serializers=None,
                     default_media_type=None):
    """Create Werkzeug URL rules.

    :param endpoint: Name of endpoint.
    :param list_route: Record listing URL route. Required.
    """
    event_view = EventStatsResource.as_view(
        EventStatsResource.view_name.format(endpoint),
    )

    views = [
        dict(rule=event_stats_route, view_func=event_view),
    ]

    return views


class EventStatsResource(ContentNegotiatedMethodView):

    view_name = 'event_stats_item'

    def __init__(self, **kwargs):
        """Constructor."""
        super(EventStatsResource, self).__init__(
            serializers={
                'application/json': stat_data_to_json_serializer,
            },
            default_method_media_type={
                'GET': 'application/json',
            },
            default_media_type='application/json',
            **kwargs)

    @pass_event
    def get(self, event, obj_id, **kwargs):
        """Get a community's metadata."""
        # check the ETAG
        # self.check_etag(str(community.updated))

        import ipdb
        ipdb.set_trace()
        stats_api = StatAggregator(current_search_client, event)
        return self.make_response(stats_api.get_total_count())
