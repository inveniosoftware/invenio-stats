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

"""InvenioStats views."""

from flask import Blueprint, jsonify, request
from invenio_rest.views import ContentNegotiatedMethodView

from .errors import InvalidRequestInputError, UnknownQueryError
from .proxies import current_stats

blueprint = Blueprint(
    'invenio_stats',
    __name__,
    url_prefix='/stats',
)

# def serializer(data, *args, **kwargs):
#     return jsonify(data)


class StatsQueryResource(ContentNegotiatedMethodView):
    """REST API resource providing access to statistics."""

    view_name = 'stat_query'

    def __init__(self, **kwargs):
        """Constructor."""
        super(StatsQueryResource, self).__init__(
            serializers={
                'application/json':
                lambda data, *args, **kwargs: jsonify(data),
            },
            default_method_media_type={
                'GET': 'application/json',
            },
            default_media_type='application/json',
            **kwargs)

    def get(self, **kwargs):
        """Get a community's metadata."""
        config = request.get_json(force=False)
        if config is None:
            config = {}
        if config is None or not isinstance(config, dict):
            raise InvalidRequestInputError(
                'Invalid Input. It should be of the form '
                '\{ STATISTIC_NAME: \{ PARAMETERS \}\}'
            )
        result = {}
        for query_name, params in config.items():
            try:
                query_cfg = current_stats.queries[query_name]
            except KeyError:
                raise UnknownQueryError(query_name)

            # Check that the user is allowed to ask for this statistic
            permission = current_stats.permission_factory(query_name, params)
            if permission is not None and not permission.can():
                message = ('You do not have a permission to query the '
                           'statistic "{}" with those '
                           'parameters'.format(query_name))
                if current_user.is_authenticated:
                    abort(403, message)
                abort(401, message)
            try:
                result[query_name] = query_cfg.query_class(
                    query_name=query_name, **query_cfg.query_config
                ).run(**params)
                pass
            except ValueError as e:
                raise InvalidRequestInputError(e.args[0])
        return self.make_response(result)


stats_view = StatsQueryResource.as_view(
    StatsQueryResource.view_name,
)

blueprint.add_url_rule(
    '',
    view_func=stats_view,
)
