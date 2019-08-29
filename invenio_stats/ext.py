# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2016-2019 CERN.
#
# Invenio is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Invenio module for collecting statistics."""

from __future__ import absolute_import, print_function

from collections import namedtuple

from invenio_queues.proxies import current_queues
from werkzeug.utils import cached_property

from . import config
from .errors import DuplicateAggregationError, DuplicateEventError, \
    DuplicateQueryError, UnknownAggregationError, UnknownEventError, \
    UnknownQueryError
from .processors import EventsIndexer
from .receivers import register_receivers
from .utils import load_or_import_from_config, obj_or_import_string

_Event = namedtuple(
    'Event', ['name', 'queue', 'templates', 'cls', 'params'])

_Aggregation = namedtuple(
    'Aggregation', ['name', 'templates', 'cls', 'params'])

_Query = namedtuple(
    'Query', ['name', 'cls', 'permission_factory', 'params'])


class _InvenioStatsState(object):
    """State object for Invenio stats."""

    def __init__(self, app):
        self.app = app
        self.exchange = app.config['STATS_MQ_EXCHANGE']

    @property
    def events_config(self):
        return self.app.config['STATS_EVENTS']

    @property
    def aggregations_config(self):
        return self.app.config['STATS_AGGREGATIONS']

    @property
    def queries_config(self):
        return self.app.config['STATS_QUERIES']

    @cached_property
    def events(self):
        """Configured events."""
        result = {}
        for name, event in self.events_config.items():
            event = obj_or_import_string(event)
            if callable(event):
                event = event(self.app)

            queue = current_queues.queues['stats-{}'.format(name)]
            result[name] = _Event(
                name=name,
                queue=queue,
                templates=event['templates'],
                cls=obj_or_import_string(event['cls']),
                params=dict(queue=queue, **event.get('params', {})),
            )
        return result

    @cached_property
    def aggregations(self):
        """Configured aggregations."""
        result = {}
        for name, agg in self.aggregations_config.items():
            agg = obj_or_import_string(agg)
            if callable(agg):
                agg = agg(self.app)

            result[name] = _Aggregation(
                name=name,
                templates=agg['templates'],
                cls=obj_or_import_string(agg['cls']),
                params=agg.get('params', {})
            )
        return result

    @cached_property
    def queries(self):
        """Configured queries."""
        result = {}
        for name, query in self.queries_config.items():
            query = obj_or_import_string(query)
            if callable(query):
                query = query(self.app)

            result[name] = _Query(
                name=name,
                cls=obj_or_import_string(query['cls']),
                params=query.get('params', {}),
                permission_factory=query.get('permission_factory'),
            )
        return result

    @cached_property
    def permission_factory(self):
        """Load default permission factory for Buckets collections."""
        return load_or_import_from_config(
            'STATS_PERMISSION_FACTORY', app=self.app
        )

    def publish(self, event_type, events):
        """Publish events."""
        assert event_type in self.events
        current_queues.queues['stats-{}'.format(event_type)].publish(events)

    def consume(self, event_type, no_ack=True, payload=True):
        """Comsume all pending events."""
        assert event_type in self.events
        return current_queues.queues['stats-{}'.format(event_type)].consume(
            payload=payload)


class InvenioStats(object):
    """Invenio-Stats extension."""

    def __init__(self, app=None, **kwargs):
        """Extension initialization."""
        if app:
            self.init_app(app, **kwargs)

    def init_app(self, app):
        """Flask application initialization."""
        self.init_config(app)

        state = _InvenioStatsState(app)
        self._state = app.extensions['invenio-stats'] = state
        if app.config['STATS_REGISTER_RECEIVERS']:
            signal_receivers = {key: value for key, value in
                                app.config.get('STATS_EVENTS', {}).items()
                                if 'signal' in value}
            register_receivers(app, signal_receivers)

        return state

    def init_config(self, app):
        """Initialize configuration."""
        for k in dir(config):
            if k.startswith('STATS_'):
                app.config.setdefault(k, getattr(config, k))

    def __getattr__(self, name):
        """Proxy to state object."""
        return getattr(self._state, name, None)
