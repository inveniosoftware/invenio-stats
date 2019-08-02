# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2016-2018 CERN.
#
# Invenio is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Invenio module for collecting statistics."""

from __future__ import absolute_import, print_function

from collections import namedtuple

from invenio_queues.proxies import current_queues
from pkg_resources import iter_entry_points
from werkzeug.utils import cached_property

from . import config
from .errors import DuplicateAggregationError, DuplicateEventError, \
    DuplicateQueryError, UnknownAggregationError, UnknownEventError, \
    UnknownQueryError
from .receivers import register_receivers
from .utils import load_or_import_from_config


class _InvenioStatsState(object):
    """State object for Invenio stats."""

    def __init__(self, app):
        self.app = app
        self.exchange = app.config['STATS_MQ_EXCHANGE']
        self.stats_aggregations = app.config['STATS_AGGREGATIONS']
        self.stats_events = app.config['STATS_EVENTS']
        self.stats_queries = app.config['STATS_QUERIES']

    @cached_property
    def events(self):
        EventConfig = namedtuple('EventConfig',
                                 ['queue', 'config', 'templates',
                                  'processor_class', 'processor_config'])
        # import iter_entry_points here so that it can be mocked in tests
        result = {}

        for event_config in self.stats_events.values():
            queue = current_queues.queues[
                'stats-{}'.format(event_config['event_type'])]
            result[event_config['event_type']] = EventConfig(
                queue=queue,
                config=event_config,
                templates=event_config['templates'],
                processor_class=event_config['processor_class'],
                processor_config=dict(
                    queue=queue, **event_config.get('processor_config', {})
                )
            )
        return result

    @cached_property
    def aggregations(self):
        AggregationConfig = namedtuple(
            'AggregationConfig',
            ['name', 'config', 'templates', 'aggregator_class',
             'aggregator_config']
        )
        result = {}

        for agg_config in self.stats_aggregations.values():
            result[agg_config['aggregation_name']] = AggregationConfig(
                name=agg_config['aggregation_name'],
                config=agg_config,
                templates=agg_config['templates'],
                aggregator_class=agg_config['aggregator_class'],
                aggregator_config=agg_config.get('aggregator_config', {})
            )
        return result

    @cached_property
    def queries(self):
        QueryConfig = namedtuple(
            'QueryConfig',
            ['query_class', 'query_config', 'permission_factory', 'config']
        )
        result = {}

        for query_config in self.stats_queries.values():
            result[query_config['query_name']] = QueryConfig(
                config=query_config,
                query_class=query_config['query_class'],
                query_config=dict(
                    query_name=query_config['query_name'],
                    **query_config.get('query_config', {})
                ),
                permission_factory=query_config.get('permission_factory')
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
