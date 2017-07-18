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

"""Invenio module for collecting statistics."""

from __future__ import absolute_import, print_function

from collections import namedtuple

from invenio_queues.proxies import current_queues
from pkg_resources import iter_entry_points, resource_listdir
from werkzeug.utils import cached_property

from . import config
from .errors import DuplicateEventError, UnknownEventError
from .indexer import EventsIndexer
from .receivers import filedownload_receiver, recordview_receiver


class _InvenioStatsState(object):
    """State object for Invenio stats."""

    def __init__(self, app, events_entry_point_group):
        self.app = app
        self.exchange = app.config['STATS_MQ_EXCHANGE']
        self.suffix = app.config['STATS_INDICES_SUFFIX']
        self.enabled_events = app.config['STATS_EVENTS']
        self.events_entry_point_group = events_entry_point_group

    @cached_property
    def _events_config(self):
        """Load events configuration."""
        # import iter_entry_points here so that it can be mocked in tests
        result = {}
        for ep in iter_entry_points(group=self.events_entry_point_group):
            for cfg in ep.load()():
                if cfg['event_type'] not in self.enabled_events:
                    continue
                elif cfg['event_type'] in result:
                    raise DuplicateEventError(
                        'Duplicate event {0} in entry point '
                        '{1}'.format(cfg['event_type'], ep.name))
                result[cfg['event_type']] = cfg
        return result

    @cached_property
    def events(self):
        EventConfig = namedtuple('EventConfig',
                                 ['queue', 'config', 'processor'])
        # import iter_entry_points here so that it can be mocked in tests
        result = {}
        config = self._events_config

        for event in self.enabled_events:
            if event not in config.keys():
                raise UnknownEventError(
                    'Unknown event {0} '.format(event))

        for cfg in config.values():
            queue = current_queues.queues[
                'stats-{}'.format(cfg['event_type'])]
            result[cfg['event_type']] = EventConfig(
                queue=queue,
                config=cfg,
                processor=cfg['processor'](queue)
            )
        return result

    # def __init__(self, app, entry_point_group):
    #     self.app = app
    #     self.event_types = []
    #     # self.queues = dict(app.extensions['invenio-queues'].queues

    #     if entry_point_group:
    #         self.load_entry_point_group(entry_point_group)

    def indexer(self, event_type):
        # TODO: Allow customization of indexer and suffix
        return EventsIndexer(
            current_queues.queues['stats-{}'.format(event_type)],
            prefix=self.app.config['STATS_INDICES_PREFIX'],
            suffix=self.suffix
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

    # def register_eventtype(self, event_type, package_name):
    #     """Register an event type."""
    #     if event_type in self.event_types:
    #         raise RuntimeError('Event type already registered.')
    #     self.event_types.append(event_type)

    # def load_entry_point_group(self, entry_point_group):
    #     """Load actions from an entry point group."""
    #     for ep in iter_entry_points(group=entry_point_group):
    #         self.register_eventtype(ep.name, ep.module_name)


class InvenioStats(object):
    """Invenio-Stats extension."""

    def __init__(self, app=None, **kwargs):
        """Extension initialization."""
        if app:
            self.init_app(app, **kwargs)

    def init_app(self, app, events_entry_point_group='invenio_stats.events'):
        """Flask application initialization."""
        self.init_config(app)

        if app.config['STATS_REGISTER_RECEIVERS']:
            from invenio_files_rest.signals import file_downloaded
            from invenio_records_ui.signals import record_viewed
            file_downloaded.connect(filedownload_receiver, sender=app)
            record_viewed.connect(recordview_receiver, sender=app)

        state = _InvenioStatsState(
            app,
            events_entry_point_group=events_entry_point_group
        )
        self._state = app.extensions['invenio-stats'] = state

        return state

    def init_config(self, app):
        """Initialize configuration."""
        for k in dir(config):
            if k.startswith('STATS_'):
                app.config.setdefault(k, getattr(config, k))

    def __getattr__(self, name):
        """Proxy to state object."""
        return getattr(self._state, name, None)
