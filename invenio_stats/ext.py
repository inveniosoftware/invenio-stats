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

from celery import current_app as current_celery_app
from invenio_files_rest.signals import file_downloaded
from invenio_records_ui.signals import record_viewed
from pkg_resources import iter_entry_points

from . import config
from .cli import stats as stats_cli
from .indexer import EventsIndexer
from .manager import IndexTemplate
from .queue import EventQueue
from .receivers import filedownload_receiver, recordview_receiver


class _InvenioStatsState(object):
    """State object for Invenio stats."""

    def __init__(self, app, entry_point_group):
        self.app = app
        self.exchange = app.config['STATS_MQ_EXCHANGE']
        self.suffix = app.config['STATS_INDICES_SUFFIX']
        self.event_types = dict()

        if entry_point_group:
            self.load_entry_point_group(entry_point_group)

    def event_queue(self, event_type, **kwargs):
        return EventQueue(
            self.exchange,
            event_type,
            **kwargs
        )

    def indexer(self, event_type):
        # TODO: Allow customization of indexer and suffix
        return EventsIndexer(
            self.event_queue(event_type),
            prefix=self.app.config['STATS_INDICIES_PREFIX'],
            suffix=self.suffix
        )

    def _action(self, action, event_types=None):
        with current_celery_app.pool.acquire(block=True) as conn:
            for e in (event_types or self.event_types):
                getattr(self.event_queue(e).queue(conn), action)()

    def declare(self, **kwargs):
        """Declare queue for all or specific event types."""
        self._action('declare', **kwargs)

    def purge(self, **kwargs):
        """Purge queue for all or specific event types."""
        self._action('purge', **kwargs)

    def delete(self, **kwargs):
        """Delete queue for all or specific event types."""
        self._action('delete', **kwargs)

    def publish(self, event_type, events):
        """Publish events."""
        assert event_type in self.event_types
        self.event_queue(event_type).publish(events)

    def consume(self, event_type, no_ack=True, payload=True):
        """Comsume all pending events."""
        assert event_type in self.event_types
        return self.event_queue(event_type, no_ack=no_ack).consume(
            payload=payload)

    def register_eventtype(self, event_type, package_name):
        """Register an event type."""
        if event_type in self.event_types:
            raise RuntimeError('Event type already registered.')

        name = '{0}-{1}'.format(
            self.app.config['STATS_INDICIES_PREFIX'],
            event_type
        )

        self.event_types[event_type] = IndexTemplate(
            event_type, name, package_name)

    def load_entry_point_group(self, entry_point_group):
        """Load actions from an entry point group."""
        for ep in iter_entry_points(group=entry_point_group):
            self.register_eventtype(ep.name, ep.module_name)


class InvenioStats(object):
    """Invenio-Stats extension."""

    def __init__(self, app=None, **kwargs):
        """Extension initialization."""
        if app:
            self.init_app(app, **kwargs)

    def init_app(self, app, entry_point_group='invenio_stats.estemplates'):
        """Flask application initialization."""
        self.init_config(app)

        if app.config['STATS_REGISTER_RECEIVERS']:
            file_downloaded.connect(filedownload_receiver, sender=app)
            record_viewed.connect(recordview_receiver, sender=app)

        if hasattr(app, 'cli'):
            app.cli.add_command(stats_cli)

        state = _InvenioStatsState(app, entry_point_group=entry_point_group)
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
