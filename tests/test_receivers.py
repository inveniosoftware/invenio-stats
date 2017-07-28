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

"""Signal receivers tests."""

from blinker import Namespace
from invenio_queues.proxies import current_queues

from invenio_stats import InvenioStats
from invenio_stats.proxies import current_stats


def test_register_receivers(base_app, event_entrypoints):
    """Test signal-receiving/event-emitting functions registration."""
    try:
        _signals = Namespace()
        my_signal = _signals.signal('my-signal')

        def event_builder1(event, sender_app, signal_param, *args, **kwargs):
            event.update(dict(event_param1=signal_param))
            return event

        def event_builder2(event, sender_app, signal_param, *args, **kwargs):
            event.update(dict(event_param2=event['event_param1'] + 1))
            return event

        base_app.config.update(dict(
            STATS_EVENTS=dict(
                event_0=dict(
                    signal=my_signal,
                    event_builders=[event_builder1, event_builder2]
                )
            )
        ))
        InvenioStats(base_app)
        current_queues.declare()
        my_signal.send(base_app, signal_param=42)
        my_signal.send(base_app, signal_param=42)
        events = [event for event in current_stats.consume('event_0')]
        # two events should have been created from the sent events. They should
        # have been both processed by the two event builders.
        assert events == [{'event_param1': 42, 'event_param2': 43}] * 2
    finally:
        current_queues.delete()
