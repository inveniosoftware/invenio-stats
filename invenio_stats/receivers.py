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

"""Function registering signal-receiving/event-emitting functions."""

from __future__ import absolute_import, print_function

from .proxies import current_stats
from .utils import obj_or_import_string


def register_receivers(app, config):
    """Register signal receivers which send events."""
    for event_name, event_config in config.items():
        event_builders = [
            obj_or_import_string(func)
            for func in event_config.get('event_builders', [])
        ]

        def event_emitter(*args, **kwargs):
            """Receive a signal and send an event."""
            # Send the event only if it is registered
            if event_name in current_stats.events:
                event = {}
                for builder in event_builders:
                    event = builder(event, *args, **kwargs)
                if event:
                    current_stats.publish(event_name, [event])
        # Connect the receiver to the signal
        obj_or_import_string(
            event_config['signal']
        ).connect(event_emitter, weak=False)
