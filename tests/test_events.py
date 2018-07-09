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

"""Events tests."""
from invenio_queues.proxies import current_queues
from invenio_stats.proxies import current_stats


def test_event_queues_declare(app, event_entrypoints):
    """Test that event queues are declared properly."""
    try:
        for event in current_stats.events.values():
            assert not event.queue.exists
        current_queues.declare()
        for event in current_stats.events.values():
            assert event.queue.exists
    finally:
        current_queues.delete()


def test_publish_and_consume_events(app, event_entrypoints):
    """Test that events are published and consumed properly."""
    try:
        event_type = 'file-download'
        events = [{"payload": "test {}".format(idx)} for idx in range(3)]
        current_queues.declare()
        current_stats.publish(event_type, events)
        assert list(current_stats.consume(event_type)) == events
    finally:
        current_queues.delete()
