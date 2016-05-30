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

"""Test event queue."""

from __future__ import absolute_import, print_function

import pytest

from invenio_stats import EventQueue, current_stats


def test_producer(event_queue):
    """Test basic publishing/consuming."""
    event_queue.publish([{'test': 'data'}])
    # Consume and test data.
    msgs = list(event_queue.consume())
    assert len(msgs) == 1
    assert msgs[0] == {'test': 'data'}
    # Consume again and message is gone.
    assert len(list(event_queue.consume())) == 0


def test_routing(app, celery, exchange, event_queue):
    """Test basic routing of events."""
    q1 = EventQueue(exchange, 'test-event-1')
    q2 = EventQueue(exchange, 'test-event-2')

    # Initialize and declare queues
    with celery.pool.acquire(block=True) as conn:
        for q in [q1, q2]:
            q.queue(conn).declare()
            q.queue(conn).purge()

    q1.publish([{'event': '1'}])
    q2.publish([{'event': '2'}])

    assert list(q1.consume()) == [{'event': '1'}]
    assert list(q2.consume()) == [{'event': '2'}]


def test_ext_interface(app):
    """Test extension interface."""
    current_stats.register_eventtype('test-event-3', None)
    current_stats.register_eventtype('test-event-4', None)
    pytest.raises(
        RuntimeError, current_stats.register_eventtype, 'test-event-4', None)
    current_stats.declare()

    # Test basic publish/consume.
    current_stats.publish('test-event-3', [dict(data='val')])
    assert len(list(current_stats.consume('test-event-3'))) == 1

    # Test purge queue (with messages in queue)
    current_stats.publish('test-event-3', [dict(data='val')])
    current_stats.purge(event_types=['test-event-3'])
    assert len(list(current_stats.consume('test-event-3'))) == 0

    # Test purge queue (with messages in different queue).
    current_stats.publish('test-event-3', [dict(data='val')])
    current_stats.purge(event_types=['test-event-4'])
    assert len(list(current_stats.consume('test-event-3'))) == 1

    current_stats.delete()
    current_stats.publish('test-event-3', [dict(data='val')])
