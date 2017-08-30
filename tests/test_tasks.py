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

"""Test celery tasks."""

from __future__ import absolute_import, print_function

from datetime import datetime

from invenio_stats import current_stats
from invenio_stats.tasks import aggregate_events, process_events


def test_process_events(app, es, event_queues):
    """Test process event."""
    current_stats.publish('file-download',
                          [dict(timestamp='2017-01-01T00:00:00',
                                visitor_id='testuser1',
                                unique_id='2017-01-01T00:00:00-hash',
                                data='val')])
    process_events.delay(['file-download'])
    # FIXME: no need to publish events. We should just mock "consume" and test
    # that the events are properly received and processed.
