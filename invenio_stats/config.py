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

"""Proxy to the current stats module."""

from __future__ import absolute_import, print_function

from datetime import timedelta

from kombu import Exchange

STATS_MQ_EXCHANGE = Exchange(
    'events',
    type='direct',
    delivery_mode='transient',  # in-memory queue
)
"""Default exchange for message queue."""

STATS_INDICES_PREFIX = 'events'
"""Allowed event types."""

STATS_REGISTER_RECEIVERS = True
"""Register signal receivers."""

STATS_INDICES_SUFFIX = '%Y.%W',
"""Suffix of indices."""

CELERY_BEAT_SCHEDULE = {
    'indexer': {
        'task': 'invenio_stats.tasks.index_events',
        'schedule': timedelta(seconds=5),
    },
}

STATS_EVENTS = [
    'record_view',
    'file_download'
]
