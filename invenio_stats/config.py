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

from .utils import default_permission_factory

STATS_REGISTER_RECEIVERS = True
"""Enable the registration of signal receivers.

Default is ``True``.
The signal receivers are functions which will listen to the signals listed in
by the ``STATS_EVENTS`` config variable. An event will be generated for each
signal sent.
"""

STATS_EVENTS = {
    'file-download': {
        'signal': 'invenio_files_rest.signals.file_downloaded',
        'event_builders': [
            'invenio_stats.contrib.event_builders.file_download_event_builder'
        ]
    },
}
"""Enabled Events.

Each key is the name of an event. A queue will be created for each event.

If the dict of an event contains the 'signal' key, and the config variable
``STATS_REGISTER_RECEIVERS`` is ``True``, a signal receiver will be registered.
Receiver function which will be connected on a signal and emit events.
The key is the name of the emitted event.

signal:
    Signal to which the receiver will be connected to.

event_builders:
    list of functions which will create and enhance the event. Each function
    will receive the event created by the previous function and can update it.
"""


STATS_AGGREGATIONS = {
    'file-download-agg': {},
}


STATS_QUERIES = {
    'bucket-file-download-histogram': {},
    'bucket-file-download-total': {},
}


STATS_PERMISSION_FACTORY = default_permission_factory
"""Permission factory used by the statistics REST API.

This is a function which returns a permission granting or forbidding access
to a request. It is of the form ``permission_factory(query_name, params)``
where ``query_name`` is the name of the statistic requested by the user and
``params`` is a dict of parameters for this statistic. The result of the
function is a Permission.

See Invenio-access and Flask-principal for a better understanding of the
access control mechanisms.
"""


STATS_MQ_EXCHANGE = Exchange(
    'events',
    type='direct',
    delivery_mode='transient',  # in-memory queue
)
"""Default exchange used for the message queues."""
