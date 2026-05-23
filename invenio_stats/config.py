# SPDX-FileCopyrightText: 2016-2018 CERN.
# SPDX-FileCopyrightText: 2022 TU Wien.
# SPDX-License-Identifier: MIT

"""Proxy to the current stats module."""

from kombu import Exchange

from .utils import default_permission_factory

STATS_REGISTER_RECEIVERS = True
"""Enable the registration of signal receivers.

Default is ``True``.
The signal receivers are functions which will listen to the signals listed in
by the ``STATS_EVENTS`` config variable. An event will be generated for each
signal sent.
"""

STATS_EVENTS = {}
"""Enabled Events.

Each key is the name of an event. A queue will be created for each event.

If the dict of an event contains the ``signal`` key, and the config variable
``STATS_REGISTER_RECEIVERS`` is ``True``, a signal receiver will be registered.
Receiver function which will be connected on a signal and emit events. The key
is the name of the emitted event.

``signal``: Signal to which the receiver will be connected to.

``event_builders``: list of functions which will create and enhance the event.
    Each function will receive the event created by the previous function and
    can update it. Keep in mind that these functions will run synchronously
    during the creation of the event, meaning that if the signal is sent during
    a request they will increase the response time.

You can find a sampe of STATS_EVENT configuration in the `registrations.py`
"""


STATS_AGGREGATIONS = {}


STATS_QUERIES = {}


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
    "events",
    type="direct",
    delivery_mode="transient",  # in-memory queue
)
"""Default exchange used for the message queues."""

STATS_REGISTER_INDEX_TEMPLATES = False
"""Register templates as index templates.

Default behaviour will register the templates as search templates.
"""

STATS_EVENTS_UTC_DATETIME_ENABLED = False
"""Enable timezone-aware UTC datetimes for event timestamps.

When set to ``False`` (default), naive UTC datetimes are used (tzinfo is
stripped via ``datetime.replace(tzinfo=None)``). Set to ``True`` to use
timezone-aware UTC datetimes with explicit UTC timezone information.
"""
