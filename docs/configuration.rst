..
    This file is part of Invenio.
    Copyright (C) 2017 CERN.

    Invenio is free software; you can redistribute it
    and/or modify it under the terms of the GNU General Public License as
    published by the Free Software Foundation; either version 2 of the
    License, or (at your option) any later version.

    Invenio is distributed in the hope that it will be
    useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
    General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with Invenio; if not, write to the
    Free Software Foundation, Inc., 59 Temple Place, Suite 330, Boston,
    MA 02111-1307, USA.

    In applying this license, CERN does not
    waive the privileges and immunities granted to it by virtue of its status
    as an Intergovernmental Organization or submit itself to any jurisdiction.


Configuration
=============


Events creation
---------------
The creation of events can be done via signals. Invenio-stats provides an
easy way to generate those events.


.. autodata:: invenio_stats.config.STATS_REGISTER_RECEIVERS

.. autodata:: invenio_stats.config.STATS_EVENTS


Events processing
-----------------
If you create events they will be queued in an AMQP queue. You should
ensure that you regularly process them. You do this by configuring a Celery
Beat schedule similar to this:

.. code-block:: python

    from datetime import timedelta
    CELERY_BEAT_SCHEDULE = {
        'indexer': {
            'task': 'invenio_stats.tasks.process_events',
            'schedule': timedelta(hours=3),
        },
    }

This example uses the Celery beat process to trigger an event processing
task every 3 hours.

Invenio-stats provides two tasks:

* `invenio_stats.tasks.process_events`

* `invenio_stats.tasks.aggregate_events`


Queues configuration
--------------------
Invenio-stats creates AMQP queues in order to buffer events. Those queues
need to be configured. Change these parameters only if you know what you are
doing.

`invenio_stats.config.STATS_MQ_EXCHANGE`: Default exchange used for the
message queues.

