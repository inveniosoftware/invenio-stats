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


Overview
--------
Invenio-Stats provides enables an Invenio instance to generate statistics and
access them via a REST API.


1. Event processing
~~~~~~~~~~~~~~~~~~~
Statistics can measure the occurence of event within the application (
e.g. file downloads, record views) by plugging multiple components like this:


.. graphviz::

    digraph G {
    rankdir=LR;

    Module [
        label="Other Module",
        width=1.5,
        height=3,
        fixedsize=true, // don't allow nodes to change sizes dynamically
        shape=rectangle
        color=grey,
        style=filled,
    ];
    Module -> Emitter [label="(1) signals"];

    subgraph cluster_invenio_stats {
        rank=same;
        fontsize = 20;
        label = "Invenio Stats";
        style = "solid";
        Emitter [label="Event\nEmitter", shape="parallelogram"];

        subgraph cluster_celery {
            label="Celery Tasks"
            style="dashed"
            fontsize = 15;
            Processor [label=<Events<BR/>Indexer<BR/><FONT POINT-SIZE="10">Event Processor</FONT>>, shape=Mcircle]
            Aggregator [label="Event\nAggregator", shape=Mcircle]
        }
    }
    Queue [label="Message Queue\n(RabbitMQ)", margin=0.2, shape="cds"];
    Elasticsearch [label="Elasticsearch", shape="cylinder", height=2];
    
    Emitter -> Queue [label="(2) events"];
    
    Queue -> Processor [label="(3) event"];
    
    Processor -> Elasticsearch [label="(4) processed events"];

    Aggregator -> Elasticsearch [label="(5) processed events" dir=back];
    Aggregator -> Elasticsearch [label="(6) aggregated statistics"];
    }



1.1. Registering the Event type
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you create one or more event types you need to register them in your module.

First create a function which will list all the events provided by your Invenio
module as well as their default configuration.

.. code-block:: python

 def register_events():
    return [dict(event_type='file-download',
                 templates='contrib/file-download',
                 processor_class=EventsIndexer,
                 processor_config=dict(
                    preprocessors=[
                        flag_robots,
                        anonymize_user,
                        build_file_unique_id
                    ]
                 )),
            ...

The templates attribute defines the Elasticsearch templates that will be used
for indexing, and the processor related attributes will be explained later.

Once you created the function you need to declare it in the setup.py under
the entrypoint `invenio_stats.events`.

.. code-block:: python
 
 'invenio_stats.events': [
     'invenio_stats = '
     'invenio_stats.contrib.registrations:register_events'
 ]

The function enables to dynamically add new event using for example
information from the database.

Only enabled events will be processed. Events are enabled in the configuration
like this:

.. code-block:: python

    # in config.py
    STATS_EVENTS = {
        'file-download': {
            'event_builders': [
                'invenio_stats.contrib.event_builders.file_download_event_builder'
            ]
        },
    }

An Invenio overlay should list all the events it wants to process.

It is also possible to override the default configuration of an event
using `processor_config` the `STATS_EVENTS` config variable:

.. code-block:: python

    STATS_EVENTS = {
        'file-download': dict(
            signal='invenio_files_rest.signals.file_downloaded',
            event_builders=[
                'invenio_stats.contrib.event_builders.file_download_event_builder'
            ],
            processor_config=dict(
                preprocessors=[
                    'invenio_stats.processors:flag_robots',
                    'invenio_stats.processors:anonymize_user',
                    'invenio_stats.contrib.event_builders:build_file_unique_id',
                ],
                # Keep only 1 file download for each file and user every 30 sec
                double_click_window=30,
                # Create one index per month which will store file download events
                suffix='%Y-%m'
            ))
    }

1.2. Emitting Signals (optional)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The above diagram shows a **signal** being monitored. This signal occurences
are registered as **Events**. Invenio modules already send many different
signals. If no signal exist it is possible to emit your own signals.

.. code-block:: python
 
    # in signals.py 
    from blinker import Namespace

    _signals = Namespace()
    file_downloaded = _signals.signal('file-downloaded')
    
    # at the point where the event happens
    file_downloaded.send(current_app._get_current_object(), obj=obj)

Note that this is optional. It is also possible to directly emit your own
events without using signals.

It is possible to completely disable the signal handling by setting
`STATS_REGISTER_RECEIVERS = False` in the configuration.

1.3. Emitting Events
^^^^^^^^^^^^^^^^^^^

Events can be generated either from signals or directly in any module.

In order to automatically generate events when a signal is sent add `signal`
to the `STATS_EVENTS` configuration. The following configuration shows how to
generate a `file-download` event when the `file_downloaded` signal is emitted:

.. code-block:: python

    # in config.py
    STATS_EVENTS = {
        'file-download': {
            'signal': 'invenio_files_rest.signals.file_downloaded',
            'event_builders': [
                'invenio_stats.contrib.event_builders.file_download_event_builder'
            ]
        },
    }

If you prefer to generate your event directly, here is an example:

.. code-block:: python

    from .proxies import current_stats

    event = dict(
        timestamp=datetime.datetime.utcnow().isoformat(),
        mydata='somedata'
    )

    current_stats.publish('my-event-name', [event])


1.4. Event Processing
^^^^^^^^^^^^^^^^^^^
Now that an event is recorded, the next step is adding it to our Elasticsearch
storage so that the new statistics can be calculated. A default event indexer
for this task is provided in processors.py. An indexer is assigned to each
event as seen in section 1, and also a list preprocessors is given. These are
functions, similar to the event builders, which will be called before the
indexing to Elasticsearch.

It is possible to pass as a parameter a time window in seconds (10s by
default) within which, multiple events from the same user to the resource will
count as 1, allowing for more accurate statistics.

After the processing has taken place the event is indexed in Elasticsearch,
according to the template provided in the event registration. The index is
under the alias **events-stats-file-download**. It is possible to index events
per different intervals (day, month or other).

1.5. Aggregating
^^^^^^^^^^^^^^
Aggregating refers to calculating the actual statistics.
The procedure so far stored in Elasticsearch the "raw" event. In order to
provide specific statistics, aggregations are needed.
Aggregations are registered in the same way as events, under the entrypoint
'invenio_stats.aggregations'.

.. code-block:: python

 'invenio_stats.aggregations': [
      'invenio_stats = '
      'invenio_stats.contrib.registrations:register_aggregations'
 ]

The function returns a dictionary with the configuration for the aggregation.

.. code-block:: python

 def register_aggregations():
    """Register sample aggregations."""
    return [dict(aggregation_name='file-download-agg',
                 templates='contrib/aggregations/aggr-file-download',
                 aggregator_class=StatAggregator,
                 aggregator_config=dict(
                     client=current_search_client,
                     event='file-download',
                     aggregation_field='unique_id',
                     aggregation_interval='day',
                     copy_fields=dict(
                         file_key='file_key',
                         bucket_id='bucket_id',
                         file_id='file_id',
                     )
                 ))
            ]

An aggregator class must be specified, the default one is StatAggregator in aggregations.py. The required parameters designate the **event** for which we want to calculate the statistics, based on what **field** we should identify the events, what should be the **interval** and what fields should be copied in the results for better readability. The results are then stored in Elasticsearch under the alias **stats-file-download**.


.. graphviz::


    digraph G {
    rankdir=LR;
    WEB [
        label="WEB",
        shape=rectangle,
        color=grey,
        style=filled,
        width=1.5,
        height=3,
    ]
    WEB -> REST [label="(1) HTTP request"];
    REST -> WEB [label="(6) HTTP response"];


    subgraph cluster_invenio_stats {
        fontsize = 20;
        label = "Invenio Stats";
        style = "solid";
        REST [
            label="Statistics\nREST API\n/api/stats/",
            shape=rectangle,
            width=1.5,
            height=3,
        ]
        Query [label="Aggregation\nQuery", shape="Msquare"]
        REST -> Query [label="(2) query"];
        Query -> REST [label="(5) statistics"];
    }
    Elasticsearch [label="Elasticsearch", shape="cylinder", height=2];
    Query -> Elasticsearch [label="(3) query"];
    Elasticsearch -> Query [label="(4) stats"];
    }


2. Querying
~~~~~~~~~~~
The statistics are accessible via REST API.
The queries are predefined and they are registered in the same way as events and aggregations, under the entrypoint 'invenio_stats.queries'.

.. code-block:: python
 
 'invenio_stats.queries': [
     'invenio_stats = '           
     'invenio_stats.contrib.registrations:register_queries'
 ]

Again the registering function returns the configuraton for the query.

.. code-block:: python

 def register_queries():
    """Register queries."""
    return [
        dict(
            query_name='bucket-file-download-histogram',
            query_class=ESDateHistogramQuery,
            query_config=dict(
                index='stats-file-download',
                doc_type='file-download-day-aggregation',
                copy_fields=dict(
                    bucket_id='bucket_id',
                    file_key='file_key',
                ),
                required_filters=dict(
                    bucket_id='bucket_id',
                    file_key='file_key',
                )
            )
        )
   ...

The logic is identical, we specify the query class, each for a given statistic. The document type specifies the Elasticsearch doc_type from where we will get the results, the copy fields function in the same as in queries, and the required filters are the parameters which must be given in order to identify the resource.

An example request fetching statistics is the following:

.. code-block:: bash
 
 $ curl -XPOST localhost:5000/stats -H "Content-Type: application/json" -d '{"mystat": {"stat": "bucket-file-download-histogram", "params": {"start_date":"2016-12-18", "end_date":"2016-12-19", "interval": "day", "bucket_id": 20, "file_key": "file1.txt"}}}'

Not every statistic of interest has to be derived from elasticsearch. For example a ratio of open access vs closed access records has to be calculated using information from the database. # explain
