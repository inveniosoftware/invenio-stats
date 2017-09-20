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

===============
 Invenio-Stats
===============

This module provides the functionality to measure the occurence of any event in an invenio application, e.g. file downloads, record views and others. During the logging of an event, it is possible to define processes that will prepare or append to the data recorded, e.g. add the country of origin from the ip. Monitoring works by listening to the signals triggered by the declared events. The event data are The different components used for the stream processing, are the following, in order of appearance:

 **Event Signal** > **Receiver** > **Message Queue** > **Task Queue Worker** > **Elasticsearch**

The default Message Queue is RabbitMQ and Task Queue is Celery.
These signals also carry any other information required for cataloguing, such as timestamp, record info etc.
Each step to register and start monitoring an event is described below.

1. Event Registration
---------------------
An event is registered by the function declared in the setup.py under the entrypoint 'invenio_stats.events'

.. code-block:: python
 
 'invenio_stats.events': [
     'invenio_stats = '
     'invenio_stats.contrib.registrations:register_events'
 ]

The registration function returns a list of dictionaries with the event configurations.

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

The templates attribute defines the Elasticsearch templates that will be used for indexing, and the processor related attributes are explained in section 3.

2. Event Emission
-----------------
To start monitoring an event we define a signal, and trigger at the desired point:

.. code-block:: python
 
 # in signals.py 
 from blinker import Namespace

 _signals = Namespace()
 file_downloaded = _signals.signal('file-downloaded')
 
 # at the point where the event happens
 file_downloaded.send(current_app._get_current_object(), obj=obj)

The signal will be received, processed and published to the Message Queue by the function specified in the configuration. For each event we can specify a set of builder functions to add data that may be needed. E.g. the following receiver will add the user information:

.. code-block:: python
 
 def file_download_event_builder(event, sender_app, obj=None, **kwargs):
    """Build a file-download event."""
    event.update(dict(
        # When:
        timestamp=datetime.datetime.utcnow().isoformat(),
        # What:
        bucket_id=str(obj.bucket_id),
        file_id=str(obj.file_id),
        file_key=obj.key,
        # Who:
        **get_user()
    ))
    return event

The configuration for this event can define what builders will be used.

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

Publishing to the Message Queue is handled automatically.

3. Event Processing
-------------------
Now that an event is recorded, the next step is adding it to our Elasticsearch storage so that the new statistics can be calculated. A default event indexer for this task is provided in processors.py. An indexer is assigned to each event as seen in section 1, and also a list preprocessors is given. These are functions, similar to the event builders, which will be called before the indexing to Elasticsearch.

It is possible to pass as a parameter a time window in seconds (10s by default) within which, multiple events from the same user to the resource will count as 1, allowing for more accurate statistics.

After the processing has taken place the event is indexed in Elasticsearch, according to the template provided in the event registration. The index is under the alias **events-stats-file-download**. It is possible to index events per different intervals (day, month or other).

4. Aggregating
--------------
Aggregating refers to calculating the actual statistics.
The procedure so far stored in Elasticsearch the "raw" event. In order to provide specific statistics, aggregations are needed.
Aggregations are registered in the same way as events, under the entrypoint 'invenio_stats.aggregations'.

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

5. Querying
-----------
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

Architecture
------------
The architecture of the statistics module decouples the events, from the aggregations and the queries allowing the registrations of queries seperately, from any module. All three components are loaded in a lazy way and stored a dictionary, therefore if needed they can be overwritten by newer versions from different modules. 
 # Add part with queries not tied to events but take stats from the DB
 # Mention features that are available by using this pattern of seperated events, aggregations and queries