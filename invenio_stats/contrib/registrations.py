# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2017-2018 CERN.
#
# Invenio is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Registration of contrib events."""
from invenio_search import current_search_client

from invenio_stats.aggregations import StatAggregator
from invenio_stats.contrib.event_builders import build_file_unique_id, \
    build_record_unique_id
from invenio_stats.processors import EventsIndexer, anonymize_user, flag_robots
from invenio_stats.queries import ESDateHistogramQuery, ESTermsQuery


def register_events():
    """Register sample events."""
    return [
        dict(
            event_type='file-download',
            templates='invenio_stats.contrib.file_download',
            processor_class=EventsIndexer,
            processor_config=dict(
                preprocessors=[
                    flag_robots,
                    anonymize_user,
                    build_file_unique_id
                ])),
        dict(
            event_type='record-view',
            templates='invenio_stats.contrib.record_view',
            processor_class=EventsIndexer,
            processor_config=dict(
                preprocessors=[
                    flag_robots,
                    anonymize_user,
                    build_record_unique_id
                ]))
    ]


def register_aggregations():
    """Register sample aggregations."""
    return [dict(
        aggregation_name='file-download-agg',
        templates='invenio_stats.contrib.aggregations.aggr_file_download',
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
            ),
            metric_aggregation_fields={
                'unique_count': ('cardinality', 'unique_session_id',
                                 {'precision_threshold': 1000}),
                'volume': ('sum', 'size', {}),
            },
        )), dict(
        aggregation_name='record-view-agg',
        templates='invenio_stats.contrib.aggregations.aggr_record_view',
        aggregator_class=StatAggregator,
        aggregator_config=dict(
            client=current_search_client,
            event='record-view',
            aggregation_field='unique_id',
            aggregation_interval='day',
            copy_fields=dict(
                record_id='record_id',
                pid_type='pid_type',
                pid_value='pid_value',
            ),
            metric_aggregation_fields={
                'unique_count': ('cardinality', 'unique_session_id',
                                 {'precision_threshold': 1000}),
            },
        ))]


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
        ),
        dict(
            query_name='bucket-file-download-total',
            query_class=ESTermsQuery,
            query_config=dict(
                index='stats-file-download',
                doc_type='file-download-day-aggregation',
                copy_fields=dict(
                    # bucket_id='bucket_id',
                ),
                required_filters=dict(
                    bucket_id='bucket_id',
                ),
                aggregated_fields=['file_key']
            )
        ),
    ]
