# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2017-2019 CERN.
#
# Invenio is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Aggregation classes."""

from __future__ import absolute_import, print_function

import math
from collections import OrderedDict
from copy import deepcopy
from datetime import datetime
from functools import wraps

import six
from dateutil import parser
from dateutil.relativedelta import relativedelta
from elasticsearch import VERSION as ES_VERSION
from elasticsearch.helpers import bulk
from elasticsearch_dsl import Index, Search
from invenio_search import current_search, current_search_client
from invenio_search.utils import prefix_index

from .utils import get_bucket_size, get_doctype

SUPPORTED_INTERVALS = OrderedDict([
    ('hour', '%Y-%m-%dT%H'),
    ('day', '%Y-%m-%d'),
    ('month', '%Y-%m'),
])

INTERVAL_ROUNDING = {
    'hour': ('minute', 'second', 'microsecond'),
    'day': ('hour', 'minute', 'second', 'microsecond'),
    'month': ('day', 'hour', 'minute', 'second', 'microsecond'),
}

INTERVAL_DELTAS = {
    'hour': relativedelta(hours=1),
    'day': relativedelta(days=1),
    'month': relativedelta(months=1),
}


def filter_robots(query):
    """Modify an elasticsearch query so that robot events are filtered out."""
    return query.filter('term', is_robot=False)


def format_range_dt(dt, interval):
    """Format range filter datetime to the closest aggregation interval."""
    dt_rounding_map = {
        'hour': 'h', 'day': 'd', 'month': 'M', 'year': 'y'}

    if not isinstance(dt, six.string_types):
        dt = dt.replace(microsecond=0).isoformat()
    return '{0}||/{1}'.format(dt, dt_rounding_map[interval])


class BookmarkAPI(object):
    """Bookmark API class.

    It provides an interface that lets us interact with a bookmark.
    """

    # NOTE: these work up to ES_6
    MAPPINGS = {
        "mappings": {
            "aggregation-bookmark": {
                "date_detection": False,
                "properties": {
                    "date": {
                        "type": "date",
                        "format": "date_optional_time"
                    },
                    "aggregation_type": {
                        "type": "keyword"
                    }
                }
            }
        }
    }

    # NOTE: For ES7 mappings need one-level of less nesting
    MAPPINGS_ES7 = {
        "mappings": deepcopy(MAPPINGS['mappings']['aggregation-bookmark'])
    }

    def __init__(self, client, agg_type, agg_interval):
        """Construct bookmark instance.

        :param client: elasticsearch client
        :param agg_type: aggregation type for the bookmark
        """
        # NOTE: doc_type is going to be deprecated with ES_7
        self.doc_type = get_doctype('aggregation-bookmark')
        self.bookmark_index = prefix_index('stats-bookmarks')
        self.client = client
        self.agg_type = agg_type
        self.agg_interval = agg_interval

    def _ensure_index_exists(func):
        """Decorator for ensuring the bookmarks index exists."""
        @wraps(func)
        def wrapped(self, *args, **kwargs):
            if not Index(self.bookmark_index, using=self.client).exists():
                self.client.indices.create(
                    index=self.bookmark_index, body=BookmarkAPI.MAPPINGS
                    if ES_VERSION[0] < 7 else BookmarkAPI.MAPPINGS_ES7)
            return func(self, *args, **kwargs)
        return wrapped

    @_ensure_index_exists
    def set_bookmark(self, value):
        """Set bookmark for starting next aggregation."""
        self.client.index(
            index=self.bookmark_index,
            doc_type=self.doc_type,
            body={'date': value, 'aggregation_type': self.agg_type},
        )

    @_ensure_index_exists
    def get_bookmark(self):
        """Get last aggregation date."""
        # retrieve the oldest bookmark
        query_bookmark = (
            Search(using=self.client, index=self.bookmark_index)
            .filter('term', aggregation_type=self.agg_type)
            .sort({'date': {'order': 'desc'}})[0:1]  # fetch one document only
        )
        bookmark = next(iter(query_bookmark.execute()), None)
        if bookmark:
            return datetime.strptime(
                bookmark.date, SUPPORTED_INTERVALS[self.agg_interval])

    @_ensure_index_exists
    def list_bookmarks(self, start_date=None, end_date=None, limit=None):
        """List bookmarks."""
        query = Search(
            using=self.client,
            index=self.bookmark_index,
        ).filter(
            'term', aggregation_type=self.agg_type
        ).sort({'date': {'order': 'desc'}})

        range_args = {}
        if start_date:
            range_args['gte'] = format_range_dt(start_date, self.agg_interval)
        if end_date:
            range_args['lte'] = format_range_dt(end_date)
        if range_args:
            query = query.filter('range', date=range_args)

        return query[0:limit].execute() if limit else query.scan()


ALLOWED_METRICS = {
    'avg',
    'cardinality',
    'extended_stats',
    'geo_centroid',
    'max',
    'min',
    'percentiles',
    'stats',
    'sum',
}


class StatAggregator(object):
    """Generic aggregation class.

    This aggregation class queries Elasticsearch events and creates a new
    elasticsearch document for each aggregated day/month/year... This enables
    to "compress" the events and keep only relevant information.

    The expected events shoud have at least those two fields:

    .. code-block:: json

        {
            "timestamp": "<ISO DATE TIME>",
            "field_on_which_we_aggregate": "<A VALUE>"
        }

    The resulting aggregation documents will be of the form:

    .. code-block:: json

        {
            "timestamp": "<ISO DATE TIME>",
            "field_on_which_we_aggregate": "<A VALUE>",
            "count": "<NUMBER OF OCCURENCE OF THIS EVENT>",
            "field_metric": "<METRIC CALCULATION ON A FIELD>"
        }

    This aggregator saves a bookmark document after each run. This bookmark
    is used to aggregate new events without having to redo the old ones.
    """

    def __init__(self, name, event, client=None,
                 field=None, metric_fields=None,
                 copy_fields=None, query_modifiers=None,
                 interval='day', index_interval='month',
                 max_bucket_size=10000):
        """Construct aggregator instance.

        :param event: aggregated event.
        :param client: elasticsearch client.
        :param field: field on which the aggregation will be done.
        :param metric_fields: dictionary of fields on which a
            metric aggregation will be computed. The format of the dictionary
            is "destination field" ->
            tuple("metric type", "source field", "metric_options").
        :param copy_fields: list of fields which are copied from the raw events
            into the aggregation.
        :param query_modifiers: list of functions modifying the raw events
            query. By default the query_modifiers are [filter_robots].
        :param interval: aggregation time window. default: month.
        :param index_interval: time window of the elasticsearch indices which
            will contain the resulting aggregations.
        """
        self.name = name
        self.event = event
        self.event_index = prefix_index('events-stats-{}'.format(event))
        self.client = client or current_search_client
        self.index = prefix_index('stats-{}'.format(event))
        self.field = field
        self.metric_fields = metric_fields or {}
        self.interval = interval
        self.doc_id_suffix = SUPPORTED_INTERVALS[interval]
        self.index_interval = index_interval
        self.index_name_suffix = SUPPORTED_INTERVALS[index_interval]
        self.copy_fields = copy_fields or {}
        self.query_modifiers = (query_modifiers if query_modifiers is not None
                                else [filter_robots])
        self.bookmark_api = BookmarkAPI(self.client, self.name, self.interval)
        self.max_bucket_size = max_bucket_size

        if any(v not in ALLOWED_METRICS
               for k, (v, _, _) in self.metric_fields.items()):
            raise(ValueError('Metric aggregation type should be one of [{}]'
                             .format(', '.join(ALLOWED_METRICS))))

        if list(SUPPORTED_INTERVALS.keys()).index(interval) \
                > list(SUPPORTED_INTERVALS.keys()).index(index_interval):
            raise(ValueError('Aggregation interval should be'
                             ' shorter than index interval'))

    def _get_oldest_event_timestamp(self):
        """Search for the oldest event timestamp."""
        # Retrieve the oldest event in order to start aggregation
        # from there
        query_events = Search(
            using=self.client,
            index=self.event_index
        ).sort(
            {'timestamp': {'order': 'asc'}}
        )[0:1]
        result = query_events.execute()
        # There might not be any events yet if the first event have been
        # indexed but the indices have not been refreshed yet.
        if len(result) == 0:
            return None
        return parser.parse(result[0]['timestamp'])

    @property
    def doc_type(self):
        """Get document type for the aggregation."""
        return get_doctype('{0}-{1}-aggregation'.format(
            self.event, self.interval))

    def _split_date_range(self, lower_limit, upper_limit):
        res = {}
        current_interval = lower_limit
        delta = INTERVAL_DELTAS[self.interval]
        while current_interval < upper_limit:
            dt_key = current_interval.strftime(
                SUPPORTED_INTERVALS[self.interval])
            res[dt_key] = current_interval
            current_interval += delta

        dt_key = upper_limit.strftime(
            SUPPORTED_INTERVALS[self.interval])
        res[dt_key] = upper_limit
        return res

    def agg_iter(self, dt):
        """Aggregate and return dictionary to be indexed in ES."""
        rounded_dt = format_range_dt(dt, self.interval)
        self.agg_query = Search(using=self.client, index=self.event_index) \
            .filter('range', timestamp={'gte': rounded_dt, 'lte': rounded_dt})

        # apply query modifiers
        for modifier in self.query_modifiers:
            self.agg_query = modifier(self.agg_query)

        total_buckets = get_bucket_size(
            self.client,
            self.event_index,
            self.field,
            start_date=rounded_dt,
            end_date=rounded_dt,
        )

        num_partitions = max(
            int(math.ceil(float(total_buckets) / self.max_bucket_size)), 1)
        for p in range(num_partitions):
            terms = self.agg_query.aggs.bucket(
                'terms', 'terms',
                field=self.field,
                include={'partition': p, 'num_partitions': num_partitions},
                size=self.max_bucket_size,
            )
            terms.metric(
                'top_hit', 'top_hits', size=1, sort={'timestamp': 'desc'}
            )
            for dst, (metric, src, opts) in self.metric_fields.items():
                terms.metric(dst, metric, field=src, **opts)

            results = self.agg_query.execute(
                # NOTE: Without this, the aggregation changes above, do not
                # invalidate the search's response cache, and thus you would
                # always get the same results for each partition.
                ignore_cache=True,
            )
            for aggregation in results.aggregations['terms'].buckets:
                doc = aggregation.top_hit.hits.hits[0]['_source']
                interval_date = datetime.strptime(
                    doc["timestamp"], "%Y-%m-%dT%H:%M:%S"
                ).replace(
                    **dict.fromkeys(INTERVAL_ROUNDING[self.interval], 0)
                )

                aggregation_data = {}
                aggregation_data['timestamp'] = interval_date.isoformat()
                aggregation_data[self.field] = aggregation['key']
                aggregation_data['count'] = aggregation['doc_count']

                if self.metric_fields:
                    for f in self.metric_fields:
                        aggregation_data[f] = aggregation[f]['value']

                for destination, source in self.copy_fields.items():
                    if isinstance(source, six.string_types):
                        aggregation_data[destination] = doc[source]
                    else:
                        aggregation_data[destination] = source(
                            doc,
                            aggregation_data
                        )

                index_name = 'stats-{0}-{1}'.format(
                    self.event, interval_date.strftime(self.index_name_suffix))

                yield dict(
                    _id='{0}-{1}'.format(
                        aggregation['key'],
                        interval_date.strftime(self.doc_id_suffix)),
                    _index=prefix_index(index_name),
                    _type=self.doc_type,
                    _source=aggregation_data
                )

    def _upper_limit(self, end_date, lower_limit):
        return min(
            end_date or datetime.max,  # ignore if `None`
            datetime.utcnow(),
        )

    def run(self, start_date=None, end_date=None, update_bookmark=True):
        """Calculate statistics aggregations."""
        # If no events have been indexed there is nothing to aggregate
        if not Index(self.event_index, using=self.client).exists():
            return

        lower_limit = (
            start_date or
            self.bookmark_api.get_bookmark() or
            self._get_oldest_event_timestamp()
        )
        # Stop here if no bookmark could be estimated.
        if lower_limit is None:
            return

        upper_limit = self._upper_limit(end_date, lower_limit)
        dates = self._split_date_range(lower_limit, upper_limit)
        for dt_key, dt in sorted(dates.items()):
            bulk(
                self.client,
                self.agg_iter(dt),
                stats_only=True,
                chunk_size=50
            )
            if update_bookmark:
                self.bookmark_api.set_bookmark(dt.strftime(self.doc_id_suffix))

    def list_bookmarks(self, start_date=None, end_date=None, limit=None):
        """List the aggregation's bookmarks."""
        return self.bookmark_api.list_bookmarks(start_date, end_date, limit)

    def delete(self, start_date=None, end_date=None):
        """Delete aggregation documents."""
        aggs_query = Search(
            using=self.client,
            index=self.index,
            doc_type=self.doc_type
        ).extra(_source=False)

        range_args = {}
        if start_date:
            range_args['gte'] = format_range_dt(start_date, self.interval)
        if end_date:
            range_args['lte'] = format_range_dt(end_date, self.interval)
        if range_args:
            aggs_query = aggs_query.filter('range', timestamp=range_args)

        bookmarks_query = Search(
            using=self.client,
            index=self.bookmark_api.bookmark_index,
        ).filter(
            'term', aggregation_type=self.name
        ).sort({'date': {'order': 'desc'}})

        if range_args:
            bookmarks_query = bookmarks_query.filter('range', date=range_args)

        def _delete_actions():
            for query in (aggs_query, bookmarks_query):
                affected_indices = set()
                for doc in query.scan():
                    affected_indices.add(doc.meta.index)
                    yield dict(_index=doc.meta.index,
                               _op_type='delete',
                               _id=doc.meta.id,
                               _type=doc.meta.doc_type)
                current_search_client.indices.flush(
                    index=','.join(affected_indices), wait_if_ongoing=True)
        bulk(self.client, _delete_actions(), refresh=True)
