# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2017-2019 CERN.
# Copyright (C)      2022 TU Wien.
#
# Invenio is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Aggregation classes."""

import math
from datetime import datetime

from dateutil import parser
from dateutil.relativedelta import relativedelta
from invenio_search import current_search_client
from invenio_search.engine import dsl, search
from invenio_search.utils import prefix_index

from .bookmark import SUPPORTED_INTERVALS, BookmarkAPI, format_range_dt
from .utils import get_bucket_size

INTERVAL_ROUNDING = {
    "hour": ("minute", "second", "microsecond"),
    "day": ("hour", "minute", "second", "microsecond"),
    "month": ("day", "hour", "minute", "second", "microsecond"),
}

INTERVAL_DELTAS = {
    "hour": relativedelta(hours=1),
    "day": relativedelta(days=1),
    "month": relativedelta(months=1),
}


def filter_robots(query):
    """Modify a search query so that robot events are filtered out."""
    return query.filter("term", is_robot=False)


ALLOWED_METRICS = {
    "avg",
    "cardinality",
    "extended_stats",
    "geo_centroid",
    "max",
    "min",
    "percentiles",
    "stats",
    "sum",
}


class StatAggregator(object):
    """Generic aggregation class.

    This aggregation class queries search events and creates a new
    search document for each aggregated day/month/year... This enables
    to "compress" the events and keep only relevant information.

    The expected events should have at least those two fields:

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
            "count": "<NUMBER OF OCCURRENCE OF THIS EVENT>",
            "field_metric": "<METRIC CALCULATION ON A FIELD>",
            "updated_timestamp": "<ISO DATE TIME>"
        }

    This aggregator saves a bookmark document after each run. This bookmark
    is used to aggregate new events without having to redo the old ones.

    Note the difference between the `timestamp` and the `updated_timestamp`. The first one identifies
    the date that is being calculated. The second one is to identify when the aggregation was modified.
    That might be useful if there are more actions depending on that action, like reindexing.
    """

    def __init__(
        self,
        name,
        event,
        client=None,
        field=None,
        metric_fields=None,
        copy_fields=None,
        query_modifiers=None,
        interval="day",
        index_interval="month",
        max_bucket_size=10000,
    ):
        """Construct aggregator instance.

        :param event: aggregated event.
        :param client: search client.
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
        :param index_interval: time window of the search indices which
            will contain the resulting aggregations.
        """
        self.name = name
        self.event = event
        self.event_index = prefix_index(f"events-stats-{event}")
        self.client = client or current_search_client
        self.index = prefix_index(f"stats-{event}")
        self.field = field
        self.metric_fields = metric_fields or {}
        self.interval = interval
        self.doc_id_suffix = SUPPORTED_INTERVALS[interval]
        self.index_interval = index_interval
        self.index_name_suffix = SUPPORTED_INTERVALS[index_interval]
        self.copy_fields = copy_fields or {}
        self.query_modifiers = (
            query_modifiers if query_modifiers is not None else [filter_robots]
        )
        self.bookmark_api = BookmarkAPI(self.client, self.name, self.interval)
        self.max_bucket_size = max_bucket_size

        if any(v not in ALLOWED_METRICS for k, (v, _, _) in self.metric_fields.items()):
            raise (
                ValueError(
                    "Metric aggregation type should be one of [{}]".format(
                        ", ".join(ALLOWED_METRICS)
                    )
                )
            )

        if list(SUPPORTED_INTERVALS.keys()).index(interval) > list(
            SUPPORTED_INTERVALS.keys()
        ).index(index_interval):
            raise (
                ValueError("Aggregation interval should be shorter than index interval")
            )

    def _get_oldest_event_timestamp(self):
        """Search for the oldest event timestamp."""
        # Retrieve the oldest event in order to start aggregation
        # from there
        query_events = (
            dsl.Search(using=self.client, index=self.event_index)
            .sort({"timestamp": {"order": "asc"}})
            .extra(size=1)
        )
        result = query_events.execute()
        # There might not be any events yet if the first event have been
        # indexed but the indices have not been refreshed yet.
        if len(result) == 0:
            return None
        return parser.parse(result[0]["timestamp"])

    def _split_date_range(self, lower_limit, upper_limit):
        """Return dict of rounded dates in range, split by aggregation interval.

        .. code-block:: python

            self._split_date_range(
                datetime(2023, 1, 10, 12, 34),
                datetime(2023, 1, 13, 11, 20),
            ) == {
                "2023-01-10": datetime.datetime(2023, 1, 10, 12, 34),
                "2023-01-11": datetime.datetime(2023, 1, 11, 12, 34),
                "2023-01-12": datetime.datetime(2023, 1, 12, 12, 34),
                "2023-01-13": datetime.datetime(2023, 1, 13, 11, 20),
            }
        """
        res = {}
        current_interval = lower_limit
        delta = INTERVAL_DELTAS[self.interval]
        while current_interval < upper_limit:
            dt_key = current_interval.strftime(SUPPORTED_INTERVALS[self.interval])
            res[dt_key] = current_interval
            current_interval += delta

        dt_key = upper_limit.strftime(SUPPORTED_INTERVALS[self.interval])
        res[dt_key] = upper_limit
        return res

    def agg_iter(self, dt, previous_bookmark):
        """Aggregate and return dictionary to be indexed in the search engine."""
        rounded_dt = format_range_dt(dt, self.interval)
        agg_query = (
            dsl.Search(using=self.client, index=self.event_index).filter(
                # Filter for the specific interval (hour, day, month)
                "term",
                timestamp=rounded_dt,
            )
            # we're only interested in the aggregated results but not the search hits,
            # so we tell the search to ignore them to save some bandwidth
            .extra(size=0)
        )
        # apply query modifiers
        for modifier in self.query_modifiers:
            agg_query = modifier(agg_query)

        total_buckets = get_bucket_size(
            self.client,
            self.event_index,
            self.field,
            start_date=rounded_dt,
            end_date=rounded_dt,
        )

        num_partitions = max(
            int(math.ceil(float(total_buckets) / self.max_bucket_size)), 1
        )
        for p in range(num_partitions):
            terms = agg_query.aggs.bucket(
                "terms",
                "terms",
                field=self.field,
                include={"partition": p, "num_partitions": num_partitions},
                size=self.max_bucket_size,
            )
            terms.metric("top_hit", "top_hits", size=1, sort={"timestamp": "desc"})
            for dst, (metric, src, opts) in self.metric_fields.items():
                terms.metric(dst, metric, field=src, **opts)
            # Let's get also the last time that the event happened
            terms.metric("last_update", "max", field="updated_timestamp")

            results = agg_query.execute(
                # NOTE: Without this, the aggregation changes above, do not
                # invalidate the search's response cache, and thus you would
                # always get the same results for each partition.
                ignore_cache=True,
            )
            for aggregation in results.aggregations["terms"].buckets:
                doc = aggregation.top_hit.hits.hits[0]["_source"].to_dict()
                aggregation = aggregation.to_dict()
                interval_date = datetime.strptime(
                    doc["timestamp"], "%Y-%m-%dT%H:%M:%S"
                ).replace(**dict.fromkeys(INTERVAL_ROUNDING[self.interval], 0))
                if aggregation["last_update"]["value_as_string"] and previous_bookmark:
                    last_date = datetime.fromisoformat(
                        aggregation["last_update"]["value_as_string"].rstrip("Z")
                    )
                    if last_date < previous_bookmark:
                        continue
                aggregation_data = {}
                aggregation_data["timestamp"] = interval_date.isoformat()
                aggregation_data[self.field] = aggregation["key"]
                aggregation_data["count"] = aggregation["doc_count"]
                aggregation_data["updated_timestamp"] = datetime.utcnow().isoformat()

                if self.metric_fields:
                    for f in self.metric_fields:
                        aggregation_data[f] = aggregation[f]["value"]

                for destination, source in self.copy_fields.items():
                    if isinstance(source, str):
                        aggregation_data[destination] = doc[source]
                    else:
                        aggregation_data[destination] = source(doc, aggregation_data)

                index_name = prefix_index(
                    "stats-{0}-{1}".format(
                        self.event, interval_date.strftime(self.index_name_suffix)
                    )
                )
                yield {
                    "_id": "{0}-{1}".format(
                        aggregation["key"], interval_date.strftime(self.doc_id_suffix)
                    ),
                    "_index": index_name,
                    "_source": aggregation_data,
                }

    def _upper_limit(self, end_date):
        return min(
            end_date or datetime.max,  # ignore if `None`
            datetime.utcnow(),
        )

    def run(self, start_date=None, end_date=None, update_bookmark=True):
        """Calculate statistics aggregations."""
        # If no events have been indexed there is nothing to aggregate
        if not dsl.Index(self.event_index, using=self.client).exists():
            return

        previous_bookmark = self.bookmark_api.get_bookmark()
        lower_limit = (
            start_date or previous_bookmark or self._get_oldest_event_timestamp()
        )
        # Stop here if no bookmark could be estimated.
        if lower_limit is None:
            return

        upper_limit = self._upper_limit(end_date)
        dates = self._split_date_range(lower_limit, upper_limit)
        # Let's get the timestamp before we start the aggregation.
        # This will be used for the next iteration. Some events might be processed twice
        if not end_date:
            end_date = datetime.utcnow().isoformat()

        results = []
        for dt_key, dt in sorted(dates.items()):
            results.append(
                search.helpers.bulk(
                    self.client,
                    self.agg_iter(dt, previous_bookmark),
                    stats_only=True,
                    chunk_size=50,
                )
            )
        if update_bookmark:
            self.bookmark_api.set_bookmark(end_date)
        return results

    def list_bookmarks(self, start_date=None, end_date=None, limit=None):
        """List the aggregation's bookmarks."""
        return self.bookmark_api.list_bookmarks(start_date, end_date, limit)

    def delete(self, start_date=None, end_date=None):
        """Delete aggregation documents."""
        aggs_query = dsl.Search(
            using=self.client,
            index=self.index,
        ).extra(_source=False)

        range_args = {}
        if start_date:
            range_args["gte"] = format_range_dt(start_date, self.interval)
        if end_date:
            range_args["lte"] = format_range_dt(end_date, self.interval)
        if range_args:
            aggs_query = aggs_query.filter("range", timestamp=range_args)

        bookmarks_query = (
            dsl.Search(
                using=self.client,
                index=self.bookmark_api.bookmark_index,
            )
            .filter("term", aggregation_type=self.name)
            .sort({"date": {"order": "desc"}})
        )

        if range_args:
            bookmarks_query = bookmarks_query.filter("range", date=range_args)

        def _delete_actions():
            for query in (aggs_query, bookmarks_query):
                affected_indices = set()
                for doc in query.scan():
                    affected_indices.add(doc.meta.index)
                    yield {
                        "_index": doc.meta.index,
                        "_op_type": "delete",
                        "_id": doc.meta.id,
                    }
                current_search_client.indices.flush(
                    index=",".join(affected_indices), wait_if_ongoing=True
                )

        search.helpers.bulk(self.client, _delete_actions(), refresh=True)
