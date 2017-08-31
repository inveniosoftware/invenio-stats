# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2017 CERN.
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

"""Aggregation classes."""

from __future__ import absolute_import, print_function

import datetime
from collections import OrderedDict

import six
from elasticsearch.helpers import bulk
from elasticsearch_dsl import Index, Search
from invenio_search import current_search_client


def filter_robots(query):
    """Modify an elasticsearch query so that robot events are filtered out."""
    return query.filter('term', is_robot=False)


class StatAggregator(object):
    """Generic aggregation class.

    This aggregation class queries elasticsearch events and creates a new
    elasticsearch document for each aggregated day/month/year... This enables
    to "compress" the events and keep only relevant information.

    The expected events shoud have at least those two fields:

    .. code-block:: JSON

        {
            timestamp: "<ISO DATE TIME>",
            field_on_which_we_aggregate: "<A VALUE>"
        }

    The resulting aggregation documents will be of the form:

        {
            timestamp: "<ISO DATE TIME>",
            field_on_which_we_aggregate: "<A VALUE>",
            count: <NUMBER OF OCCURENCE OF THIS EVENT>
        }

    This aggregator saves a bookmark document after each run. This bookmark
    is used to aggregate new events without having to redo the old ones.
    """

    def __init__(self, client, event,
                 aggregation_field=None,
                 copy_fields=None,
                 query_modifiers=None,
                 aggregation_interval='month',
                 index_interval='month', batch_size=7,
                 subaggregations=[]):
        """Construct aggregator instance."""
        self.client = client
        self.event = event
        self.aggregation_alias = 'stats-{}'.format(self.event)
        self.aggregation_field = aggregation_field
        self.copy_fields = copy_fields or {}
        self.aggregation_interval = aggregation_interval
        self.index_interval = index_interval
        self.query_modifiers = (query_modifiers if query_modifiers is not None
                                else [filter_robots])
        self.supported_intervals = OrderedDict([('hour', '%Y-%m-%dT%H'),
                                                ('day', '%Y-%m-%d'),
                                                ('month', '%Y-%m'),
                                                ('year', '%Y')])
        if list(self.supported_intervals.keys()).index(aggregation_interval) \
                > \
                list(self.supported_intervals.keys()).index(index_interval):
            raise(ValueError('Aggregation interval should be'
                             ' shorter than index interval'))
        self.index_name_suffix = self.supported_intervals[index_interval]
        self.doc_id_suffix = self.supported_intervals[aggregation_interval]
        self.batch_size = batch_size
        self.subaggregations = subaggregations

    def get_bookmark(self):
        """Get last aggregation date."""
        if not Index(self.aggregation_alias,
                     using=self.client).exists():
            if not Index('events-stats-{}'.format(self.event),
                         using=self.client).exists():
                return datetime.date.today()
            indices = self.get_indices_with_alias('events-stats-{}'.
                                                  format(self.event))
            indices.sort()
            oldest_index = indices[0]
            return datetime.datetime.strptime(oldest_index,
                                              'events-stats-{}-%Y-%m-%d'.
                                              format(self.event))
        query_bookmark = Search(using=self.client,
                                index=self.aggregation_alias,
                                doc_type='{0}-bookmark'.format(self.event))
        query_bookmark.aggs.metric('latest', 'max', field='date')
        bookmark = query_bookmark.execute()[0]
        # change it to doc_id_suffix
        bookmark = datetime.datetime.strptime(bookmark.date,
                                              self.doc_id_suffix)
        return bookmark

    def set_bookmark(self):
        """Set bookmark for starting next aggregation."""
        def _success_date():
            bookmark = {
                'date': self.new_bookmark or datetime.datetime.utcnow().
                strftime(self.doc_id_suffix)
            }

            yield dict(_index=self.last_index_written,
                       _type='{}-bookmark'.format(self.event),
                       _source=bookmark)
        if self.last_index_written:
            bulk(self.client,
                 _success_date(),
                 stats_only=True)

    def agg_iter(self, lower_limit=None,
                 upper_limit=datetime.datetime.utcnow().replace(microsecond=0).
                 isoformat()):
        """Aggregate and return dictionary to be indexed in ES."""
        if lower_limit is None:
            lower_limit = self.get_bookmark().isoformat()
        aggregation_data = {}
        self.agg_query = Search(using=self.client,
                                index='events-stats-{}'.format(self.event)).\
            filter('range', timestamp={'gte': lower_limit,
                                       'lte': upper_limit})
        # apply query modifiers
        for modifier in self.query_modifiers:
            self.agg_query = modifier(self.agg_query)

        hist = self.agg_query.aggs.bucket(
            'histogram',
            'date_histogram',
            field='timestamp',
            interval=self.aggregation_interval
        )
        terms = hist.bucket(
            'terms', 'terms', field=self.aggregation_field, size=0
        )
        top = terms.metric(
            'top_hit', 'top_hits', size=1, sort={'timestamp': 'desc'}
        )

        results = self.agg_query.execute()
        index_name = None
        for interval in results.aggregations['histogram'].buckets:
            interval_date = datetime.datetime.strptime(
                interval['key_as_string'], '%Y-%m-%dT%H:%M:%S')
            for aggregation in interval['terms'].buckets:
                aggregation_data['timestamp'] = interval_date.isoformat()
                aggregation_data[self.aggregation_field] = aggregation['key']
                aggregation_data['count'] = aggregation['doc_count']

                doc = aggregation.top_hit.hits.hits[0]['_source']
                for destination, source in self.copy_fields.items():
                    if isinstance(source, six.string_types):
                        aggregation_data[destination] = doc[source]
                    else:
                        aggregation_data[destination] = source(
                            doc,
                            aggregation_data
                        )

                index_name = 'stats-{0}-{1}'.\
                             format(self.event,
                                    interval_date.strftime(
                                        self.index_name_suffix))
                self.indices.add(index_name)
                yield dict(_id='{0}-{1}'.
                           format(aggregation['key'],
                                  interval_date.strftime(
                                      self.doc_id_suffix)),
                           _index=index_name,
                           _type='{0}-{1}-aggregation'.
                           format(self.event, self.aggregation_interval),
                           _source=aggregation_data)
        self.last_index_written = index_name

    def get_indices_with_alias(self, alias_name):
        """Return list of indices with given alias."""
        result = []
        all_indices = self.client.indices.get_aliases()
        for index in all_indices:
            if alias_name in all_indices[index]['aliases']:
                result.append(index)
        return result

    def run(self):
        """Calculate statistics aggregations."""
        lower_limit = self.get_bookmark()
        upper_limit = min(
            datetime.datetime.utcnow().
            replace(microsecond=0),
            datetime.datetime.combine(lower_limit +
                                      datetime.timedelta(self.batch_size),
                                      datetime.datetime.min.time())
        )
        while upper_limit <= datetime.datetime.utcnow():
            self.indices = set()
            self.new_bookmark = upper_limit.strftime(self.doc_id_suffix)
            bulk(self.client,
                 self.agg_iter(lower_limit, upper_limit),
                 stats_only=True,
                 chunk_size=50)
            # Flush all indices which have been modified
            current_search_client.indices.flush(
                index=','.join(self.indices),
                wait_if_ongoing=True
            )
            self.set_bookmark()
            self.indices = set()
            lower_limit = lower_limit + datetime.timedelta(self.batch_size)
            upper_limit = min(datetime.datetime.utcnow().
                              replace(microsecond=0),
                              lower_limit +
                              datetime.timedelta(self.batch_size))
            if lower_limit > upper_limit:
                break
