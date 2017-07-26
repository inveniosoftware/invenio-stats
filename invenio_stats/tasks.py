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

"""Celery background tasks."""

from __future__ import absolute_import, print_function

import datetime
from collections import OrderedDict

from celery import shared_task
from elasticsearch.helpers import bulk
from elasticsearch_dsl import Index, Search
from invenio_search import current_search_client

from .proxies import current_stats


@shared_task
def process_events(event_types):
    """Index statistics events."""
    results = []
    for e in event_types:
        results.append((e, current_stats.events[e].processor.run()))
    return results


@shared_task
def aggregate_events(aggregations):
    """Aggregate indexed events."""
    results = []
    for a in aggregations:
        aggregator = current_stats.aggregations[a].aggregator(
            **current_stats.aggregations[a].call_params)
        results.append(aggregator.run())
    return results


class StatAggregator(object):
    """Aggregator class."""

    def __init__(self, client, event,
                 aggregation_field=None,
                 aggregation_interval='month',
                 index_interval='month'):
        """Construct aggregator instance."""
        self.client = client
        self.event = event
        self.aggregation_alias = 'stats-{}'.format(self.event)
        self.aggregation_field = aggregation_field
        self.aggregation_interval = aggregation_interval
        self.index_interval = index_interval
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

    def get_bookmark(self):
        """Get last aggregation date."""
        if not Index(self.aggregation_alias,
                     using=self.client).exists():
            if not Index('events-stats-{}'.format(self.event),
                         using=self.client).exists():
                return datetime.date.today()
            oldest_index = self.get_first_index_with_alias('events-stats-{}'.
                                                           format(self.event))
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
                                              self.supported_intervals[
                                                  self.aggregation_interval])
        return bookmark

    def set_bookmark(self):
        """Set bookmark for starting next aggregation."""
        current_search_client.indices.flush(index='*')

        def _success_date():
            bookmark = {
                'date': self.new_bookmark or datetime.datetime.utcnow().
                strftime(self.supported_intervals[
                    self.aggregation_interval])
            }

            yield dict(_index=self.last_index_written,
                       _type='{}-bookmark'.format(self.event),
                       _source=bookmark)
        if self.last_index_written:
            bulk(self.client,
                 _success_date(),
                 stats_only=True)

    def agg_iter(self):
        """Aggregate and return dictionary to be indexed in ES."""
        aggregation_data = {}
        self.agg_query = Search(using=self.client,
                                index='events-stats-{}'.format(self.event)).\
            filter('range', timestamp={'gte': self.get_bookmark().isoformat(),
                                       'lte': datetime.datetime.utcnow().
                                       replace(microsecond=0).isoformat()})
        self.agg_query.aggs.bucket('per_{}'.format(self.aggregation_interval),
                                   'date_histogram',
                                   field='timestamp',
                                   interval=self.aggregation_interval)
        self.agg_query.aggs['per_{}'.format(self.aggregation_interval)].\
            bucket('per_{}'.format(self.aggregation_field),
                   'terms', field='file_id', size=0)
        results = self.agg_query.execute()
        index_name = None
        for interval in results.aggregations[
                'per_{}'.format(self.aggregation_interval)].buckets:
            interval_date = datetime.datetime.strptime(
                interval['key_as_string'], '%Y-%m-%dT%H:%M:%S')
            for aggregation in interval['per_{}'.format(
                    self.aggregation_field)].buckets:
                aggregation_data['timestamp'] = interval_date.isoformat()
                aggregation_data[self.aggregation_field] = aggregation['key']
                aggregation_data['count'] = aggregation['doc_count']
                index_name = 'stats-{0}-{1}'.\
                             format(self.event,
                                    interval_date.strftime(
                                        self.index_name_suffix))
                yield dict(_id='{0}-{1}'.
                           format(aggregation['key'],
                                  interval_date.strftime(
                                      self.doc_id_suffix)),
                           _index='stats-{0}-{1}'.
                           format(self.event,
                                  interval_date.strftime(
                                      self.index_name_suffix)),
                           _type='{0}-{1}-aggregation'.
                           format(self.event, self.aggregation_interval),
                           _source=aggregation_data)
        self.last_index_written = index_name

    def get_first_index_with_alias(self, alias_name):
        """Get all indices under an alias."""
        result = []
        all_indices = self.client.indices.get_aliases()
        for index in all_indices:
            if alias_name in all_indices[index]['aliases']:
                result.append(index)
        result.sort()
        return result[0]

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
        self.new_bookmark = (datetime.datetime.utcnow() -
                             datetime.timedelta(hours=1)).\
            strftime(self.doc_id_suffix)
        bulk(self.client,
             self.agg_iter(),
             stats_only=True)
        self.set_bookmark()
