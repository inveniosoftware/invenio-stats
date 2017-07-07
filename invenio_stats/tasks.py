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
def aggregate_event(event):
    """Calculate file downloads for today."""
    if event == 'file-download':
        aggregation_field = 'file_id'
    else:
        aggregation_field = 'record_id'
    file_download_aggregator = StatAggregator(
        client=current_search_client,
        event=event,
        aggregation_field=aggregation_field)
    file_download_aggregator.run()


class StatPreprocessor(object):
    """Preprocessor class.

    Subclass this class in order to provide specific statistics calculations.
    """

    def run(self):
        """Run calculations."""
        pass


class StatAggregator(StatPreprocessor):
    """Aggregator class."""

    def __init__(self, client, event,
                 aggregation_field=None,
                 index_name_suffix='%Y-%m',
                 aggregation_interval='day'):
        """Construct aggregator instance."""
        self.client = client
        self.event = event
        self.aggregation_alias = 'stats-{}'.format(self.event)
        self.aggregation_field = aggregation_field
        self.index_name_suffix = index_name_suffix
        self.aggregation_interval = aggregation_interval

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
                                              format(self.event)).date()

        query_bookmark = Search(using=self.client,
                                index=self.aggregation_alias,
                                doc_type='{0}-bookmark'.format(self.event))
        query_bookmark.aggs.metric('latest', 'max', field='date')
        bookmark = query_bookmark.execute()[0]
        bookmark = datetime.datetime.strptime(bookmark.date, '%Y-%m-%d')
        return bookmark.date()

    def set_bookmark(self):
        """Set bookmark for starting next aggregation."""
        def _success_date():
            bookmark = {
                'date': datetime.date.today().isoformat()
            }
            yield dict(_index='stats-{0}-{1}'.
                       format(self.event,
                              datetime.date.today().strftime(
                                  self.index_name_suffix)),
                       _type='{}-bookmark'.format(self.event),
                       _source=bookmark)

        bulk(self.client,
             _success_date(),
             stats_only=True)

    def date_range(self, start_date, end_date):
        """Get all dates that haven't been processed."""
        if start_date >= end_date:
            for n in range((start_date - end_date).days + 1):
                yield end_date + datetime.timedelta(n)
        else:
            for n in range((end_date - start_date).days + 1):
                yield start_date + datetime.timedelta(n)

    def agg_iter(self, date):
        """Aggregate and return dictionary to be indexed in ES."""
        aggregation_data = {}
        result = self.event_agg_query.execute()
        for aggregation in result.aggregations:
            for hit in result.aggregations[aggregation]['buckets']:
                aggregation_data[self.aggregation_field] = hit['key']
                aggregation_data['count'] = hit['doc_count']
                print('aggregation_data:', aggregation_data)

                tobeindexed = dict(_id='{0}_{1}'.
                                   format(hit['key'],
                                          date),
                                   _index='stats-{0}-{1}'.
                                   format(self.event,
                                          date.strftime(
                                              self.index_name_suffix)),
                                   _type='{}-aggregation'.
                                   format(self.event),
                                   _source=aggregation_data)
                yield tobeindexed

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
        for date in self.date_range(self.get_bookmark(),
                                    datetime.date.today()):
            print("Aggregating day:", date)
            events_alias_name = 'events-stats-{0}'.format(self.event)
            print(events_alias_name)
            if not self.get_indices_with_alias(events_alias_name):
                continue

            self.event_agg_query = Search(using=current_search_client,
                                          index=events_alias_name)
            self.event_agg_query.aggs.bucket('by-{}'.format(self.event),
                                             'terms',
                                             field=self.aggregation_field,
                                             size=999999)
            bulk(self.client,
                 self.agg_iter(date),
                 stats_only=True)
            self.set_bookmark()
