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

from celery import shared_task
import datetime

from invenio_search import current_search_client
from elasticsearch.helpers import bulk
from elasticsearch_dsl import Search, Index

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
    file_download_aggregator = StatAggregator(
        client=current_search_client,
        event='file_download',
        time_frame='monthly')
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
                 time_frame, query='*'):
        """Construct aggregator instance."""
        self.client = client
        self.event = event
        self.time_frame = time_frame
        self.query = query
        self.aggregation_alias = '{0}-{1}'.format(self.time_frame,
                                                  self.event)

    def get_bookmark(self):
        """Get last aggregation date."""
        if not Index(self.aggregation_alias,
                     using=self.client).exists():
            oldest_index = self.get_indices_with_alias('events-stats_{}'.
                                                       format(self.event))
            return datetime.datetime.strptime(oldest_index,
                                              'events-stats_{}-%Y.%m.%d'.
                                              format(self.event)).date()

        import ipdb
        ipdb.set_trace()
        query_bookmark = Search(using=self.client,
                                index=self.aggregation_alias,
                                doc_type='{0}_bookmark'.format(self.event))
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
            yield dict(_index='{0}-{1}-{2}'.
                       format(self.time_frame,
                              self.event,
                              datetime.date.today().strftime('%Y-%m')),
                       _type='{0}_bookmark'.format(self.event),
                       _source=bookmark)

        bulk(self.client,
             _success_date(),
             stats_only=True)

    def pending_dates(self):
        """Get all dates that haven't been processed."""
        start_date = self.get_bookmark()
        end_date = datetime.date.today()
        import ipdb
        ipdb.set_trace()
        if start_date >= end_date:
            for n in range((start_date - end_date).days + 1):
                yield start_date + datetime.timedelta(n)
        else:
            for n in range((end_date - start_date).days + 1):
                yield start_date - datetime.timedelta(n)

    def aggr_iter(self, date):
        """Aggregate and return dictionary to be indexed in ES."""
        aggregation_data = {}
        result = self.query.execute()

        import ipdb
        ipdb.set_trace()

        for aggregation in result.aggregations:
            for hit in result.aggregations[aggregation]['buckets']:
                aggregation_data['bucket'] = hit['key']
                aggregation_data['count'] = hit['doc_count']
                print('aggregation_data:', aggregation_data)

                yield dict(_id='{0}_{1}'.
                           format(hit['key'],
                                  date),
                           _index='{0}-{1}-{2}'.
                           format(self.time_frame,
                                  self.event,
                                  date.strftime('%Y-%m')),
                           _type='{0}_aggregation'.
                           format(self.event),
                           _source=aggregation_data)

    def get_indices_with_alias(self, alias_name):
        """Get all indices under an alias."""
        result = []
        all_indices = current_search_client.indices.get_aliases()
        for index in all_indices:
            if alias_name in all_indices[index]['aliases']:
                result.append(index)
        result.sort()
        return result[0]

    def run(self):
        """Calculate statistics aggregations."""
        for date in self.pending_dates():
            print("Aggregating date:", date)
            index_name = 'events-stats_{0}-{1}'.format(
                self.event, date.isoformat().replace('-', '.'))
            if not Index(index_name, using=current_search_client).exists():
                continue

            self.query = Search(using=current_search_client,
                                index=index_name)
            self.query.aggs.bucket('by_{}'.format(self.event),
                                   'terms', field='bucket',
                                   size=999999)
            bulk(self.client,
                 self.aggr_iter(date),
                 stats_only=True)
        self.set_bookmark()
