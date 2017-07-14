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
        event='file-download')
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

    def __init__(self, client, event):
        """Construct aggregator instance."""
        self.client = client
        self.event = event
        self.aggregation_alias = 'stats-{}'.format(self.event)

    def query(self, query_field='_id', obj_id='all', time_range='total'):
        if time_range == 'total':
            agg_query = Search(using=self.client,
                               index=self.aggregation_alias)
            if obj_id == 'all':
                pass
            else:
                field = {query_field: obj_id}
                agg_query = Search(using=self.client,
                                   index=self.aggregation_alias).\
                    query('match', **field)
        if time_range == 'month':
            if obj_id == 'all':
                agg_query = Search(using=self.client,
                                   index='stats-{0}-{1}'.
                                   format(self.event,
                                          datetime.date.today().
                                          strftime('%Y-%m')))
            else:
                field = {query_field: obj_id}
                agg_query = Search(using=self.client,
                                   index='stats-{0}-{1}'.
                                   format(self.event,
                                          datetime.date.today().
                                          strftime('%Y-%m'))).\
                    query('match', **field)

        agg_query.aggs.bucket('by-{}'.format(self.event),
                              'terms', field=query_field,
                              size=9999999)
        result = agg_query.execute()
        return result.hits.hits

    def get_total_count(self, query_field='_id', obj_id='all'):
        if obj_id == 'all':
            agg_query = Search(using=self.client,
                               index=self.aggregation_alias)
        else:
            field = {query_field: obj_id}
            agg_query = Search(using=self.client,
                               index=self.aggregation_alias).\
                query('match', **field)
        agg_query.aggs.bucket('by-{}'.format(self.event),
                              'terms', field=query_field,
                              size=9999999)
        result = agg_query.execute()
        return result.hits.hits

    def get_last_month_count(self, query_field='_id', obj_id='all'):
        if obj_id == 'all':
            agg_query = Search(using=self.client,
                               index='stats-{0}-{1}'.
                               format(self.event,
                                      datetime.date.today().
                                      strftime('%Y-%m')))
        else:
            field = {query_field: obj_id}
            agg_query = Search(using=self.client,
                               index='stats-{0}-{1}'.
                               format(self.event,
                                      datetime.date.today().
                                      strftime('%Y-%m'))).\
                query('match', **field)
        agg_query.aggs.bucket('by-{}'.format(self.event),
                              'terms', field=query_field,
                              size=9999999)
        result = agg_query.execute()
        return result.hits.hits

    def get_last_week_count(self, query_field='_id', obj_id='all'):
        week_results = []
        date_today = datetime.date.today()
        for date in self.date_range(date_today - datetime.timedelta(7),
                                    date_today):
            print(date)
            index_name = 'events-stats-{0}-{1}'.\
                         format(self.event, date.strftime('%Y-%m-%d'))
            if not Index(index_name, using=self.client).exists():
                continue
            if obj_id == 'all':
                day_query = Search(using=self.client,
                                   index=index_name)
            else:
                field = {query_field: obj_id}
                day_query = Search(using=self.client,
                                   index=index_name).\
                    query('match', **field)
            week_results += day_query.execute().hits.hits
        return week_results

    def get_count_by_day(self, day):
        index_name = 'events-stats-{0}-{1}'.format(self.event, day)
        if not Index(index_name, using=self.client).exists():
            print("Index doesn't exist:", index_name)
            return 0
        count_query = Search(using=self.client, index=index_name)
        return count_query.count()

    def get_count_in_day_range(self, time_range):
        result = {}
        for day in self.date_range(time_range[0], time_range[1]):
            result[day.isoformat()] = \
                self.get_count_by_day(day)
        return result

    def get_bookmark(self):
        """Get last aggregation date."""
        import ipdb
        ipdb.set_trace()
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
                              datetime.date.today().strftime('%Y-%m')),
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

        import ipdb
        ipdb.set_trace()
        for aggregation in result.aggregations:
            for hit in result.aggregations[aggregation]['buckets']:
                aggregation_data['bucket'] = hit['key']
                aggregation_data['count'] = hit['doc_count']
                print('aggregation_data:', aggregation_data)

                tobeindexed = dict(_id='{0}_{1}'.
                                   format(hit['key'],
                                          date),
                                   _index='stats-{0}-{1}'.
                                   format(self.event,
                                          date.strftime('%Y-%m')),
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

    def run(self):
        """Calculate statistics aggregations."""
        for date in self.date_range(self.get_bookmark(),
                                    datetime.date.today()):
            print("Aggregating day:", date)
            index_name = 'events-stats-{0}-{1}'.format(self.event, date)
            print(index_name)
            if not Index(index_name, using=self.client).exists():
                continue

            self.event_agg_query = Search(using=current_search_client,
                                          index=index_name)
            self.event_agg_query.aggs.bucket('by-{}'.format(self.event),
                                             'terms', field='bucket_id',
                                             size=999999)
            bulk(self.client,
                 self.agg_iter(date),
                 stats_only=True)
        self.set_bookmark()
