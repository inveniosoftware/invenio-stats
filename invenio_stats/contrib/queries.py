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

"""Registration of contrib events."""

from datetime import datetime

import dateutil.parser
import six
from elasticsearch_dsl import Search

from ..queries import ESQuery


def register():
    """Register queries."""
    return [
        dict(
            query_name='file-download',
            query_class=FileDownloadsQuery,
            query_config=dict(
                index='stats-file-download',
                doc_type='file-download-day-aggregation'
            )
        ),
    ]


allowed_intervals = ['year', 'quarter', 'month', 'week', 'day', 'hour',
                     'minute', 'second']


def extract_date(date):
    """Extract date from string if necessary.

    :returns: the extracted date.
    """
    if isinstance(date, six.string_types):
        try:
            date = dateutil.parser.parse(date)
        except ValueError:
            raise ValueError(
                'Invalid date format for statistic {}.'
            ).format(self.query_name)
    if not isinstance(date, datetime):
        raise TypeError(
            'Invalid date type for statistic {}.'
        ).format(self.query_name)
    return date


class FileDownloadsQuery(ESQuery):
    """Query returning file downloads statistics."""

    def run(self, aggregation_interval='day', start_date=None,
            end_date=None):
        """Run the query."""
        # Validate arguments
        start_date = extract_date(start_date) if start_date else None
        end_date = extract_date(end_date) if end_date else None
        if aggregation_interval not in allowed_intervals:
            raise ValueError(
                'Invalid aggregation time interval for statistic {}.'
            ).format(self.query_name)
        self.agg_query = Search(using=self.client,
                                index=self.index,
                                doc_type=self.doc_type)
        if start_date is not None or end_date is not None:
            time_range = {}
            if start_date is not None:
                time_range['gte'] = start_date.isoformat()
            if end_date is not None:
                time_range['lt'] = end_date.isoformat()
            self.agg_query.filter('range', timestamp=time_range)
        hist_agg = self.agg_query.aggs.bucket(
            'per_{}'.format(aggregation_interval),
            'date_histogram',
            field='timestamp',
            interval=aggregation_interval
        )
        hist_agg.metric('total', 'sum', field='count')
        query_result = self.agg_query.execute().to_dict()
        for agg in query_result['aggregations']['per_day']['buckets']:
            agg['count'] = agg['total']['value']
            del agg['total']
            del agg['doc_count']
        return dict(
            aggregations=query_result['aggregations'],
        )
