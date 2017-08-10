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

"""Query processing classes."""

from datetime import datetime

import dateutil.parser
import six
from elasticsearch_dsl import Search
from invenio_search import current_search_client

from .errors import InvalidRequestInputError


class ESQuery(object):
    """Elasticsearch query."""

    def __init__(self, query_name, doc_type, index, client=None,
                 *args, **kwargs):
        """Constructor.

        :param doc_type: queried document type.
        :param index: queried index.
        :param client: elasticsearch client used to query.
        """
        super(ESQuery, self).__init__()
        self.index = index
        self.client = client or current_search_client
        self.query_name = query_name
        self.doc_type = doc_type

    def run(self, *args, **kwargs):
        """Run the query."""
        raise NotImplementedError()


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


class ESDateHistogramQuery(ESQuery):
    """Elasticsearch date histogram query."""

    allowed_intervals = ['year', 'quarter', 'month', 'week', 'day']
    """Allowed intervals for the histogram aggregation."""

    def __init__(self, time_field='timestamp', copy_fields=None,
                 required_filters=None, *args, **kwargs):
        """Constructor.

        :param time_field: name of the timestamp field.
        :param copy_fields: list of fields to copy from the top hit document
            into the resulting aggregation item.
        :param required_filters: Dict of "mandatory query parameter" ->
            "filtered field".
        """
        super(ESDateHistogramQuery, self).__init__(*args, **kwargs)
        self.time_field = time_field
        self.copy_fields = copy_fields or dict()
        self.required_filters = required_filters or {}

    def validate_arguments(self, interval, start_date, end_date, **kwargs):
        """Validate query arguments."""
        if interval not in self.allowed_intervals:
            raise InvalidRequestInputError(
                'Invalid aggregation time interval for statistic {}.'
            ).format(self.query_name)
        if kwargs.keys() != self.required_filters.keys():
            raise InvalidRequestInputError(
                'Missing one of the required parameters {0} in '
                'query {1}'.format(set(self.required_filters.keys()),
                                   self.query_name)
            )

    def build_query(self, interval, start_date, end_date, **kwargs):
        """Build the elasticsearch query."""
        agg_query = Search(using=self.client,
                           index=self.index,
                           doc_type=self.doc_type)[0:0]
        if start_date is not None or end_date is not None:
            time_range = {}
            if start_date is not None:
                time_range['gte'] = start_date.isoformat()
            if end_date is not None:
                time_range['lte'] = end_date.isoformat()
            agg_query = agg_query.filter(
                'range',
                **{self.time_field: time_range})
        hist_agg = agg_query.aggs.bucket(
            'histogram',
            'date_histogram',
            field=self.time_field,
            interval=interval
        )
        hist_agg.metric('total', 'sum', field='count')

        if self.copy_fields:
            hist_agg.metric(
                'top_hit', 'top_hits', size=1, sort={'timestamp': 'desc'}
            )

        for query_param, filtered_field in self.required_filters.items():
            if query_param in kwargs:
                agg_query = agg_query.filter(
                    'term', **{filtered_field: kwargs[query_param]}
                )

        return agg_query

    def process_query_result(self, result, interval, start_date, end_date):
        """Build the result using the query result."""
        def build_bucket(agg):
            result = dict(
                value=agg['total']['value'],
                key=agg['key'],
            )
            if self.copy_fields and agg['top_hit']['hits']['hits']:
                doc = agg['top_hit']['hits']['hits'][0]['_source']
                for destination, source in self.copy_fields.items():
                    if isinstance(source, six.string_types):
                        result[destination] = doc[source]
                    else:
                        result[destination] = source(
                            result,
                            doc
                        )
            return result
        return dict(
            type='bucket',
            key_type='date',
            interval=interval,
            buckets=list(map(build_bucket,
                             result['aggregations']['histogram']['buckets']))
        )

    def run(self, interval='day', start_date=None,
            end_date=None, **kwargs):
        """Run the query."""
        start_date = extract_date(start_date) if start_date else None
        end_date = extract_date(end_date) if end_date else None
        self.validate_arguments(interval, start_date, end_date, **kwargs)

        agg_query = self.build_query(interval, start_date,
                                     end_date, **kwargs)
        query_result = agg_query.execute().to_dict()
        res = self.process_query_result(query_result, interval,
                                        start_date, end_date)
        return res


class ESTermsQuery(ESQuery):
    """Elasticsearch sum query."""

    def __init__(self, time_field='timestamp', copy_fields=None,
                 required_filters=None, aggregated_fields=None,
                 *args, **kwargs):
        """Constructor.

        :param time_field: name of the timestamp field.
        :param copy_fields: list of fields to copy from the top hit document
            into the resulting aggregation item.
        :param required_filters: Dict of "mandatory query parameter" ->
            "filtered field".
        :param aggregated_fields: List of fields which will be used in the
            terms aggregations.
        """
        super(ESTermsQuery, self).__init__(*args, **kwargs)
        self.time_field = time_field
        self.copy_fields = copy_fields or dict()
        self.required_filters = required_filters or {}
        self.aggregated_fields = aggregated_fields or []
        assert len(self.aggregated_fields) > 0

    def validate_arguments(self, start_date, end_date, **kwargs):
        """Validate query arguments."""
        if kwargs.keys() != self.required_filters.keys():
            raise InvalidRequestInputError(
                'Missing one of the required parameters {0} in '
                'query {1}'.format(set(self.required_filters.keys()),
                                   self.query_name)
            )

    def build_query(self, start_date, end_date, **kwargs):
        """Build the elasticsearch query."""
        agg_query = Search(using=self.client,
                           index=self.index,
                           doc_type=self.doc_type)[0:0]
        if start_date is not None or end_date is not None:
            time_range = {}
            if start_date is not None:
                time_range['gte'] = start_date.isoformat()
            if end_date is not None:
                time_range['lte'] = end_date.isoformat()
            agg_query = agg_query.filter(
                'range',
                **{self.time_field: time_range})

        term_agg = agg_query.aggs
        for term in self.aggregated_fields:
            term_agg = term_agg.bucket(term, 'terms', field=term, size=0)
        term_agg.metric('total', 'sum', field='count')

        if self.copy_fields:
            term_agg.metric(
                'top_hit', 'top_hits', size=1, sort={'timestamp': 'desc'}
            )

        for query_param, filtered_field in self.required_filters.items():
            if query_param in kwargs:
                agg_query = agg_query.filter(
                    'term', **{filtered_field: kwargs[query_param]}
                )

        return agg_query

    def process_query_result(self, result, start_date, end_date):
        """Build the result using the query result."""
        def build_buckets(agg, fields, res):
            """Build recursively result buckets."""
            if fields:
                field = fields[0]
                res.update(
                    type='bucket',
                    field=field,
                    key_type='terms',
                    buckets=list(map(
                        lambda sub: build_buckets(sub, fields[1:],
                                                  dict(key=sub['key'])),
                        agg[field]['buckets']))
                )
            else:
                res.update(
                    value=agg['total']['value'],
                    key=agg['key'],
                )
                if self.copy_fields and agg['top_hit']['hits']['hits']:
                    doc = agg['top_hit']['hits']['hits'][0]['_source']
                    for destination, source in self.copy_fields.items():
                        if isinstance(source, six.string_types):
                            res[destination] = doc[source]
                        else:
                            res[destination] = source(
                                res,
                                doc
                            )
            return res

        return build_buckets(result['aggregations'], self.aggregated_fields,
                             dict())

    def run(self, start_date=None, end_date=None, **kwargs):
        """Run the query."""
        start_date = extract_date(start_date) if start_date else None
        end_date = extract_date(end_date) if end_date else None
        self.validate_arguments(start_date, end_date, **kwargs)

        agg_query = self.build_query(start_date, end_date, **kwargs)
        query_result = agg_query.execute().to_dict()
        res = self.process_query_result(query_result, start_date, end_date)
        return res
