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
from invenio_search import current_search_client

from invenio_stats.aggregations import StatAggregator
from invenio_stats.contrib.event_builders import build_file_unique_id
from invenio_stats.processors import EventsIndexer, anonymize_user, flag_robots
from invenio_stats.queries import ESDateHistogramQuery, ESTermsQuery


def register_events():
    """Register sample events."""
    return [dict(event_type='file-download',
                 templates='contrib/file-download',
                 processor_class=EventsIndexer,
                 processor_config=dict(
                    preprocessors=[
                        flag_robots,
                        anonymize_user,
                        build_file_unique_id
                    ]
                 )),
            dict(event_type='record-view',
                 templates='contrib/record-view',
                 processor_class=EventsIndexer)]


def register_aggregations():
    """Register sample aggregations."""
    return [dict(aggregation_name='file-download-agg',
                 templates='contrib/aggregations/aggr-file-download',
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
                     )
                 ))
            ]


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
