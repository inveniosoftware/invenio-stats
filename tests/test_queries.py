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

"""Query tests."""

import datetime

import pytest
from conftest import _create_file_download_event
from invenio_queues.proxies import current_queues
from invenio_search import current_search, current_search_client

from invenio_stats.aggregations import StatAggregator
from invenio_stats.contrib.registrations import register_queries
from invenio_stats.proxies import current_stats
from invenio_stats.queries import ESDateHistogramQuery, ESTermsQuery
from invenio_stats.tasks import aggregate_events, process_events


@pytest.mark.parametrize('aggregated_events',
                         [dict(file_number=1,
                               event_number=2,
                               start_date=datetime.date(2017, 1, 1),
                               end_date=datetime.date(2017, 1, 7))],
                         indirect=['aggregated_events'])
def test_histogram_query(app, event_queues, aggregated_events):
    """Test that the histogram query returns the correct
    results for each day."""
    # reading the configuration as it is registered from registrations.py
    query_configs = register_queries()
    histo_query = ESDateHistogramQuery(query_name='test_histo',
                                       **query_configs[0]['query_config'])
    results = histo_query.run(bucket_id='B0000000000000000000000000000001',
                              file_key='test.pdf',
                              start_date=datetime.datetime(2017, 1, 1),
                              end_date=datetime.datetime(2017, 1, 7))
    for day_result in results['buckets']:
        assert int(day_result['value']) == 2


@pytest.mark.parametrize('aggregated_events',
                         [dict(file_number=1,
                               event_number=7,
                               start_date=datetime.date(2017, 1, 1),
                               end_date=datetime.date(2017, 1, 7))],
                         indirect=['aggregated_events'])
def test_terms_query(app, event_queues, aggregated_events):
    """Test that the terms query returns the correct total count."""
    query_configs = register_queries()
    terms_query = ESTermsQuery(query_name='test_total_count',
                               **query_configs[1]['query_config'])
    results = terms_query.run(bucket_id='B0000000000000000000000000000001',
                              start_date=datetime.datetime(2017, 1, 1),
                              end_date=datetime.datetime(2017, 1, 7))
    assert int(results['buckets'][0]['value']) == 49
