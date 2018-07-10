# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2018 CERN.
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

"""CLI tests."""

from click.testing import CliRunner
from conftest import _create_file_download_event, _create_record_view_event
from elasticsearch_dsl import Search

from invenio_stats import current_stats
from invenio_stats.cli import stats


def test_events_process(script_info, event_queues, es_with_templates):
    """Test "events process" CLI command."""
    es = es_with_templates
    search = Search(using=es)
    runner = CliRunner()

    # Invalid argument
    result = runner.invoke(
        stats, ['events', 'process', 'invalid-event-type', '--eager'],
        obj=script_info)
    assert result.exit_code == 2
    assert 'Invalid event type(s):' in result.output

    current_stats.publish(
        'file-download',
        [_create_file_download_event(date) for date in
         [(2018, 1, 1, 10), (2018, 1, 1, 12), (2018, 1, 1, 14)]])
    current_stats.publish(
        'record-view',
        [_create_record_view_event(date) for date in
         [(2018, 1, 1, 10), (2018, 1, 1, 12), (2018, 1, 1, 14)]])

    result = runner.invoke(
        stats, ['events', 'process', 'file-download', '--eager'],
        obj=script_info)
    assert result.exit_code == 0

    es.indices.refresh(index='*')

    assert search.index('events-stats-file-download-2018-01-01').count() == 3
    assert search.index('events-stats-file-download').count() == 3
    assert not es.indices.exists('events-stats-record-view-2018-01-01')
    assert not es.indices.exists_alias(name='events-stats-record-view')

    result = runner.invoke(
        stats, ['events', 'process', 'record-view', '--eager'],
        obj=script_info)
    assert result.exit_code == 0

    es.indices.refresh(index='*')
    assert search.index('events-stats-file-download-2018-01-01').count() == 3
    assert search.index('events-stats-file-download').count() == 3
    assert search.index('events-stats-record-view-2018-01-01').count() == 3
    assert search.index('events-stats-record-view').count() == 3

    # Create some more events
    current_stats.publish(
        'file-download', [_create_file_download_event((2018, 2, 1, 12))])
    current_stats.publish(
        'record-view', [_create_record_view_event((2018, 2, 1, 10))])

    # Process all event types
    result = runner.invoke(
        stats, ['events', 'process', '--eager'], obj=script_info)
    assert result.exit_code == 0

    es.indices.refresh(index='*')
    assert search.index('events-stats-file-download-2018-01-01').count() == 3
    assert search.index('events-stats-file-download-2018-02-01').count() == 1
    assert search.index('events-stats-file-download').count() == 4
    assert search.index('events-stats-record-view-2018-01-01').count() == 3
    assert search.index('events-stats-record-view-2018-02-01').count() == 1
    assert search.index('events-stats-record-view').count() == 4