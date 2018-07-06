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

"""Aggregation classes."""

from __future__ import absolute_import, print_function

from functools import wraps

import click
from dateutil.parser import parse as dateutil_parse
from flask.cli import with_appcontext
from werkzeug.local import LocalProxy

from .proxies import current_stats
from .tasks import aggregate_events, process_events


def lazy_result(f):
    """Decorate function to return LazyProxy."""
    @wraps(f)
    def decorated(ctx, param, value):
        return LocalProxy(lambda: f(ctx, param, value))
    return decorated


@lazy_result
def _validate_event_type(ctx, param, value):
    invalid_values = set(value) - set(current_stats.enabled_events)
    if invalid_values:
        raise click.BadParameter(
            'Invalid event type(s): {}. Valid values: {}'.format(
                invalid_values, set(current_stats.enabled_events)))
    return value


def _parse_date(ctx, param, value):
    if value:
        return dateutil_parse(value)


@lazy_result
def _validate_aggregation_type(ctx, param, value):
    invalid_values = set(value) - set(current_stats.enabled_aggregations)
    if invalid_values:
        raise click.BadParameter(
            'Invalid aggregation type(s): {}. Valid values: {}'.format(
                invalid_values, set(current_stats.enabled_aggregations)))
    return value


aggr_arg = click.argument(
    'aggregation-types', nargs=-1, callback=_validate_aggregation_type)


@click.group()
def stats():
    """Statistics commands."""


@stats.group()
def events():
    """Event management commands."""


@events.command('process')
@click.argument('event-types', nargs=-1, callback=_validate_event_type)
@click.option('--eager', '-e', is_flag=True)
@with_appcontext
def _events_process(event_types=None, eager=False):
    """Process stats events."""
    event_types = event_types or list(current_stats.enabled_events)
    if eager:
        process_events.apply((event_types,), throw=True)
        click.secho('Events processed successfully.', fg='green')
    else:
        process_events.delay(event_types)
        click.secho('Events processing task sent...', fg='yellow')


@stats.group()
def aggregations():
    """Aggregation management commands."""


@aggregations.command('process')
@aggr_arg
@click.option('--start-date', callback=_parse_date)
@click.option('--end-date', callback=_parse_date)
@click.option('--update-bookmark', '-b', is_flag=True)
@click.option('--eager', '-e', is_flag=True)
@with_appcontext
def _aggregations_process(aggregation_types=None,
                          start_date=None, end_date=None,
                          update_bookmark=False, eager=False):
    """Process stats aggregations."""
    aggregation_types = (aggregation_types or
                         list(current_stats.enabled_aggregations))
    if eager:
        aggregate_events.apply(
            (aggregation_types,),
            dict(start_date=start_date, end_date=end_date,
                 update_bookmark=update_bookmark),
            throw=True)
        click.secho('Aggregations processed successfully.', fg='green')
    else:
        aggregate_events.delay(
            aggregation_types, start_date=start_date, end_date=end_date)
        click.secho('Aggregations processing task sent...', fg='yellow')


@aggregations.command('delete')
@aggr_arg
@click.option('--start-date', callback=_parse_date)
@click.option('--end-date', callback=_parse_date)
@click.confirmation_option(
    prompt='Are you sure you want to delete aggregations?')
@with_appcontext
def _aggregations_delete(aggregation_types=None,
                         start_date=None, end_date=None):
    """Delete computed aggregations."""
    aggregation_types = (aggregation_types or
                         list(current_stats.enabled_aggregations))
    for a in aggregation_types:
        aggregator = current_stats.aggregations[a].aggregator_class(
            **current_stats.aggregations[a].aggregator_config)
        aggregator.delete(start_date, end_date)


@aggregations.command('list-bookmarks')
@aggr_arg
@click.option('--start-date', callback=_parse_date)
@click.option('--end-date', callback=_parse_date)
@click.option('--limit', '-n', default=5)
@with_appcontext
def _aggregations_list_bookmarks(aggregation_types=None,
                                 start_date=None, end_date=None, limit=None):
    """List aggregation bookmarks."""
    aggregation_types = (aggregation_types or
                         list(current_stats.enabled_aggregations))
    for a in aggregation_types:
        aggregator = current_stats.aggregations[a].aggregator_class(
            **current_stats.aggregations[a].aggregator_config)
        bookmarks = aggregator.list_bookmarks(start_date, end_date, limit)
        click.echo('{}:'.format(a))
        for b in bookmarks:
            click.echo(' - {}'.format(b.date))
