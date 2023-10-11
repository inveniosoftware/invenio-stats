# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2018 CERN.
# Copyright (C) 2022 TU Wien.
#
# Invenio is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Aggregation classes."""

from functools import wraps

import click
from dateutil.parser import parse as dateutil_parse
from datetime import datetime
from flask import current_app
from flask.cli import with_appcontext
from invenio_search.engine import search
from invenio_search.proxies import current_search_client
from werkzeug.local import LocalProxy

from .proxies import current_stats
from .tasks import aggregate_events, process_events


def lazy_result(f):
    """Decorate function to return LazyProxy."""

    @wraps(f)
    def decorated(ctx, param, value):
        return LocalProxy(lambda: f(ctx, param, value))

    return decorated


def _parse_date(ctx, param, value):
    if value:
        return dateutil_parse(value)


@lazy_result
def _validate_event_type(ctx, param, value):
    invalid_values = set(value) - set(current_stats.events)
    if invalid_values:
        raise click.BadParameter(
            "Invalid event type(s): {}. Valid values: {}".format(
                ", ".join(invalid_values), ", ".join(current_stats.events)
            )
        )
    return value


@lazy_result
def _validate_aggregation_type(ctx, param, value):
    invalid_values = set(value) - set(current_stats.aggregations)
    if invalid_values:
        raise click.BadParameter(
            "Invalid aggregation type(s): {}. Valid values: {}".format(
                ", ".join(invalid_values), ", ".join(current_stats.aggregations)
            )
        )
    return value


aggr_arg = click.argument(
    "aggregation-types", nargs=-1, callback=_validate_aggregation_type
)


@click.group()
def stats():
    """Statistics commands."""


@stats.group()
def events():
    """Event management commands."""


@events.command("process")
@click.argument("event-types", nargs=-1, callback=_validate_event_type)
@click.option("--eager", "-e", is_flag=True)
@with_appcontext
def _events_process(event_types=None, eager=False):
    """Process stats events."""
    # NOTE: event_types is a LocalProxy so it needs to be casted to be passed
    # to celery
    event_types = list(event_types or current_stats.events)
    process_task = process_events.si(event_types)

    if eager:
        process_task.apply(throw=True)
        click.secho("Events processed successfully.", fg="green")
    else:
        process_task.delay()
        click.secho("Events processing task sent...", fg="yellow")


@stats.group()
def aggregations():
    """Aggregation management commands."""


@aggregations.command("process")
@aggr_arg
@click.option("--start-date", callback=_parse_date)
@click.option("--end-date", callback=_parse_date)
@click.option("--update-bookmark", "-b", is_flag=True)
@click.option("--eager", "-e", is_flag=True)
@with_appcontext
def _aggregations_process(
    aggregation_types=None,
    start_date=None,
    end_date=None,
    update_bookmark=False,
    eager=False,
):
    """Process stats aggregations."""
    # NOTE: aggregation_types is a LocalProxy so it needs to be casted to be
    # passed to celery
    aggregation_types = list(aggregation_types or current_stats.aggregations)
    agg_task = aggregate_events.si(
        aggregation_types,
        start_date=start_date.isoformat() if start_date else None,
        end_date=end_date.isoformat() if end_date else None,
        update_bookmark=update_bookmark,
    )

    if eager:
        agg_task.apply(throw=True)
        click.secho("Aggregations processed successfully.", fg="green")
    else:
        agg_task.delay()
        click.secho("Aggregations processing task sent...", fg="yellow")


@aggregations.command("delete")
@aggr_arg
@click.option("--start-date", callback=_parse_date)
@click.option("--end-date", callback=_parse_date)
@click.confirmation_option(prompt="Are you sure you want to delete aggregations?")
@with_appcontext
def _aggregations_delete(aggregation_types=None, start_date=None, end_date=None):
    """Delete computed aggregations."""
    aggregation_types = aggregation_types or current_stats.aggregations
    for a in aggregation_types:
        aggr_cfg = current_stats.aggregations[a]
        aggregator = aggr_cfg.cls(name=aggr_cfg.name, **aggr_cfg.params)
        aggregator.delete(start_date, end_date)


@aggregations.command("list-bookmarks")
@aggr_arg
@click.option("--start-date", callback=_parse_date)
@click.option("--end-date", callback=_parse_date)
@click.option("--limit", "-n", default=5)
@with_appcontext
def _aggregations_list_bookmarks(
    aggregation_types=None, start_date=None, end_date=None, limit=None
):
    """List aggregation bookmarks."""
    aggregation_types = aggregation_types or current_stats.aggregations
    for a in aggregation_types:
        aggr_cfg = current_stats.aggregations[a]
        aggregator = aggr_cfg.cls(name=aggr_cfg.name, **aggr_cfg.params)
        bookmarks = aggregator.list_bookmarks(start_date, end_date, limit)
        click.echo("{}:".format(a))
        for b in bookmarks:
            click.echo(" - {}".format(b.date))


@stats.command("migrate_zenodo")
@with_appcontext
def _migrate():
    """Migrate the statistics from zenodo."""
    print("Checking if there are any `legacy` indices")
    my_date = datetime.utcnow().isoformat()
    painless = f'ctx._source.parent_recid=ctx._source.conceptrecid;ctx._source.updated_timestamp="{my_date}";'
    # Removing obsolete fields
    for f in [
        "conceptdoi",
        "resource_type",
        "access_right",
        "referrer",
        "size",
        "conceptrecid",
        "doi",
        "is_parent",
        "owners",
        "communities",
    ]:
        painless += f'ctx._source.remove("{f}");'
    legacy_indices = current_search_client.cat.indices("legacy*", format="json")
    i = 0
    total = len(legacy_indices)
    for my_index in sorted(legacy_indices, key=lambda d: d["index"]):
        if my_index["index"] == "legacy-zenodo-stats-bookmarks":
            continue
        print("%i/%i Doing index: %s" % (i, total, my_index["index"]))
        i += 1
        target = my_index["index"].replace("legacy-zenodo", "zenodo-prod")
        source = my_index["index"]
        try:
            old = current_search_client.count({}, source)
            new = current_search_client.count({}, target)
            if old == new:
                print("\tThe target has the same number of entries. Skipping")
                continue
        except search.exceptions.NotFoundError:
            pass
        try:
            print(
                {
                    "conflicts": "proceed",
                    "source": {
                        "index": my_index["index"],
                        "query": {
                            "bool": {"must_not": [{"term": {"is_parent": True}}]}
                        },
                    },
                    "dest": {"index": target, "op_type": "create"},
                    "script": {
                        "lang": "painless",
                        "source": painless,
                    },
                }
            )
            current_search_client.reindex(
                {
                    "conflicts": "proceed",
                    "source": {
                        "index": my_index["index"],
                        "query": {
                            "bool": {"must_not": [{"term": {"is_parent": True}}]}
                        },
                    },
                    "dest": {"index": target, "op_type": "create"},
                    "script": {
                        "lang": "painless",
                        "source": painless,
                    },
                }
            )
        except Exception as d:
            print("NOPE")
            print(d)
