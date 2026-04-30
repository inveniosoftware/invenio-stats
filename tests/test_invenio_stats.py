# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2016-2026 CERN.
# Copyright (C)      2022 TU Wien.
#
# Invenio is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Invenio Stats extension tests."""

from flask import Flask
from invenio_queues import InvenioQueues
from invenio_queues.proxies import current_queues

from invenio_stats import InvenioStats
from invenio_stats.contrib.config import EVENTS_CONFIG
from invenio_stats.ext import finalize_app
from invenio_stats.proxies import current_stats


def test_version():
    """Test version import."""
    from invenio_stats import __version__

    assert __version__


def test_init():
    """Test extension initialization."""
    app = Flask("testapp")
    ext = InvenioStats(app)
    assert "invenio-stats" in app.extensions

    app = Flask("testapp")
    ext = InvenioStats()
    assert "invenio-stats" not in app.extensions
    ext.init_app(app)
    assert "invenio-stats" in app.extensions


def test_extension_get_query_cache(app, queries_config):
    """Test if the query object cache works properly."""
    query1 = current_stats.get_query("test-query")
    query2 = current_stats.get_query("test-query")

    assert query1 is query2


def test_finalize_app_warms_event_state():
    """Test that the finalizer warms event caches."""
    app = Flask("testapp")
    app.config["STATS_EVENTS"] = {"file-download": EVENTS_CONFIG["file-download"]}
    InvenioStats(app)
    InvenioQueues(app)

    with app.app_context():
        finalize_app(app)

        stats_state = app.extensions["invenio-stats"]
        assert "events" in stats_state.__dict__
        assert "aggregations" not in stats_state.__dict__
        assert "queries" not in stats_state.__dict__
        assert "permission_factory" not in stats_state.__dict__
        assert "stats-file-download" in current_queues.queues
        assert current_stats.events["file-download"].queue == (
            current_queues.queues["stats-file-download"]
        )
