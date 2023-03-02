# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2016-2018 CERN.
# Copyright (C)      2022 TU Wien.
#
# Invenio is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Invenio Stats extension tests."""

from flask import Flask

from invenio_stats import InvenioStats
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
