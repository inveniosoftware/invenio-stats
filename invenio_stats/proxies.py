# SPDX-FileCopyrightText: 2016-2018 CERN.
# SPDX-FileCopyrightText: 2022 TU Wien.
# SPDX-License-Identifier: MIT

"""Proxy to the current stats module."""

from flask import current_app
from werkzeug.local import LocalProxy

current_stats = LocalProxy(lambda: current_app.extensions["invenio-stats"])
