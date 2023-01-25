# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2016-2018 CERN.
# Copyright (C)      2022 TU Wien.
#
# Invenio is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Proxy to the current stats module."""

from flask import current_app
from werkzeug.local import LocalProxy

current_stats = LocalProxy(lambda: current_app.extensions["invenio-stats"])
