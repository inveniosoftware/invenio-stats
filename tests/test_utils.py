# SPDX-FileCopyrightText: 2017-2026 CERN.
# SPDX-License-Identifier: MIT

"""Test utility functions."""

from unittest.mock import patch

import pytest

from invenio_stats.utils import get_geoip, get_user


def myfunc():
    """Example function."""
    pass


def test_get_user(app, mock_users, request_headers):
    """Test the get_user function."""
    header = request_headers["user"]
    with (
        patch("invenio_stats.utils.current_user", mock_users["authenticated"]),
        app.test_request_context(
            headers=header, environ_base={"REMOTE_ADDR": "142.0.0.1"}
        ),
    ):
        user = get_user()
    assert user["user_id"] == mock_users["authenticated"].get_id()
    assert user["user_agent"] == header["USER_AGENT"]
    assert user["ip_address"] == "142.0.0.1"
