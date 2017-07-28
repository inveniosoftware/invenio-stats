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

"""Test utility functions."""

from flask import request
from mock import patch

from invenio_stats.utils import get_user


def test_get_user(app, mock_users, request_headers):
    """Test the get_user function."""
    header = request_headers['user']
    with patch(
                'invenio_stats.utils.current_user',
                mock_users['authenticated']
            ), app.test_request_context(
                headers=header, environ_base={'REMOTE_ADDR': '142.0.0.1'}
            ):
        user = get_user()
    assert user['user_id'] == mock_users['authenticated'].get_id()
    assert user['user_agent'] == header['USER_AGENT']
    assert user['ip_address'] == '142.0.0.1'
