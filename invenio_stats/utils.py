# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2016 CERN.
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

"""Utilities for Invenio-Stats."""

from __future__ import absolute_import, print_function

import hashlib

from flask import request
from flask_login import current_user
from geolite2 import geolite2


def get_geoip(ip):
    """Lookup country for IP address."""
    reader = geolite2.reader()
    ip_data = reader.get(ip)
    if ip_data is not None:
        return dict(country=ip_data.country)
    return {}


def anonimize_user(doc):
    """Process user information."""
    ip = doc.pop('ip_address', None)
    if ip:
        doc.update(get_geoip(ip))

    uid = doc.pop('user_id', '')
    ua = doc.pop('user_agent', '')

    m = hashlib.sha224()
    # TODO: include random salt here, that changes once a day.
    # m.update(random_salt)
    if uid:
        m.update(uid.encode('utf-8'))
    elif ua:
        m.update(ua)
    else:
        # TODO: add random data?
        pass

    doc.update(dict(
        visitor_id=m.hexdigest()
    ))

    return doc


def get_user():
    """User information.

    .. note::

       **Privacy note** A users IP address, user agent string, and user id
       (if logged in) is sent to a message queue, where it is stored for about
       5 minutes. The information is used to:

       - Detect robot visits from the user agent string.
       - Generate an anonymized visitor id (using a random salt per day).
       - Detect the users host contry based on the IP address.

       The information is then discarded.
    """
    # TODO: Take proxy into account
    return dict(
        ip_address=request.remote_addr,
        user_agent=request.user_agent.string,
        user_id=(
            current_user.get_id() if current_user.is_authenticated else None
        ),
    )
