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

"""Signal receivers for certain events."""

from __future__ import absolute_import, print_function

from datetime import datetime

from .proxies import current_stats
from .utils import get_user


def filedownload_receiver(sender_app, obj=None, **kwargs):
    """Log that a file was downloaded."""
    current_stats.publish('file_download', dict(
        # When:
        timestamp=datetime.utcnow().isoformat(),
        # What:
        bucket=str(obj.bucket_id),
        filename=obj.key,
        # Who:
        **get_user()
    ))


def recordview_receiver(sender_app, pid=None, record=None, **kwargs):
    """Log that a record was viewed."""
    current_stats.publish('record_view', dict(
        # When:
        timestamp=datetime.utcnow().isoformat(),
        # What:
        id=record.id,
        pid_type=pid.pid_type,
        pid_value=pid.pid_value,
        labels=record.get('communities', []),
        # Who:
        **get_user()
    ))
