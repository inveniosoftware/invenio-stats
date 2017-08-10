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

"""Signal receivers for certain events."""

from __future__ import absolute_import, print_function

import datetime

from ..proxies import current_stats
from ..utils import get_user, load_or_import_from_config


def file_download_event_builder(event, sender_app, obj=None, **kwargs):
    """Build a file-download event."""
    event.update(dict(
        # When:
        timestamp=datetime.datetime.utcnow().isoformat(),
        # What:
        bucket_id=str(obj.bucket_id),
        file_id=str(obj.file_id),
        file_key=obj.key,
        # Who:
        **get_user()
    ))
    return event


def build_file_unique_id(doc):
    """Build file unique identifier."""
    doc['unique_id'] = '{0}_{1}'.format(doc['bucket_id'], doc['file_id'])
    return doc


def record_view_event_builder(event, sender_app, pid=None, record=None,
                              **kwargs):
    """Build a record-view event."""
    event.update(dict(
        # When:
        timestamp=datetime.datetime.utcnow().isoformat(),
        # What:
        record_id=str(record.id),
        pid_type=pid.pid_type,
        pid_value=str(pid.pid_value),
        # Who:
        **get_user()
    ))
    return event
