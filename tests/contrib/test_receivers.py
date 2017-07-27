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

"""Signal receivers tests."""

from invenio_files_rest.signals import file_downloaded

from invenio_stats.tasks import aggregate_events, process_events

# def test_file_download_receiver(app, mock_user_ctx, sequential_ids, objects):
#     """Test the file-download event emitter and signal receiver."""
#     for j in range(len(objects)):
#         file_obj = objects[0]
#         file_obj.bucket_id = sequential_ids[0]
#         with app.test_request_context(
#             headers={'USER_AGENT':
#                      'Mozilla/5.0 (Windows NT 6.1; WOW64) '
#                      'AppleWebKit/537.36 (KHTML, like Gecko)'
#                      'Chrome/45.0.2454.101 Safari/537.36'}):
#             file_downloaded.send(app, obj=file_obj)
#             process_events(['file-download'])
#             # FIXME: no need to process the events. We should instead thest
#             # that the events are sent to the queue, i.e. consume them.
