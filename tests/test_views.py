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

"""Test view functions."""
import json

from flask import url_for


def test_post_request(app, db, query_entrypoints,
                      users, custom_permission_factory,
                      sample_histogram_query_data):
    """Test post request to stats API."""
    with app.test_client() as client:
        headers = [('Content-Type', 'application/json'),
                   ('Accept', 'application/json')]
        sample_histogram_query_data['mystat']['stat'] = 'test-query'
        resp = client.post(
            url_for('invenio_stats.stat_query',
                    access_token=users['authorized'].allowed_token),
            headers=headers,
            data=json.dumps(sample_histogram_query_data))
        resp_json = json.loads(resp.data.decode('utf-8'))
        assert resp_json['mystat']['value'] == 100

        sample_histogram_query_data['mystat']['stat'] = 'unknown-query'
        resp = client.post(
            url_for('invenio_stats.stat_query',
                    access_token=users['authorized'].allowed_token),
            headers=headers,
            data=json.dumps(sample_histogram_query_data))
        assert resp.status_code == 400


def test_unauthorized_request(app, query_entrypoints,
                              custom_permission_factory,
                              sample_histogram_query_data, users):
    """Test rejecting unauthorized requests."""

    def client_req(user):
        with app.test_client() as client:
            headers = [('Content-Type', 'application/json'),
                       ('Accept', 'application/json')]
            resp = client.post(
                url_for('invenio_stats.stat_query',
                        access_token=user.allowed_token if user else None),
                headers=headers,
                data=json.dumps(sample_histogram_query_data))
            print(resp.data)
            return resp.status_code

    sample_histogram_query_data['mystat']['stat'] = 'test-query'
    assert client_req(users['unauthorized']) == 403
    assert client_req(None) == 401
    assert client_req(users['authorized']) == 200

    assert custom_permission_factory.query_name == 'test-query'
    assert custom_permission_factory.params == \
        sample_histogram_query_data['mystat']['params']
