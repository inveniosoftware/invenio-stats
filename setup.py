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

"""Invenio module for collecting statistics."""

import os

from setuptools import find_packages, setup

readme = open('README.rst').read()
history = open('CHANGES.rst').read()

tests_require = [
    'check-manifest>=0.25',
    'coverage>=4.0',
    'elasticsearch<5,>=2.0.0',
    'elasticsearch-dsl<5,>=2.0.0',
    'invenio-db>=1.0.0b5',
    'isort>=4.2.15',
    'mock>=1.0.0',
    'pydocstyle>=1.0.0',
    'pytest-cache>=1.0',
    'pytest-cov>=1.8.0',
    'pytest-pep8>=1.0.6',
    'pytest>=2.8.0',
    'python-dateutil>=2.6.0',
]

extras_require = {
    'docs': [
        'Sphinx>=1.4',
    ],
    'tests': tests_require,
}

extras_require['all'] = [
    'invenio-records-ui>=1.0.0a9',
]

for reqs in extras_require.values():
    extras_require['all'].extend(reqs)

setup_requires = [
    'pytest-runner>=2.6.2',
]

install_requires = [
    'arrow>=0.7.0',
    'Flask>=0.11',
    'invenio-files-rest>=1.0.0a16',
    'invenio-search>=1.0.0a10',
    'invenio-queues>=1.0.0a1',
    'python-geoip>=1.2',
    'maxminddb-geolite2>=2017.0404',
    # 'python-geoip-geolite2>=2015.0303',
    'robot-detection>=0.4',
]

packages = find_packages()


# Get the version string. Cannot be done with import!
g = {}
with open(os.path.join('invenio_stats', 'version.py'), 'rt') as fp:
    exec(fp.read(), g)
    version = g['__version__']

setup(
    name='invenio-stats',
    version=version,
    description=__doc__,
    long_description=readme + '\n\n' + history,
    keywords='invenio statistics',
    license='GPLv2',
    author='CERN',
    author_email='info@invenio-software.org',
    url='https://github.com/inveniosoftware/invenio-stats',
    packages=packages,
    zip_safe=False,
    include_package_data=True,
    platforms='any',
    entry_points={
        'invenio_base.apps': [
            'invenio_stats = invenio_stats:InvenioStats',
        ],
        'invenio_base.api_apps': [
            'invenio_stats = invenio_stats:InvenioStats',
        ],
        'invenio_celery.tasks': [
            'invenio_stats = invenio_stats.tasks',
        ],
        'invenio_base.api_blueprints': [
            'invenio_files_rest = invenio_stats.views:blueprint',
        ],
        'invenio_search.templates': [
            'invenio_stats = invenio_stats.templates:register_templates',
        ],
        'invenio_queues.queues': [
            'invenio_stats = invenio_stats.queues:declare_queues',
        ],
        'invenio_stats.events': [
            'invenio_stats = '
            'invenio_stats.contrib.registrations:register_events'
        ],
        'invenio_stats.aggregations': [
            'invenio_stats = '
            'invenio_stats.contrib.registrations:register_aggregations'
        ],
        'invenio_stats.queries': [
            'invenio_stats = '
            'invenio_stats.contrib.registrations:register_queries'
        ]
    },
    extras_require=extras_require,
    install_requires=install_requires,
    setup_requires=setup_requires,
    tests_require=tests_require,
    classifiers=[
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: GNU General Public License v2 (GPLv2)',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Development Status :: 1 - Planning',
    ],
)
