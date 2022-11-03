# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2016-2018 CERN.
#
# Invenio is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Invenio module for collecting statistics."""

import os

from setuptools import find_packages, setup

readme = open('README.rst').read()
history = open('CHANGES.rst').read()

tests_require = [
    "invenio-accounts>=2.0.0",
    "invenio-db>=1.0.14",
    "invenio-files-rest>=1.3.0",
    "invenio-oauth2server>=1.3.0",
    "invenio-records>=2.0.0",
    "invenio-records-ui>=1.2.0",
    "pytest-invenio>=2.1.0"
]

invenio_search_version = '2.1.0'

extras_require = {
    'docs': [
        'Sphinx>=5,<6',
    ],
    'opensearch1': [
        'invenio-search[opensearch1]>={}'.format(invenio_search_version)
    ],
    'opensearch2': [
        'invenio-search[opensearch2]>={}'.format(invenio_search_version)
    ],
    'elasticsearch7': [
        'invenio-search[elasticsearch7]>={}'.format(invenio_search_version)
    ],
    'tests': tests_require,
}

extras_require['all'] = []
for name, reqs in extras_require.items():
    if name[0] == ':' or name in (
        'elasticsearch7'
        'opensearch1'
        'opensearch2'
    ):
        continue
    extras_require['all'].extend(reqs)

setup_requires = [
    'pytest-runner>=6.0.0',
]

install_requires = [
    'counter-robots>=2018.6',
    'invenio-base>=1.2.13',
    'invenio-cache>=1.1.0',
    'invenio-celery>=1.2.5',
    'invenio-queues>=1.0.0a2',
    'maxminddb-geolite2>=2018.703',
    'python-dateutil>=2.7.0',
    'python-geoip>=1.2',
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
    license='MIT',
    author='CERN',
    author_email='info@invenio-software.org',
    url='https://github.com/inveniosoftware/invenio-stats',
    packages=packages,
    zip_safe=False,
    include_package_data=True,
    platforms='any',
    entry_points={
        'flask.commands': [
            'stats = invenio_stats.cli:stats',
        ],
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
            'invenio_stats = invenio_stats.views:blueprint',
        ],
        'invenio_search.templates': [
            'invenio_stats = invenio_stats.templates:register_templates',
        ],
        'invenio_queues.queues': [
            'invenio_stats = invenio_stats.queues:declare_queues',
        ]
    },
    extras_require=extras_require,
    install_requires=install_requires,
    setup_requires=setup_requires,
    tests_require=tests_require,
    classifiers=[
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Development Status :: 3 - Alpha',
    ],
)
