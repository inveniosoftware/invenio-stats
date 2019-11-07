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
    'check-manifest>=0.35',
    'coverage>=4.0',
    'invenio-accounts>=1.0.1',
    'invenio-db>=1.0.2',
    'invenio-files-rest>=1.0.0a23',
    'invenio-oauth2server>=1.0.1',
    'invenio-records>=1.0.0',
    'invenio-records-ui>=1.0.1',
    'isort>=4.2.15',
    'mock>=1.3.0',
    'pydocstyle>=1.0.0',
    'pytest-cov>=1.8.0',
    'pytest-pep8>=1.0.6',
    'pytest>=3.8.1,<4',
]

invenio_search_version = '1.2.3'

extras_require = {
    'docs': [
        'Sphinx>=1.4',
    ],
    'elasticsearch2': [
        'invenio-search[elasticsearch2]>={}'.format(invenio_search_version)
    ],
    'elasticsearch5': [
        'invenio-search[elasticsearch5]>={}'.format(invenio_search_version)
    ],
    'elasticsearch6': [
        'invenio-search[elasticsearch6]>={}'.format(invenio_search_version)
    ],
    'elasticsearch7': [
        'invenio-search[elasticsearch7]>={}'.format(invenio_search_version)
    ],
    'tests': tests_require,
}

extras_require['all'] = []
for name, reqs in extras_require.items():
    if name[0] == ':' or name in (
        'elasticsearch2',
        'elasticsearch5',
        'elasticsearch6',
        'elasticsearch7'
    ):
        continue
    extras_require['all'].extend(reqs)

setup_requires = [
    'pytest-runner>=2.6.2',
]

install_requires = [
    'counter-robots>=2018.6',
    'Flask>=0.11.1',
    'invenio-cache>=1.0.0',
    'invenio-queues>=1.0.0a1',
    'maxminddb-geolite2>=2017.0404',
    'python-dateutil>=2.6.1',
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
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Development Status :: 3 - Alpha',
    ],
)
