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
    "invenio-accounts>=1.3",
    "invenio-db>=1.0.2",
    "invenio-files-rest>=1.0.0a23",
    "invenio-oauth2server>=1.0.1",
    "invenio-records>=1.0.0",
    "invenio-records-ui>=1.0.1",
    "pytest-invenio>=1.4.2",
    # due to https://github.com/PyCQA/pydocstyle/issues/620
    "pydocstyle==6.1.1"
]

invenio_search_version = '1.2.3'

extras_require = {
    'docs': [
        'Sphinx>=4.5,<5',
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
    ):
        continue
    extras_require['all'].extend(reqs)

setup_requires = [
    'pytest-runner>=2.6.2',
]

install_requires = [
    'counter-robots>=2018.6',
    'invenio-base>=1.2.2',
    'invenio-cache>=1.0.0',
    'invenio-celery>=1.1.3',
    'invenio-queues>=1.0.0a2',
    'maxminddb-geolite2>=2017.0404',
    'python-dateutil>=2.6.1',
    'python-geoip>=1.2',
    'jsonref>=0.3.0,<1.0.0',
    'jsonresolver>=0.3.0,<0.3.2',
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
