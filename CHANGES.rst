..
    SPDX-FileCopyrightText: 2017-2025 CERN.
    SPDX-FileCopyrightText: 2024-2026 Graz University of Technology.
    SPDX-License-Identifier: MIT


Changes
=======

Unreleased

- feat: the event preprocessors go through a single counter-robots classifier,
  exposed as the cached ``current_stats.visitor_classifier`` property and built from
  ``STATS_VISITOR_CLASSIFIER`` (an import path or ``app -> Classifier``; the default
  is the COUNTER baseline plus the extended preset). ``flag_robots`` /
  ``flag_machines`` keep setting ``is_robot`` / ``is_machine`` as before.
- feat: ``exclude_datacenter_browser`` preprocessor drops events whose user agent
  looks like a browser but whose IP resolves to a datacenter/hosting ASN, catching
  automation faking a browser from cloud infrastructure. It writes nothing to the
  event (no ``is_datacenter`` field) and only excludes, returning ``None`` or the
  document unchanged. It must run before ``anonymize_user``. Datacenter resolution
  is enabled by pointing ``STATS_VISITOR_ASN_DB`` at a GeoLite2-ASN mmdb (the
  ``counter-robots[asn]`` extra). invenio-stats holds no lists. Requires
  counter-robots>=2026.6.

Version v6.1.3 (released 2026-04-30)

- fix(stats): warm event cache on finalization

Version v6.1.2 (released 2026-03-04)

- fix(aggregations): make queries backwards-compatible with non timezone aware indices

Version v6.1.1 (released 2026-03-03)

- fix(queries): make queries backwards-compatible with non timezone aware indices

Version v6.1.0 (released 2026-01-29)

- feat(config): add STATS_EVENTS_UTC_DATETIME_ENABLED flag
  Introduce STATS_EVENTS_UTC_DATETIME_ENABLED (default: False) to strip
  tzinfo from event timestamps at build time. Set to True to opt-in to
  timezone-aware UTC datetimes.

Version v6.0.0 (released 2026-01-29)

- chore(setup): bump dependencies
- chore(black): update formatting to >= 26.0
- fix(chore): DeprecationWarning stdlib
- fix: DeprecationWarning warn use warning
- tests: extend support to Python 3.14
- i18n:push translations

Version 5.1.1 (release 2025-06-09)

- tests: fix issues with CI
- translations: add untranslated strings and add translation workflow

Version 5.1.0 (release 2025-01-20)

- aggregations: add yearly interval

Version 5.0.0 (release 2024-12-10)

- tests: remove dependency to invenio-oauth2server
- setup: bump major dependencies

Version 4.2.1 (release 2024-11-30)

- setup: change to reusable workflows
- setup: pin dependencies

Version v4.2.0 (released 2024-08-27)

- processors: allow filtering out robots/machines

Version 4.1.0 (release 2024-08-14)
----------------------------------

- introduce a new config `STATS_REGISTER_INDEX_TEMPLATES` to be able to register
  events and aggregations as index templates (ensure backwards compatibility)


Version 4.0.2 (release 2024-03-04)
----------------------------------

- aggregations: consider updated_timestamp field optional (ensure backwards compatibility)

Version 4.0.1 (release 2023-10-09)
----------------------------------

- aggregations: ensure events are aggregated only once

Version 4.0.0 (release 2023-10-03)
----------------------------------

- introduce new field `updated_timestamp`` in the events and stats templates
  and mappings
- improved calculation of aggregations skipping already aggregated events
- changed `refresh_interval` from 1m to 5s
- changed default events index name from daily to monthly
- moved BookmarkAPI to a new module

Version 3.1.0 (release 2023-04-20)
----------------------------------

- add extension method for building and caching queries

Version 3.0.0 (release 2023-03-01)
-------------------------------------

- Upgrade to ``invenio-search`` 2.x
- Drop support for Elasticsearch 2, 5, and 6
- Add support for OpenSearch 1 and 2
- Drop support for Python 2.7 and 3.6
- Remove function ``invenio_stats.utils:get_doctype``
- Fix ``validate_arguments`` for query classes
- Add ``build_event_emitter`` function for creating an ``EventEmitter`` but not registering it as a signal handler
- Add ``ext.get_event_emitter(name)``` function for caching built ``EventEmitter`` objects per name
- Replace elasticsearch-specific terminology

Version 2.0.0 (release 2023-02-23)
-------------------------------------

- add opensearch2 compatibility

Version 1.0.0a18 (release 2020-09-01)
-------------------------------------

- Fix isort arguments
- Filter pytest deprecation warnings
- Set default values for metrics instead of None, when no index found

Version 1.0.0a17 (release 2020-03-19)
-------------------------------------

- Removes Python 2.7 support.
- Centralizes Flask dependency via ``invenio-base``.

Version 1.0.0a16 (release 2020-02-24)
-------------------------------------

- bump celery dependency
- pin Werkzeug version

Version 1.0.0a15 (release 2019-11-27)
-------------------------------------

- Pin celery dependency

Version 1.0.0a14 (release 2019-11-27)
-------------------------------------

- Fix `get_bucket_size` method

Version 1.0.0a13 (release 2019-11-08)
-------------------------------------

- Bump invenio-queues

Version 1.0.0a12 (release 2019-11-08)
-------------------------------------

- Fixes templates for ElasticSearch 7
- Updates dependency of invenio-search

Version 1.0.0a11 (release 2019-10-02)
-------------------------------------

- Initial public release.
