# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2017-2018 CERN.
# Copyright (C) 2025 Graz University of Technology.
#
# Invenio is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Aggregation tests."""

import datetime
from unittest.mock import patch

import pytest
from conftest import _create_file_download_event
from helpers import mock_date
from invenio_search import current_search
from invenio_search.engine import dsl

from invenio_stats import current_stats
from invenio_stats.aggregations import StatAggregator, filter_robots
from invenio_stats.processors import EventsIndexer
from invenio_stats.tasks import aggregate_events, process_events


def test_wrong_intervals(app, search_clear):
    """Test aggregation with interval > index_interval."""
    with pytest.raises(ValueError):
        StatAggregator(
            "test-agg", "test", search_clear, interval="month", index_interval="day"
        )


@pytest.mark.parametrize(
    "indexed_events",
    [
        {
            "file_number": 1,
            "event_number": 1,
            "robot_event_number": 0,
            "start_date": datetime.date(2017, 1, 1),
            "end_date": datetime.date(2017, 1, 7),
        }
    ],
    indirect=["indexed_events"],
)
def test_get_bookmark(app, search_clear, indexed_events):
    """Test bookmark reading."""
    stat_agg = StatAggregator(
        name="file-download-agg",
        client=search_clear,
        event="file-download",
        field="file_id",
        interval="day",
    )

    with patch("invenio_stats.aggregations.datetime", mock_date(2017, 1, 7, 11, 10, 9)):
        stat_agg.run()
    current_search.flush_and_refresh(index="*")
    assert stat_agg.bookmark_api.get_bookmark() == datetime.datetime(
        2017, 1, 7, 11, 9, 9, tzinfo=datetime.timezone.utc
    )  # Note that the bookmark is one minute older


def test_overwriting_aggregations(app, search_clear, mock_event_queue):
    """Check that the StatAggregator correctly starts from bookmark.

    1. Create sample file download event and process it.
    2. Run aggregator and write count, in aggregation index.
    3. Create new events and repeat procedure to assert that the
        results within the interval of the previous events
        overwrite the aggregation,
        by checking that the document version has increased.
    4. Run one more time, without any new events, and ensure that the
        aggregations have not been overwritten
    """
    # Send some events
    mock_event_queue.consume.return_value = [
        _create_file_download_event(date) for date in [(2017, 6, 1), (2017, 6, 2, 10)]
    ]
    # Note that the events use the current time. Let's mock that as well
    with patch("invenio_stats.processors.datetime", mock_date(2017, 6, 2, 11)):
        indexer = EventsIndexer(mock_event_queue)
        indexer.run()
    current_search.flush_and_refresh(index="*")

    # Aggregate events
    with patch("invenio_stats.aggregations.datetime", mock_date(2017, 6, 2, 12)):
        aggregate_events(["file-download-agg"])
    current_search.flush_and_refresh(index="*")

    # Send new events, some on the last aggregated day and some far
    # in the future.
    res = search_clear.search(index="stats-file-download", version=True)
    for hit in res["hits"]["hits"]:
        if "file_id" in hit["_source"].keys():
            assert hit["_version"] == 1

    mock_event_queue.consume.return_value = [
        _create_file_download_event(date)
        for date in [(2017, 6, 2, 15), (2017, 7, 1)]  # second event on the same date
    ]
    with patch("invenio_stats.processors.datetime", mock_date(2017, 7, 1, 5)):
        indexer = EventsIndexer(mock_event_queue)
        indexer.run()
    current_search.flush_and_refresh(index="*")
    # Aggregate again. The aggregation should start from the last bookmark.
    with patch("invenio_stats.aggregations.datetime", mock_date(2017, 7, 1, 6)):
        d = aggregate_events(["file-download-agg"])
    assert d == [
        [
            (1, 0),
            (0, 0),
            (0, 0),
            (0, 0),
            (0, 0),
            (0, 0),
            (0, 0),
            (0, 0),
            (0, 0),
            (0, 0),
            (0, 0),
            (0, 0),
            (0, 0),
            (0, 0),
            (0, 0),
            (0, 0),
            (0, 0),
            (0, 0),
            (0, 0),
            (0, 0),
            (0, 0),
            (0, 0),
            (0, 0),
            (0, 0),
            (0, 0),
            (0, 0),
            (0, 0),
            (0, 0),
            (0, 0),
            (1, 0),
        ]
    ]
    current_search.flush_and_refresh(index="*")
    res = search_clear.search(index="stats-file-download", version=True)
    for hit in res["hits"]["hits"]:
        if hit["_source"]["timestamp"].startswith("2017-06-02"):
            assert hit["_version"] == 2
            assert hit["_source"]["count"] == 2
        else:
            assert hit["_version"] == 1

    # Run one more time, one hour later, and ensure that the aggregation does not modify anything
    with patch("invenio_stats.aggregations.datetime", mock_date(2017, 7, 1, 7)):
        d = aggregate_events(["file-download-agg"])
    assert d == [[(0, 0)]]


def test_aggregation_without_events(app, search_clear):
    """Check that the aggregation doesn't crash if there are no events.

    This scenario happens when celery starts aggregating but no events
    have been created yet.
    """
    # Aggregate events
    with patch("invenio_stats.aggregations.datetime", mock_date(2017, 9, 9)):
        StatAggregator(
            name="file-download-agg",
            event="file-download",
            field="file_id",
            interval="day",
            query_modifiers=[],
        ).run()
    assert not dsl.Index("stats-file-download", using=search_clear).exists()
    # Create the index but without any event. This happens when the events
    # have been indexed but are not yet searchable (before index refresh).
    dsl.Index("events-stats-file-download-2017", using=search_clear).create()

    # Wait for the index to be available
    current_search.flush_and_refresh(index="*")

    # Aggregate events
    with patch("invenio_stats.aggregations.datetime", mock_date(2017, 9, 9)):
        StatAggregator(
            name="test-file-download",
            event="file-download",
            field="file_id",
            interval="day",
            query_modifiers=[],
        ).run()
    assert not dsl.Index("stats-file-download", using=search_clear).exists()


def test_bookmark_removal(app, search_clear, mock_event_queue):
    """Remove aggregation bookmark and restart aggregation.

    This simulates the scenario where aggregations have been created but the
    the bookmarks have not been set due to an error.
    """
    mock_event_queue.consume.return_value = [
        _create_file_download_event(date)
        for date in [(2017, 6, 2, 15), (2017, 7, 1)]  # second event on the same date
    ]
    indexer = EventsIndexer(mock_event_queue)
    indexer.run()
    current_search.flush_and_refresh(index="*")

    def aggregate_and_check_version(expected_version):
        with patch("invenio_stats.aggregations.datetime", mock_date(2017, 7, 10)):
            StatAggregator(
                field="file_id",
                interval="day",
                name="file-download-agg",
                event="file-download",
                query_modifiers=[],
            ).run()
        current_search.flush_and_refresh(index="*")
        res = search_clear.search(index="stats-file-download", version=True)
        for hit in res["hits"]["hits"]:
            assert hit["_version"] == expected_version

    aggregate_and_check_version(1)
    aggregate_and_check_version(1)

    # Delete all bookmarks
    search_clear.indices.delete(index="stats-bookmarks")
    current_search.flush_and_refresh(index="*")
    # the aggregations should have been overwritten
    aggregate_and_check_version(2)


@pytest.mark.parametrize(
    "indexed_events",
    [
        {
            "file_number": 5,
            "event_number": 1,
            "start_date": datetime.date(2015, 1, 28),
            "end_date": datetime.date(2015, 2, 3),
        }
    ],
    indirect=["indexed_events"],
)
def test_date_range(app, search_clear, event_queues, indexed_events):
    """Test date ranges."""
    with patch("invenio_stats.aggregations.datetime", mock_date(2015, 2, 4)):
        aggregate_events(["file-download-agg"])
    current_search.flush_and_refresh(index="*")
    query = dsl.Search(using=search_clear, index="stats-file-download")[0:30].sort(
        "file_id"
    )
    results = query.execute()

    total_count = 0
    for result in results:
        if "file_id" in result:
            total_count += result.count
    assert total_count == 30


@pytest.mark.parametrize(
    "indexed_events",
    [
        {
            "file_number": 1,
            "event_number": 2,
            "robot_event_number": 3,
            "start_date": datetime.date(2015, 1, 28),
            "end_date": datetime.date(2015, 1, 30),
        }
    ],
    indirect=["indexed_events"],
)
@pytest.mark.parametrize("with_robots", [(True), (False)])
def test_filter_robots(app, search_clear, event_queues, indexed_events, with_robots):
    """Test the filter_robots query modifier."""
    query_modifiers = []
    if not with_robots:
        query_modifiers = [filter_robots]

    with patch("invenio_stats.aggregations.datetime", mock_date(2015, 2, 1)):
        StatAggregator(
            name="file-download-agg",
            client=search_clear,
            event="file-download",
            field="file_id",
            interval="day",
            query_modifiers=query_modifiers,
        ).run()

    current_search.flush_and_refresh(index="*")
    query = dsl.Search(using=search_clear, index="stats-file-download")[0:30].sort(
        "file_id"
    )
    results = query.execute()
    assert len(results) == 3
    for result in results:
        if "file_id" in result:
            assert result.count == (5 if with_robots else 2)


def test_metric_aggregations(app, search_clear, event_queues):
    """Test aggregation metrics."""
    current_stats.publish(
        "file-download",
        [
            _create_file_download_event(date, user_id="1")
            for date in [
                (2018, 1, 1, 12, 10),
                (2018, 1, 1, 12, 20),
                (2018, 1, 1, 12, 30),
                (2018, 1, 1, 13, 10),
                (2018, 1, 1, 13, 20),
                (2018, 1, 1, 13, 30),
                (2018, 1, 1, 14, 10),
                (2018, 1, 1, 14, 20),
                (2018, 1, 1, 14, 30),
                (2018, 1, 1, 15, 10),
                (2018, 1, 1, 15, 20),
                (2018, 1, 1, 15, 30),
            ]
        ],
    )
    process_events(["file-download"])
    current_search.flush_and_refresh(index="*")

    stat_agg = StatAggregator(
        name="file-download-agg",
        client=search_clear,
        event="file-download",
        field="file_id",
        metric_fields={
            "unique_count": (
                "cardinality",
                "unique_session_id",
                {"precision_threshold": 3000},
            ),
            "volume": ("sum", "size", {}),
        },
        interval="day",
    )
    with patch("invenio_stats.aggregations.datetime", mock_date(2018, 1, 2)):
        stat_agg.run()
    current_search.flush_and_refresh(index="*")

    query = dsl.Search(using=search_clear, index="stats-file-download")
    results = query.execute()
    assert len(results) == 1
    assert results[0].count == 12  # 3 views over 4 differnet hour slices
    assert results[0].unique_count == 4  # 4 different hour slices accessed
    assert results[0].volume == 9000 * 12


@pytest.mark.parametrize(
    "indexed_events",
    [
        {
            "file_number": 1,
            "event_number": 1,
            "robot_event_number": 0,
            "start_date": datetime.date(2017, 1, 1),
            "end_date": datetime.date(2017, 1, 3),
        }
    ],
    indirect=["indexed_events"],
)
def test_batch_bookmark_advances_per_interval(app, search_clear, indexed_events):
    """Test that batch_bookmark writes one bookmark per processed interval."""
    aggregator = StatAggregator(
        name="file-download-agg",
        client=search_clear,
        event="file-download",
        field="file_id",
        interval="day",
        batch_bookmark=True,
    )
    with patch("invenio_stats.aggregations.datetime", mock_date(2017, 1, 4)):
        aggregator.run(
            start_date=datetime.datetime(2017, 1, 1, tzinfo=datetime.timezone.utc),
            end_date=datetime.datetime(
                2017, 1, 3, 23, 59, 59, tzinfo=datetime.timezone.utc
            ),
        )
    current_search.flush_and_refresh(index="*")

    bookmarks = list(aggregator.list_bookmarks())
    assert len(bookmarks) == 3
    dates = sorted(b.date for b in bookmarks)
    assert dates[0].startswith("2017-01-02")
    assert dates[1].startswith("2017-01-03")
    assert dates[2].startswith("2017-01-04")


@pytest.mark.parametrize(
    "indexed_events",
    [
        {
            "file_number": 1,
            "event_number": 1,
            "robot_event_number": 0,
            "start_date": datetime.date(2017, 1, 1),
            "end_date": datetime.date(2017, 1, 3),
        }
    ],
    indirect=["indexed_events"],
)
def test_batch_bookmark_resume_after_partial_run(app, search_clear, indexed_events):
    """Test that a failure mid-loop preserves bookmarks for completed intervals."""
    aggregator = StatAggregator(
        name="file-download-agg",
        client=search_clear,
        event="file-download",
        field="file_id",
        interval="day",
        batch_bookmark=True,
    )

    # Simulate a worker kill on the third interval
    real_agg_iter = aggregator.agg_iter
    call_count = {"n": 0}

    def fail_on_third_interval(dt, previous_bookmark):
        call_count["n"] += 1
        if call_count["n"] > 2:
            raise RuntimeError("simulated worker kill")
        yield from real_agg_iter(dt, previous_bookmark)

    aggregator.agg_iter = fail_on_third_interval

    with patch("invenio_stats.aggregations.datetime", mock_date(2017, 1, 4)):
        with pytest.raises(RuntimeError):
            aggregator.run(
                start_date=datetime.datetime(2017, 1, 1, tzinfo=datetime.timezone.utc),
                end_date=datetime.datetime(
                    2017, 1, 3, 23, 59, 59, tzinfo=datetime.timezone.utc
                ),
            )
    current_search.flush_and_refresh(index="*")

    # The first two intervals' bookmarks should be present
    bookmarks = list(aggregator.list_bookmarks())
    assert len(bookmarks) == 2
    assert max(b.date for b in bookmarks).startswith("2017-01-03")

    # Re-run from the saved bookmark and confirm all three days are aggregated
    aggregator.agg_iter = real_agg_iter
    with patch("invenio_stats.aggregations.datetime", mock_date(2017, 1, 4)):
        aggregator.run(
            end_date=datetime.datetime(
                2017, 1, 3, 23, 59, 59, tzinfo=datetime.timezone.utc
            ),
        )
    current_search.flush_and_refresh(index="*")

    results = (
        dsl.Search(using=search_clear, index="stats-file-download")
        .sort("timestamp")
        .execute()
    )
    timestamps = [r.timestamp[:10] for r in results if "file_id" in r]
    assert timestamps == ["2017-01-01", "2017-01-02", "2017-01-03"]


def test_incremental_skips_unchanged_records(app, search_clear, mock_event_queue):
    """Test that the affected-set lookup returns only files touched since the bookmark."""
    # Seed one event per file for five files
    mock_event_queue.consume.return_value = [
        _create_file_download_event(
            (2017, 1, 2, 8),
            file_id="F000000000000000000000000000000{}".format(i),
        )
        for i in range(1, 6)
    ]
    with patch("invenio_stats.processors.datetime", mock_date(2017, 1, 2, 8)):
        indexer = EventsIndexer(mock_event_queue)
        indexer.run()
    current_search.flush_and_refresh(index="*")

    bookmark = datetime.datetime(2017, 1, 2, 12, tzinfo=datetime.timezone.utc)

    # Add new events for files 1 and 2 only, after the bookmark
    mock_event_queue.consume.return_value = [
        _create_file_download_event(
            (2017, 1, 2, 14),
            file_id="F000000000000000000000000000000{}".format(i),
        )
        for i in (1, 2)
    ]
    with patch("invenio_stats.processors.datetime", mock_date(2017, 1, 2, 14)):
        indexer = EventsIndexer(mock_event_queue)
        indexer.run()
    current_search.flush_and_refresh(index="*")

    aggregator = StatAggregator(
        name="file-download-agg",
        client=search_clear,
        event="file-download",
        field="file_id",
        interval="day",
        incremental=True,
    )
    affected = aggregator._affected_field_values(
        datetime.datetime(2017, 1, 2, tzinfo=datetime.timezone.utc), bookmark
    )
    assert set(affected) == {
        "F0000000000000000000000000000001",
        "F0000000000000000000000000000002",
    }


def test_incremental_matches_full_results(app, search_clear, mock_event_queue):
    """Test that the incremental and full paths produce identical aggregation docs."""
    # Seed one event per file for five files
    mock_event_queue.consume.return_value = [
        _create_file_download_event(
            (2017, 1, 2, 8),
            file_id="F000000000000000000000000000000{}".format(i),
        )
        for i in range(1, 6)
    ]
    with patch("invenio_stats.processors.datetime", mock_date(2017, 1, 2, 8)):
        indexer = EventsIndexer(mock_event_queue)
        indexer.run()
    current_search.flush_and_refresh(index="*")

    # Initial aggregation run (full scan), establishes a bookmark
    aggregator = StatAggregator(
        name="file-download-agg",
        client=search_clear,
        event="file-download",
        field="file_id",
        interval="day",
    )
    with patch("invenio_stats.aggregations.datetime", mock_date(2017, 1, 2, 12)):
        aggregator.run()
    current_search.flush_and_refresh(index="*")

    # Add new events for files 1 and 2
    mock_event_queue.consume.return_value = [
        _create_file_download_event(
            (2017, 1, 2, 14),
            file_id="F000000000000000000000000000000{}".format(i),
        )
        for i in (1, 2)
    ]
    with patch("invenio_stats.processors.datetime", mock_date(2017, 1, 2, 14)):
        indexer = EventsIndexer(mock_event_queue)
        indexer.run()
    current_search.flush_and_refresh(index="*")

    # Incremental run on top of the existing aggregation
    incremental = StatAggregator(
        name="file-download-agg",
        client=search_clear,
        event="file-download",
        field="file_id",
        interval="day",
        incremental=True,
    )
    with patch("invenio_stats.aggregations.datetime", mock_date(2017, 1, 2, 18)):
        incremental.run()
    current_search.flush_and_refresh(index="*")

    incremental_docs = {
        h["_id"]: {k: v for k, v in h["_source"].items() if k != "updated_timestamp"}
        for h in search_clear.search(index="stats-file-download")["hits"]["hits"]
    }

    # Wipe aggregation docs and the bookmark, then re-run from scratch (full scan)
    aggregator.delete()
    current_search.flush_and_refresh(index="*")
    with patch("invenio_stats.aggregations.datetime", mock_date(2017, 1, 2, 18)):
        aggregator.run()
    current_search.flush_and_refresh(index="*")

    full_docs = {
        h["_id"]: {k: v for k, v in h["_source"].items() if k != "updated_timestamp"}
        for h in search_clear.search(index="stats-file-download")["hits"]["hits"]
    }
    assert incremental_docs == full_docs
    assert len(incremental_docs) == 5
    counts = {doc["file_id"]: doc["count"] for doc in incremental_docs.values()}
    assert counts["F0000000000000000000000000000001"] == 2
    assert counts["F0000000000000000000000000000002"] == 2
    for i in (3, 4, 5):
        assert counts["F000000000000000000000000000000{}".format(i)] == 1


def test_incremental_cold_start_falls_back(app, search_clear, mock_event_queue):
    """Test that incremental falls back to the full scan when there is no bookmark."""
    mock_event_queue.consume.return_value = [
        _create_file_download_event(
            (2017, 1, 2, 8),
            file_id="F000000000000000000000000000000{}".format(i),
        )
        for i in range(1, 6)
    ]
    with patch("invenio_stats.processors.datetime", mock_date(2017, 1, 2, 8)):
        indexer = EventsIndexer(mock_event_queue)
        indexer.run()
    current_search.flush_and_refresh(index="*")

    aggregator = StatAggregator(
        name="file-download-agg",
        client=search_clear,
        event="file-download",
        field="file_id",
        interval="day",
        incremental=True,
    )

    full_calls = []
    incremental_calls = []
    real_full = StatAggregator._agg_iter_full
    real_incremental = StatAggregator._agg_iter_incremental

    def track_full(self, dt, previous_bookmark):
        full_calls.append((dt, previous_bookmark))
        yield from real_full(self, dt, previous_bookmark)

    def track_incremental(self, dt, previous_bookmark):
        incremental_calls.append((dt, previous_bookmark))
        yield from real_incremental(self, dt, previous_bookmark)

    with patch.object(StatAggregator, "_agg_iter_full", track_full), patch.object(
        StatAggregator, "_agg_iter_incremental", track_incremental
    ), patch("invenio_stats.aggregations.datetime", mock_date(2017, 1, 2, 12)):
        aggregator.run()

    assert len(full_calls) >= 1
    assert incremental_calls == []


def test_incremental_partitions_large_affected_set(app, search_clear, mock_event_queue):
    """Test that affected_partition_size splits the affected set across queries."""
    mock_event_queue.consume.return_value = [
        _create_file_download_event(
            (2017, 1, 2, 8),
            file_id="F000000000000000000000000000000{}".format(i),
        )
        for i in range(1, 6)
    ]
    with patch("invenio_stats.processors.datetime", mock_date(2017, 1, 2, 8)):
        indexer = EventsIndexer(mock_event_queue)
        indexer.run()
    current_search.flush_and_refresh(index="*")

    # Initial run establishes the bookmark
    seed = StatAggregator(
        name="file-download-agg",
        client=search_clear,
        event="file-download",
        field="file_id",
        interval="day",
    )
    with patch("invenio_stats.aggregations.datetime", mock_date(2017, 1, 2, 12)):
        seed.run()
    current_search.flush_and_refresh(index="*")

    # Touch all five files after the bookmark
    mock_event_queue.consume.return_value = [
        _create_file_download_event(
            (2017, 1, 2, 14),
            file_id="F000000000000000000000000000000{}".format(i),
        )
        for i in range(1, 6)
    ]
    with patch("invenio_stats.processors.datetime", mock_date(2017, 1, 2, 14)):
        indexer = EventsIndexer(mock_event_queue)
        indexer.run()
    current_search.flush_and_refresh(index="*")

    aggregator = StatAggregator(
        name="file-download-agg",
        client=search_clear,
        event="file-download",
        field="file_id",
        interval="day",
        incremental=True,
        affected_partition_size=2,
    )

    chunks_seen = []
    real_chunk = StatAggregator._agg_iter_chunk

    def track_chunk(self, rounded_dt, chunk, previous_bookmark):
        chunks_seen.append(list(chunk))
        yield from real_chunk(self, rounded_dt, chunk, previous_bookmark)

    with patch.object(StatAggregator, "_agg_iter_chunk", track_chunk), patch(
        "invenio_stats.aggregations.datetime", mock_date(2017, 1, 2, 18)
    ):
        aggregator.run()

    assert len(chunks_seen) == 3
    assert sorted(len(c) for c in chunks_seen) == [1, 2, 2]
    assert sum(len(c) for c in chunks_seen) == 5
    expected_files = {
        "F000000000000000000000000000000{}".format(i) for i in range(1, 6)
    }
    assert set().union(*chunks_seen) == expected_files
