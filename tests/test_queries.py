# SPDX-FileCopyrightText: 2017-2018 CERN.
# SPDX-License-Identifier: MIT

"""Query tests."""

import datetime

import pytest

from invenio_stats.queries import DateHistogramQuery, TermsQuery
from invenio_stats.utils import format_datetime_iso


@pytest.mark.parametrize(
    "aggregated_events",
    [
        {
            "file_number": 1,
            "event_number": 2,
            "start_date": datetime.date(2017, 1, 1),
            "end_date": datetime.date(2017, 1, 7),
        }
    ],
    indirect=["aggregated_events"],
)
def test_histogram_query(app, event_queues, aggregated_events, queries_config):
    """Test histogram query daily results."""
    # reading the configuration as it is registered from registrations.py
    histo_query = DateHistogramQuery(
        name="test_histo", **queries_config["bucket-file-download-histogram"]["params"]
    )
    results = histo_query.run(
        bucket_id="B0000000000000000000000000000001",
        file_key="test.pdf",
        start_date=datetime.datetime(2017, 1, 1),
        end_date=datetime.datetime(2017, 1, 7),
    )
    for day_result in results["buckets"]:
        assert int(day_result["value"]) == 2


@pytest.mark.parametrize(
    "aggregated_events",
    [
        {
            "file_number": 1,
            "event_number": 7,
            "start_date": datetime.date(2017, 1, 1),
            "end_date": datetime.date(2017, 1, 7),
        }
    ],
    indirect=["aggregated_events"],
)
def test_terms_query(app, event_queues, aggregated_events, queries_config):
    """Test that the terms query returns the correct total count."""
    terms_query = TermsQuery(
        name="test_total_count",
        **queries_config["bucket-file-download-total"]["params"]
    )
    results = terms_query.run(
        bucket_id="B0000000000000000000000000000001",
        start_date=datetime.datetime(2017, 1, 1),
        end_date=datetime.datetime(2017, 1, 7),
    )
    assert int(results["buckets"][0]["value"]) == 49


def test_query_date_formatting_config_disabled(app, queries_config):
    """Test date formatting in build_query when STATS_EVENTS_UTC_DATETIME_ENABLED is False.

    When the config is False (default), dates in range filters should be formatted
    without microseconds and timezone info for compatibility with strict_date_hour_minute_second.
    """
    # Ensure the config is False (default)
    app.config["STATS_EVENTS_UTC_DATETIME_ENABLED"] = False

    # Create datetimes with microseconds and timezone
    start_date = datetime.datetime(
        2024, 1, 15, 10, 30, 45, 123456, tzinfo=datetime.timezone.utc
    )
    end_date = datetime.datetime(
        2024, 1, 20, 18, 45, 30, 654321, tzinfo=datetime.timezone.utc
    )

    # Test format_datetime_iso directly
    formatted_start = format_datetime_iso(start_date)
    formatted_end = format_datetime_iso(end_date)

    # Should strip both microseconds and timezone
    assert formatted_start == "2024-01-15T10:30:45.123456"
    assert formatted_end == "2024-01-20T18:45:30.654321"

    # Verify build_query uses the same formatting
    query = DateHistogramQuery(
        name="test_date_format",
        **queries_config["bucket-file-download-histogram"]["params"]
    )

    agg_query = query.build_query(
        interval="day", start_date=start_date, end_date=end_date
    )

    # Convert to dict to inspect the query structure
    query_dict = agg_query.to_dict()

    # Extract the range filter from the query
    range_filter = None
    for filter_item in query_dict.get("query", {}).get("bool", {}).get("filter", []):
        if "range" in filter_item:
            range_filter = filter_item["range"]["timestamp"]
            break

    # Verify the range filter matches the formatted dates
    assert range_filter is not None, "Range filter should exist in query"
    assert range_filter["gte"] == formatted_start
    assert range_filter["lte"] == formatted_end


def test_query_date_formatting_config_enabled(app, queries_config):
    """Test date formatting in build_query when STATS_EVENTS_UTC_DATETIME_ENABLED is True.

    When the config is True, dates in range filters should preserve microseconds
    and timezone info for use with flexible date formats.
    """
    # Enable the config
    app.config["STATS_EVENTS_UTC_DATETIME_ENABLED"] = True

    # Create datetimes with microseconds and timezone
    start_date = datetime.datetime(
        2024, 1, 15, 10, 30, 45, 123456, tzinfo=datetime.timezone.utc
    )
    end_date = datetime.datetime(
        2024, 1, 20, 18, 45, 30, 654321, tzinfo=datetime.timezone.utc
    )

    # Test format_datetime_iso directly
    formatted_start = format_datetime_iso(start_date)
    formatted_end = format_datetime_iso(end_date)

    # Should keep both microseconds and timezone
    assert formatted_start == "2024-01-15T10:30:45.123456+00:00"
    assert formatted_end == "2024-01-20T18:45:30.654321+00:00"

    # Verify build_query uses the same formatting
    query = DateHistogramQuery(
        name="test_date_format",
        **queries_config["bucket-file-download-histogram"]["params"]
    )

    agg_query = query.build_query(
        interval="day", start_date=start_date, end_date=end_date
    )

    # Convert to dict to inspect the query structure
    query_dict = agg_query.to_dict()

    # Extract the range filter from the query
    range_filter = None
    for filter_item in query_dict.get("query", {}).get("bool", {}).get("filter", []):
        if "range" in filter_item:
            range_filter = filter_item["range"]["timestamp"]
            break

    # Verify the range filter matches the formatted dates
    assert range_filter is not None, "Range filter should exist in query"
    assert range_filter["gte"] == formatted_start
    assert range_filter["lte"] == formatted_end


def test_terms_query_date_formatting_config_disabled(app, queries_config):
    """Test date formatting in TermsQuery.build_query when config is False.

    Verifies that TermsQuery also correctly formats dates in range filters
    for compatibility with strict_date_hour_minute_second.
    """
    # Ensure the config is False (default)
    app.config["STATS_EVENTS_UTC_DATETIME_ENABLED"] = False

    # Create datetimes with microseconds and timezone
    start_date = datetime.datetime(
        2024, 1, 15, 10, 30, 45, 123456, tzinfo=datetime.timezone.utc
    )
    end_date = datetime.datetime(
        2024, 1, 20, 18, 45, 30, 654321, tzinfo=datetime.timezone.utc
    )

    # Test format_datetime_iso directly
    formatted_start = format_datetime_iso(start_date)
    formatted_end = format_datetime_iso(end_date)

    # Should strip both microseconds and timezone
    assert formatted_start == "2024-01-15T10:30:45.123456"
    assert formatted_end == "2024-01-20T18:45:30.654321"

    # Verify build_query uses the same formatting
    query = TermsQuery(
        name="test_terms_date_format",
        **queries_config["bucket-file-download-total"]["params"]
    )

    agg_query = query.build_query(start_date=start_date, end_date=end_date)

    # Convert to dict to inspect the query structure
    query_dict = agg_query.to_dict()

    # Extract the range filter from the query
    range_filter = None
    for filter_item in query_dict.get("query", {}).get("bool", {}).get("filter", []):
        if "range" in filter_item:
            range_filter = filter_item["range"]["timestamp"]
            break

    # Verify the range filter matches the formatted dates
    assert range_filter is not None, "Range filter should exist in query"
    assert range_filter["gte"] == formatted_start
    assert range_filter["lte"] == formatted_end


def test_terms_query_date_formatting_config_enabled(app, queries_config):
    """Test date formatting in TermsQuery.build_query when config is True.

    Verifies that TermsQuery preserves full datetime precision when configured.
    """
    # Enable the config
    app.config["STATS_EVENTS_UTC_DATETIME_ENABLED"] = True

    # Create datetimes with microseconds and timezone
    start_date = datetime.datetime(
        2024, 1, 15, 10, 30, 45, 123456, tzinfo=datetime.timezone.utc
    )
    end_date = datetime.datetime(
        2024, 1, 20, 18, 45, 30, 654321, tzinfo=datetime.timezone.utc
    )

    # Test format_datetime_iso directly
    formatted_start = format_datetime_iso(start_date)
    formatted_end = format_datetime_iso(end_date)

    # Should keep both microseconds and timezone
    assert formatted_start == "2024-01-15T10:30:45.123456+00:00"
    assert formatted_end == "2024-01-20T18:45:30.654321+00:00"

    # Verify build_query uses the same formatting
    query = TermsQuery(
        name="test_terms_date_format",
        **queries_config["bucket-file-download-total"]["params"]
    )

    agg_query = query.build_query(start_date=start_date, end_date=end_date)

    # Convert to dict to inspect the query structure
    query_dict = agg_query.to_dict()

    # Extract the range filter from the query
    range_filter = None
    for filter_item in query_dict.get("query", {}).get("bool", {}).get("filter", []):
        if "range" in filter_item:
            range_filter = filter_item["range"]["timestamp"]
            break

    # Verify the range filter matches the formatted dates
    assert range_filter is not None, "Range filter should exist in query"
    assert range_filter["gte"] == formatted_start
    assert range_filter["lte"] == formatted_end
