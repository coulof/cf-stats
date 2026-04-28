"""Unit tests for collector/collector.py — pure logic, no network calls."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from collector.collector import day_chunks, parse_iso, to_records


# ── parse_iso ──────────────────────────────────────────────────────────────────

def test_parse_iso_with_z():
    dt = parse_iso("2026-04-28T10:00:00Z")
    assert dt == datetime(2026, 4, 28, 10, 0, 0, tzinfo=timezone.utc)


def test_parse_iso_with_offset():
    dt = parse_iso("2026-04-28T12:00:00+02:00")
    assert dt == datetime(2026, 4, 28, 10, 0, 0, tzinfo=timezone.utc)


# ── day_chunks ─────────────────────────────────────────────────────────────────

def _utc(year, month, day, hour=0):
    return datetime(year, month, day, hour, tzinfo=timezone.utc)


def test_day_chunks_less_than_one_day():
    since = _utc(2026, 4, 28, 0)
    until = _utc(2026, 4, 28, 12)
    chunks = day_chunks(since, until)
    assert chunks == [(since, until)]


def test_day_chunks_exactly_one_day():
    since = _utc(2026, 4, 27)
    until = _utc(2026, 4, 28)
    chunks = day_chunks(since, until)
    assert len(chunks) == 1
    assert chunks[0] == (since, until)


def test_day_chunks_30_days():
    since = _utc(2026, 3, 29)
    until = _utc(2026, 4, 28)
    chunks = day_chunks(since, until)
    assert len(chunks) == 30
    # First chunk starts at since
    assert chunks[0][0] == since
    # Each chunk is 24h wide
    for s, e in chunks:
        assert e - s == timedelta(hours=24)
    # Last chunk ends at until
    assert chunks[-1][1] == until


def test_day_chunks_30h_gives_two_chunks():
    """30 hours → chunk 1 = 24h, chunk 2 = 6h."""
    since = _utc(2026, 4, 27)
    until = since + timedelta(hours=30)
    chunks = day_chunks(since, until)
    assert len(chunks) == 2
    assert chunks[0][1] - chunks[0][0] == timedelta(hours=24)
    assert chunks[1][1] - chunks[1][0] == timedelta(hours=6)


def test_day_chunks_contiguous():
    """Every chunk end must equal the next chunk start — no gaps."""
    since = _utc(2026, 4, 1)
    until = _utc(2026, 4, 28)
    chunks = day_chunks(since, until)
    for (_, e1), (s2, _) in zip(chunks, chunks[1:]):
        assert e1 == s2


# ── to_records ─────────────────────────────────────────────────────────────────

def _make_row(path="/blog/foo/", country="US", status=200,
              count=5, sample_interval=2.0, bytes_=12345):
    return {
        "count": count,
        "dimensions": {
            "datetimeHour":      "2026-04-28T10:00:00Z",
            "clientRequestPath": path,
            "clientCountryName": country,
            "edgeResponseStatus": status,
        },
        "sum": {"edgeResponseBytes": bytes_},
        "avg": {"sampleInterval": sample_interval},
    }


def test_to_records_basic():
    ts, path, country, status, count, si, est, byt = to_records([_make_row()])[0]
    assert path    == "/blog/foo/"
    assert country == "US"
    assert status  == 200
    assert count   == 5
    assert si      == 2.0
    assert est     == 10        # count * sample_interval
    assert byt     == 12345


def test_to_records_null_path_and_country():
    row = _make_row(path=None, country=None)
    _, path, country, *_ = to_records([row])[0]
    assert path    == ""
    assert country == ""


def test_to_records_missing_avg_defaults_to_1():
    raw = _make_row(count=7)
    raw["avg"] = None
    _, _, _, _, count, si, est, _ = to_records([raw])[0]
    assert si  == 1.0
    assert est == 7


def test_to_records_multiple_rows():
    rows = [_make_row(path=f"/blog/post-{i}/", count=i) for i in range(1, 6)]
    records = to_records(rows)
    assert len(records) == 5
    paths = [r[1] for r in records]
    assert paths == [f"/blog/post-{i}/" for i in range(1, 6)]


def test_to_records_est_requests_rounds():
    row = _make_row(count=3, sample_interval=1.7)
    *_, est, _ = to_records([row])[0]
    assert est == round(3 * 1.7)  # 5
