"""Shared fixtures for all test modules."""
from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

SCHEMA = """
CREATE TABLE IF NOT EXISTS requests_hourly (
    ts              TIMESTAMP NOT NULL,
    path            VARCHAR   NOT NULL,
    country         VARCHAR   NOT NULL,
    status          SMALLINT  NOT NULL,
    count           BIGINT    NOT NULL,
    sample_interval DOUBLE    NOT NULL,
    est_requests    BIGINT    NOT NULL,
    bytes           BIGINT    NOT NULL,
    PRIMARY KEY (ts, path, country, status)
);
"""

VIEWS_SQL = (Path(__file__).parent.parent / "collector" / "views.sql").read_text()

# Representative rows covering all three categories.
# Timestamps are relative to now() so time-filtered views always see them.
TEST_ROWS = [
    # blog (200)
    ("/blog/build-a-custom-dell-csi-driver/", "US", 200, 100),
    ("/blog/another-post/",                   "DE", 200,  40),
    # site (200)
    ("/",                                     "FR", 200,  20),
    ("/assets/stylesheets/main.css",          "US", 200,  15),
    ("/sitemap.xml",                          "GB", 200,   5),
    # spam (various)
    ("/wp-admin/install.php",                 "RU", 301, 500),
    ("/.aws/config",                          "CN", 404,  30),
    ("/postnews.php",                         "ID", 301,  10),
]


@pytest.fixture
def mem_db() -> duckdb.DuckDBPyConnection:
    """In-memory DB with schema + views, no rows."""
    con = duckdb.connect(":memory:")
    con.execute(SCHEMA)
    con.execute(VIEWS_SQL)
    return con


@pytest.fixture
def seeded_db(mem_db: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyConnection:
    """In-memory DB with schema + views + TEST_ROWS (timestamps = now-1h)."""
    mem_db.executemany(
        "INSERT INTO requests_hourly VALUES "
        "(now() - INTERVAL '1 HOUR', ?, ?, ?, 1, 1.0, ?, 1000)",
        TEST_ROWS,
    )
    return mem_db


@pytest.fixture
def file_db(tmp_path: Path) -> Path:
    """File-based DB at a temp path, seeded with TEST_ROWS.
    Used for serve.py tests that open the file with read_only=True."""
    db_path = tmp_path / "test.duckdb"
    con = duckdb.connect(str(db_path))
    con.execute(SCHEMA)
    con.execute(VIEWS_SQL)
    con.executemany(
        "INSERT INTO requests_hourly VALUES "
        "(now() - INTERVAL '1 HOUR', ?, ?, ?, 1, 1.0, ?, 1000)",
        TEST_ROWS,
    )
    con.close()
    return db_path
