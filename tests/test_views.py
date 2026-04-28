"""Tests for collector/views.sql — classification logic and aggregate views."""
from __future__ import annotations

import duckdb
import pytest

# ── classification (v_classified) ─────────────────────────────────────────────

def category(db: duckdb.DuckDBPyConnection, path: str) -> str:
    db.execute(
        "INSERT OR REPLACE INTO requests_hourly VALUES "
        "(now() - INTERVAL '1 HOUR', ?, 'US', 200, 1, 1.0, 10, 1000)",
        [path],
    )
    return db.execute(
        "SELECT category FROM v_classified WHERE path = ?", [path]
    ).fetchone()[0]


@pytest.mark.parametrize("path", [
    "/blog/build-a-custom-dell-csi-driver/",
    "/blog/another-post/",
    "/blog/",
])
def test_blog_paths(mem_db, path):
    assert category(mem_db, path) == "blog"


@pytest.mark.parametrize("path", [
    "/",
    "/index.html",
    "/sitemap.xml",
    "/sitemap.xml.gz",
    "/robots.txt",
    "/favicon.ico",
    "/404.html",
    "/search.html",
    "/assets/stylesheets/main.css",
    "/assets/images/logo.png",
    "/categories/tech/",
    "/tags/kubernetes/",
    "/archive/",
    "/about/",
])
def test_site_paths(mem_db, path):
    assert category(mem_db, path) == "site"


@pytest.mark.parametrize("path", [
    "/wp-admin/install.php",
    "/.aws/config",
    "/.env",
    "/postnews.php",
    "/.well-known/pki-validation/xmrlpc.php",
    "/update/403.php",
    "/autoload_classmap/",
    "/type.php",
])
def test_spam_paths(mem_db, path):
    assert category(mem_db, path) == "spam"


# ── v_daily_split ─────────────────────────────────────────────────────────────

def test_daily_split_counts(seeded_db):
    row = seeded_db.execute(
        "SELECT blog, site, spam, total FROM v_daily_split"
    ).fetchone()
    blog, site, spam, total = row
    assert blog  == 140   # 100 + 40
    assert site  ==  40   # 20 + 15 + 5
    assert spam  == 540   # 500 + 30 + 10
    assert total == blog + site + spam


# ── v_top_blog_30d ─────────────────────────────────────────────────────────────

def test_top_blog_ranking(seeded_db):
    rows = seeded_db.execute(
        "SELECT path, requests FROM v_top_blog_30d"
    ).fetchall()
    paths = [r[0] for r in rows]
    assert paths[0] == "/blog/build-a-custom-dell-csi-driver/"  # 100 requests
    assert paths[1] == "/blog/another-post/"                    #  40 requests
    # no site or spam paths in the blog view
    assert all(p.startswith("/blog/") for p in paths)


def test_top_blog_only_200s(mem_db):
    """404 on a blog path should not count."""
    mem_db.execute(
        "INSERT INTO requests_hourly VALUES "
        "(now() - INTERVAL '1 HOUR', '/blog/missing/', 'US', 404, 1, 1.0, 50, 1000)"
    )
    mem_db.execute(
        "INSERT INTO requests_hourly VALUES "
        "(now() - INTERVAL '1 HOUR', '/blog/missing/', 'US', 200, 1, 1.0, 10, 1000)"
    )
    row = mem_db.execute(
        "SELECT requests FROM v_top_blog_30d WHERE path = '/blog/missing/'"
    ).fetchone()
    assert row[0] == 10  # only the 200


# ── v_country_30d ─────────────────────────────────────────────────────────────

def test_country_view_excludes_spam(seeded_db):
    countries = {r[0] for r in seeded_db.execute(
        "SELECT country FROM v_country_30d"
    ).fetchall()}
    assert "US" in countries
    assert "DE" in countries
    assert "FR" in countries
    # RU and CN only hit spam paths → must not appear
    assert "RU" not in countries
    assert "CN" not in countries


def test_country_view_ordering(seeded_db):
    rows = seeded_db.execute(
        "SELECT country, requests FROM v_country_30d"
    ).fetchall()
    requests = [r[1] for r in rows]
    assert requests == sorted(requests, reverse=True)


# ── v_top_spam_30d ─────────────────────────────────────────────────────────────

def test_spam_view_top_entry(seeded_db):
    top = seeded_db.execute(
        "SELECT path, requests FROM v_top_spam_30d LIMIT 1"
    ).fetchone()
    assert top[0] == "/wp-admin/install.php"
    assert top[1] == 500
