"""Integration tests for serve.py API endpoints."""
from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import serve


@pytest.fixture
def client(file_db: Path, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    """TestClient with serve.DB pointed at the seeded temp database."""
    monkeypatch.setattr(serve, "DB", file_db)
    return TestClient(serve.app)


# ── /api/health ───────────────────────────────────────────────────────────────

def test_health_ok(client):
    r = client.get("/api/health")
    assert r.status_code == 200
    data = r.json()
    assert data["rows"] == 8
    assert data["until_ts"] is not None
    assert data["since"] is not None


def test_health_no_db(tmp_path, monkeypatch):
    monkeypatch.setattr(serve, "DB", tmp_path / "nonexistent.duckdb")
    c = TestClient(serve.app)
    assert c.get("/api/health").status_code == 503


# ── /api/summary ──────────────────────────────────────────────────────────────

def test_summary_all_categories(client):
    r = client.get("/api/summary?period=all")
    assert r.status_code == 200
    by_cat = {row["category"]: row for row in r.json()}
    assert set(by_cat) == {"blog", "site", "spam"}


def test_summary_pct_sums_to_100(client):
    rows = client.get("/api/summary?period=all").json()
    total_pct = sum(r["pct"] for r in rows)
    assert abs(total_pct - 100.0) < 0.2   # float rounding tolerance


def test_summary_blog_requests(client):
    by_cat = {r["category"]: r for r in client.get("/api/summary?period=all").json()}
    assert by_cat["blog"]["requests"] == 140  # 100 + 40


def test_summary_invalid_period(client):
    assert client.get("/api/summary?period=fortnight").status_code == 422


# ── /api/traffic ──────────────────────────────────────────────────────────────

def test_traffic_returns_rows(client):
    r = client.get("/api/traffic?period=all&granularity=day")
    assert r.status_code == 200
    rows = r.json()
    assert len(rows) >= 1
    assert "ts_ms" in rows[0]
    assert "blog" in rows[0]


def test_traffic_totals_match_summary(client):
    traffic = client.get("/api/traffic?period=all&granularity=day").json()
    total_blog = sum(r["blog"] for r in traffic)
    summary = {r["category"]: r for r in client.get("/api/summary?period=all").json()}
    assert total_blog == summary["blog"]["requests"]


def test_traffic_invalid_granularity(client):
    assert client.get("/api/traffic?granularity=weekly").status_code == 422


def test_traffic_invalid_period(client):
    assert client.get("/api/traffic?period=bad").status_code == 422


# ── /api/top-blog ─────────────────────────────────────────────────────────────

def test_top_blog_ordering(client):
    rows = client.get("/api/top-blog?period=all").json()
    assert rows[0]["requests"] >= rows[-1]["requests"]


def test_top_blog_only_blog_paths(client):
    rows = client.get("/api/top-blog?period=all").json()
    assert all(r["path"].startswith("/blog/") for r in rows)


def test_top_blog_limit(client):
    assert len(client.get("/api/top-blog?period=all").json()) <= 15


# ── /api/countries ────────────────────────────────────────────────────────────

def test_countries_excludes_spam_traffic(client):
    rows = client.get("/api/countries?period=all").json()
    countries = {r["country"] for r in rows}
    assert "RU" not in countries   # only hit wp-admin
    assert "CN" not in countries   # only hit /.aws/config


def test_countries_includes_legit(client):
    rows = client.get("/api/countries?period=all").json()
    countries = {r["country"] for r in rows}
    assert "US" in countries
    assert "DE" in countries


def test_countries_limit(client):
    assert len(client.get("/api/countries?period=all").json()) <= 15


# ── static assets ─────────────────────────────────────────────────────────────

def test_favicon_served(client):
    r = client.get("/favicon.svg")
    assert r.status_code == 200
    assert "svg" in r.headers["content-type"]


def test_root_serves_html(client):
    r = client.get("/")
    assert r.status_code == 200
    assert "text/html" in r.headers["content-type"]
    assert "lafabrique.ai" in r.text
