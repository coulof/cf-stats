FROM python:3.13-slim

WORKDIR /app

# Install dependencies first for layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Application source
COPY collector/ ./collector/
COPY serve.py   .
COPY web/       ./web/
COPY favicon.svg .
COPY docker/collector-entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# DuckDB file lives on a named volume mounted at /data
RUN mkdir /data
ENV CF_STATS_DB=/data/stats.duckdb

EXPOSE 8080
