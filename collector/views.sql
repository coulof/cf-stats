-- views.sql — classification + dashboard aggregates.
-- Idempotent. Re-run any time to update rules:
--   duckdb stats.duckdb < collector/views.sql

-- 1. Classify every row as blog / site / spam based on path + status.
--    Edit the WHEN clauses to refine. Spam is the default bucket.
CREATE OR REPLACE VIEW v_classified AS
SELECT
  *,
  CASE
    -- The useful content: every blog post lives under /blog/
    WHEN path LIKE '/blog/%' THEN 'blog'

    -- Site chrome: homepage, generated pages, static assets
    WHEN path IN ('/', '', '/index.html', '/sitemap.xml', '/sitemap.xml.gz',
                  '/robots.txt', '/favicon.ico', '/manifest.json',
                  '/404.html', '/search.html')                              THEN 'site'
    WHEN path LIKE '/assets/%'                                              THEN 'site'
    WHEN path LIKE '/categories/%' OR path LIKE '/tags/%'                   THEN 'site'
    WHEN path LIKE '/archive%'    OR path LIKE '/about%'                    THEN 'site'

    -- Everything else (wp-admin, .env, .git, .aws, random .php, etc.)
    ELSE 'spam'
  END AS category
FROM requests_hourly;


-- 2. Hourly totals per category — the "real vs noise" headline chart.
CREATE OR REPLACE VIEW v_by_category_hourly AS
SELECT ts, category,
       SUM(est_requests) AS requests,
       SUM(bytes)        AS bytes
FROM v_classified
GROUP BY ts, category;


-- 3. Daily split with one row per day, three columns.
CREATE OR REPLACE VIEW v_daily_split AS
SELECT
  date_trunc('day', ts) AS d,
  SUM(CASE WHEN category = 'blog' THEN est_requests ELSE 0 END) AS blog,
  SUM(CASE WHEN category = 'site' THEN est_requests ELSE 0 END) AS site,
  SUM(CASE WHEN category = 'spam' THEN est_requests ELSE 0 END) AS spam,
  SUM(est_requests)                                              AS total
FROM v_classified
GROUP BY 1
ORDER BY 1;


-- 4. Top blog posts, last 30 days, only successful loads.
CREATE OR REPLACE VIEW v_top_blog_30d AS
SELECT path, SUM(est_requests) AS requests
FROM v_classified
WHERE category = 'blog'
  AND ts > now() - INTERVAL 30 DAY
  AND status IN (200, 304)
GROUP BY path
ORDER BY requests DESC
LIMIT 50;


-- 5. Country breakdown for legit traffic only.
CREATE OR REPLACE VIEW v_country_30d AS
SELECT country, SUM(est_requests) AS requests
FROM v_classified
WHERE category IN ('blog', 'site')
  AND ts > now() - INTERVAL 30 DAY
  AND status IN (200, 304)
GROUP BY country
ORDER BY requests DESC;


-- 6. Spam intelligence — what scanners are hitting hardest. Useful for
--    refining rules in v_classified above (e.g. add a new exact match
--    if a "spam" pattern is actually you).
CREATE OR REPLACE VIEW v_top_spam_30d AS
SELECT path, status, SUM(est_requests) AS requests
FROM v_classified
WHERE category = 'spam'
  AND ts > now() - INTERVAL 30 DAY
GROUP BY path, status
ORDER BY requests DESC
LIMIT 50;
