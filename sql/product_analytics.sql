-- ==============================================================================
-- Product analytics — run through Trino:
--   trino --server localhost:8080 --catalog starrocks --schema ecommerce --file sql/product_analytics.sql
-- ==============================================================================

-- Top 20 products by revenue.
SELECT
    e.product_id,
    p.name,
    p.category,
    ROUND(SUM(e.total_amount), 2) AS revenue,
    COUNT(*) AS purchases
FROM starrocks.ecommerce.events e
LEFT JOIN starrocks.ecommerce.products p ON e.product_id = p.product_id
WHERE e.event_type = 'purchase'
GROUP BY e.product_id, p.name, p.category
ORDER BY revenue DESC
LIMIT 20;

-- Product conversion rate: views -> purchases.
SELECT
    e.product_id,
    p.name,
    SUM(CASE WHEN e.event_type = 'product_view' THEN 1 ELSE 0 END) AS views,
    SUM(CASE WHEN e.event_type = 'purchase' THEN 1 ELSE 0 END) AS purchases,
    ROUND(
        SUM(CASE WHEN e.event_type = 'purchase' THEN 1 ELSE 0 END) * 1.0
        / NULLIF(SUM(CASE WHEN e.event_type = 'product_view' THEN 1 ELSE 0 END), 0),
        4
    ) AS conversion_rate
FROM starrocks.ecommerce.events e
LEFT JOIN starrocks.ecommerce.products p ON e.product_id = p.product_id
GROUP BY e.product_id, p.name
HAVING SUM(CASE WHEN e.event_type = 'product_view' THEN 1 ELSE 0 END) > 0
ORDER BY conversion_rate DESC
LIMIT 20;

-- Category performance.
SELECT
    p.category,
    ROUND(SUM(e.total_amount), 2) AS revenue,
    COUNT(*) AS purchases
FROM starrocks.ecommerce.events e
JOIN starrocks.ecommerce.products p ON e.product_id = p.product_id
WHERE e.event_type = 'purchase'
GROUP BY p.category
ORDER BY revenue DESC;

-- Products with declining sales: compares today's purchase count against
-- yesterday's for each product, surfacing the largest drops.
WITH daily AS (
    SELECT product_id, event_date, COUNT(*) AS purchase_count
    FROM starrocks.ecommerce.events
    WHERE event_type = 'purchase'
    GROUP BY product_id, event_date
),
today AS (
    SELECT product_id, purchase_count AS today_count
    FROM daily
    WHERE event_date = current_date
),
yesterday AS (
    SELECT product_id, purchase_count AS yesterday_count
    FROM daily
    WHERE event_date = date_add('day', -1, current_date)
)
SELECT
    y.product_id,
    p.name,
    y.yesterday_count,
    COALESCE(t.today_count, 0) AS today_count,
    COALESCE(t.today_count, 0) - y.yesterday_count AS change
FROM yesterday y
LEFT JOIN today t ON y.product_id = t.product_id
LEFT JOIN starrocks.ecommerce.products p ON y.product_id = p.product_id
WHERE COALESCE(t.today_count, 0) < y.yesterday_count
ORDER BY change ASC
LIMIT 20;

-- Pre-aggregated near-real-time product performance (from Spark's product_metrics sink).
SELECT window_start, product_id, views, cart_additions, purchases, revenue, conversion_rate
FROM starrocks.ecommerce.product_metrics
ORDER BY window_start DESC, revenue DESC
LIMIT 50;
