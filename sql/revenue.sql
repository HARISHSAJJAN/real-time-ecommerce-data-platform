-- ==============================================================================
-- Revenue analytics — run through Trino:
--   trino --server localhost:8080 --catalog starrocks --schema ecommerce --file sql/revenue.sql
-- ==============================================================================

-- Total revenue to date (all completed purchase events).
SELECT ROUND(SUM(total_amount), 2) AS total_revenue
FROM starrocks.ecommerce.events
WHERE event_type = 'purchase';

-- Daily revenue trend.
SELECT
    event_date,
    ROUND(SUM(total_amount), 2) AS daily_revenue,
    COUNT(*) AS purchase_count
FROM starrocks.ecommerce.events
WHERE event_type = 'purchase'
GROUP BY event_date
ORDER BY event_date DESC;

-- Hourly revenue trend for a given day (adjust the date filter as needed).
SELECT
    event_date,
    event_hour,
    ROUND(SUM(total_amount), 2) AS hourly_revenue,
    COUNT(*) AS purchase_count
FROM starrocks.ecommerce.events
WHERE event_type = 'purchase'
GROUP BY event_date, event_hour
ORDER BY event_date DESC, event_hour DESC;

-- Revenue by country.
SELECT
    country,
    ROUND(SUM(total_amount), 2) AS revenue,
    COUNT(*) AS purchase_count
FROM starrocks.ecommerce.events
WHERE event_type = 'purchase'
GROUP BY country
ORDER BY revenue DESC;

-- Revenue by product (top 20).
SELECT
    e.product_id,
    p.name,
    p.category,
    ROUND(SUM(e.total_amount), 2) AS revenue,
    COUNT(*) AS purchase_count
FROM starrocks.ecommerce.events e
LEFT JOIN starrocks.ecommerce.products p ON e.product_id = p.product_id
WHERE e.event_type = 'purchase'
GROUP BY e.product_id, p.name, p.category
ORDER BY revenue DESC
LIMIT 20;

-- Pre-aggregated revenue by streaming window (from Spark's revenue_metrics sink) —
-- much cheaper than scanning raw events when only near-real-time totals are needed.
SELECT window_start, window_end, revenue, number_of_purchases, average_order_value
FROM starrocks.ecommerce.revenue_metrics
ORDER BY window_start DESC
LIMIT 60;
