-- ==============================================================================
-- Funnel analytics: product_view -> add_to_cart -> purchase — run through Trino:
--   trino --server localhost:8080 --catalog starrocks --schema ecommerce --file sql/funnel_analysis.sql
-- ==============================================================================

-- Overall funnel with stage-to-stage conversion rates.
WITH stage_counts AS (
    SELECT
        SUM(CASE WHEN event_type = 'product_view' THEN 1 ELSE 0 END) AS views,
        SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS cart_additions,
        SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchases
    FROM starrocks.ecommerce.events
)
SELECT
    views,
    cart_additions,
    purchases,
    ROUND(cart_additions * 1.0 / NULLIF(views, 0), 4) AS view_to_cart_rate,
    ROUND(purchases * 1.0 / NULLIF(cart_additions, 0), 4) AS cart_to_purchase_rate,
    ROUND(purchases * 1.0 / NULLIF(views, 0), 4) AS view_to_purchase_rate
FROM stage_counts;

-- Funnel broken down by country.
SELECT
    country,
    SUM(CASE WHEN event_type = 'product_view' THEN 1 ELSE 0 END) AS views,
    SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS cart_additions,
    SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchases,
    ROUND(
        SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) * 1.0
        / NULLIF(SUM(CASE WHEN event_type = 'product_view' THEN 1 ELSE 0 END), 0),
        4
    ) AS view_to_purchase_rate
FROM starrocks.ecommerce.events
GROUP BY country
ORDER BY views DESC;

-- Funnel broken down by device type.
SELECT
    device_type,
    SUM(CASE WHEN event_type = 'product_view' THEN 1 ELSE 0 END) AS views,
    SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS cart_additions,
    SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchases
FROM starrocks.ecommerce.events
GROUP BY device_type
ORDER BY views DESC;

-- Per-session funnel completion: sessions that viewed a product vs. those
-- that went on to add-to-cart and purchase within the same session.
WITH session_stages AS (
    SELECT
        session_id,
        MAX(CASE WHEN event_type = 'product_view' THEN 1 ELSE 0 END) AS viewed,
        MAX(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS added_to_cart,
        MAX(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchased
    FROM starrocks.ecommerce.events
    GROUP BY session_id
)
SELECT
    SUM(viewed) AS sessions_with_view,
    SUM(added_to_cart) AS sessions_with_cart_add,
    SUM(purchased) AS sessions_with_purchase,
    ROUND(SUM(added_to_cart) * 1.0 / NULLIF(SUM(viewed), 0), 4) AS session_view_to_cart_rate,
    ROUND(SUM(purchased) * 1.0 / NULLIF(SUM(added_to_cart), 0), 4) AS session_cart_to_purchase_rate
FROM session_stages;
