-- ==============================================================================
-- Customer analytics — run through Trino:
--   trino --server localhost:8080 --catalog starrocks --schema ecommerce --file sql/customer_analytics.sql
-- ==============================================================================

-- Active users (any event) in the last 24 hours of ingested data.
SELECT COUNT(DISTINCT user_id) AS active_users_24h
FROM starrocks.ecommerce.events
WHERE event_timestamp >= date_add('hour', -24, (SELECT MAX(event_timestamp) FROM starrocks.ecommerce.events));

-- Purchases per user (top 20 most frequent buyers).
SELECT
    user_id,
    COUNT(*) AS purchase_count,
    ROUND(SUM(total_amount), 2) AS total_spent
FROM starrocks.ecommerce.events
WHERE event_type = 'purchase'
GROUP BY user_id
ORDER BY purchase_count DESC
LIMIT 20;

-- Average order value overall.
SELECT ROUND(AVG(total_amount), 2) AS average_order_value
FROM starrocks.ecommerce.events
WHERE event_type = 'purchase';

-- Average order value by country.
SELECT
    country,
    ROUND(AVG(total_amount), 2) AS average_order_value,
    COUNT(*) AS purchases
FROM starrocks.ecommerce.events
WHERE event_type = 'purchase'
GROUP BY country
ORDER BY average_order_value DESC;

-- Device type distribution across all events.
SELECT
    device_type,
    COUNT(*) AS event_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_total
FROM starrocks.ecommerce.events
GROUP BY device_type
ORDER BY event_count DESC;

-- New vs. returning buyers: users whose first purchase happened today
-- vs. users who purchased before today (using the users dimension's signup_date
-- as a proxy for "new" within the last 7 days).
SELECT
    CASE WHEN u.signup_date >= date_add('day', -7, current_date) THEN 'new' ELSE 'returning' END AS user_segment,
    COUNT(DISTINCT e.user_id) AS buyers,
    ROUND(SUM(e.total_amount), 2) AS revenue
FROM starrocks.ecommerce.events e
JOIN starrocks.ecommerce.users u ON e.user_id = u.user_id
WHERE e.event_type = 'purchase'
GROUP BY CASE WHEN u.signup_date >= date_add('day', -7, current_date) THEN 'new' ELSE 'returning' END;
