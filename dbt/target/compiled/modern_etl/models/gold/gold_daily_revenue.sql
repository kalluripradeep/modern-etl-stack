-- Gold Layer: Daily revenue metrics by status
-- Business-ready aggregated data



SELECT 
    DATE(order_date) as order_date,
    status,
    COUNT(*) as order_count,
    SUM(total_amount) as total_revenue,
    AVG(total_amount) as avg_order_value,
    MIN(total_amount) as min_order_value,
    MAX(total_amount) as max_order_value,
    CURRENT_TIMESTAMP as calculated_at
FROM "destdb"."analytics"."silver_orders"
GROUP BY DATE(order_date), status
ORDER BY order_date DESC, status