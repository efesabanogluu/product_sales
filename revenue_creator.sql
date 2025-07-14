-- Generates a day-by-day date for January 2025 (recursive CTE).
WITH RECURSIVE date_range(date_id) AS (
    SELECT DATE('2025-01-01')
    UNION ALL
    SELECT DATE(date_id, '+1 day')
    FROM date_range
    WHERE date_id < '2025-01-31'
),
-- Retrieves daily total sales from the sales table.
sales_agg AS (
    SELECT
        sku_id,
        DATE(orderdate_utc) AS date_id,
        SUM(sales) AS total_sales
    FROM sales
    WHERE DATE(orderdate_utc) BETWEEN '2025-01-01' AND '2025-01-31'
    GROUP BY sku_id, DATE(orderdate_utc)
)
SELECT
    p.sku_id,
    d.date_id,
    p.price,
    COALESCE(sa.total_sales, 0) AS sales, -- NULL sales display as 0s.
    ROUND(p.price * COALESCE(sa.total_sales, 0), 2) AS revenue --Returns income to 2 decimal places.
FROM product p
CROSS JOIN date_range d -- for all (day,product) combinations.
LEFT JOIN sales_agg sa -- If there is sales data, it is merged, otherwise it remains NULL.
    ON p.sku_id = sa.sku_id AND d.date_id = sa.date_id
ORDER BY p.sku_id, d.date_id;
