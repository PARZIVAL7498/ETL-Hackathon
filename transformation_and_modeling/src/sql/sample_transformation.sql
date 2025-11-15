-----------------------------------------------
-- BRONZE → SILVER (CLEANING & STANDARDIZATION)
-----------------------------------------------

-- Insert cleaned data into silver.transactions
INSERT INTO silver.transactions
SELECT
    id,
    LOWER(category) AS category,
    COALESCE(value, mean_value) AS value,
    timestamp
FROM (
    SELECT
        *,
        AVG(value) OVER () AS mean_value
    FROM bronze.transactions
) t;


-----------------------------------------------
-- SILVER → GOLD (DAILY AGGREGATION)
-----------------------------------------------

-- Insert daily aggregated data into gold.daily_aggregates
INSERT INTO gold.daily_aggregates
SELECT
    DATE(timestamp) AS date,
    category,
    SUM(value) AS total_value
FROM silver.transactions
GROUP BY DATE(timestamp), category;
