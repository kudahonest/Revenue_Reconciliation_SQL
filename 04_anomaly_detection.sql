/* ============================================================
   PROJECT 1: Revenue Reconciliation & Anomaly Detection
   FILE:      04_anomaly_detection.sql
   PURPOSE:   Flag statistically unusual transactions for
              priority investigation.

   TWO METHODS:
   1. Z-Score   — how many standard deviations from the mean?
                  Best for normally distributed data.
   2. IQR       — interquartile range fence method.
                  More robust for skewed distributions (common
                  in revenue data with large outliers).

   BUSINESS CONTEXT:
   After reconciliation, not all matched transactions are
   equally trustworthy. Large or unusual values — even if
   they match across systems — warrant review. This script
   produces a prioritised list of transactions to investigate.
   ============================================================ */


-- ── METHOD 1: Z-SCORE ANOMALY DETECTION ──────────────────────────────────────

WITH category_stats AS (
    -- Calculate mean and standard deviation per product category
    SELECT
        product_category,
        COUNT(*)                            AS transaction_count,
        AVG(net_revenue)                    AS mean_net_revenue,
        STDEV(net_revenue)                  AS std_net_revenue,
        MIN(net_revenue)                    AS min_net_revenue,
        MAX(net_revenue)                    AS max_net_revenue
    FROM vw_sales_deduplicated
    GROUP BY product_category
),

scored AS (
    SELECT
        s.transaction_id,
        s.customer_id,
        s.product_category,
        s.transaction_date,
        s.net_revenue,
        c.mean_net_revenue,
        c.std_net_revenue,

        -- Z-score: how many standard deviations from the mean?
        ROUND(
            (s.net_revenue - c.mean_net_revenue)
            / NULLIF(c.std_net_revenue, 0),
        3)                                  AS z_score,

        -- Percentile rank within category
        ROUND(
            PERCENT_RANK() OVER (
                PARTITION BY s.product_category
                ORDER BY s.net_revenue
            ) * 100, 2
        )                                   AS revenue_percentile
    FROM vw_sales_deduplicated  s
    JOIN category_stats         c ON s.product_category = c.product_category
)

SELECT
    transaction_id,
    customer_id,
    product_category,
    transaction_date,
    net_revenue,
    ROUND(mean_net_revenue, 2)              AS category_mean,
    ROUND(std_net_revenue, 2)              AS category_std_dev,
    z_score,
    revenue_percentile,

    -- Priority flag based on z-score severity
    CASE
        WHEN ABS(z_score) > 4   THEN '🔴 CRITICAL — >4 std devs'
        WHEN ABS(z_score) > 3   THEN '🟠 HIGH     — >3 std devs'
        WHEN ABS(z_score) > 2   THEN '🟡 MEDIUM   — >2 std devs'
        ELSE                         '🟢 NORMAL'
    END                                     AS anomaly_severity,

    CASE
        WHEN z_score > 0 THEN 'ABOVE AVERAGE'
        ELSE                  'BELOW AVERAGE'
    END                                     AS direction,

    -- Rank within category (1 = most anomalous)
    RANK() OVER (
        PARTITION BY product_category
        ORDER BY ABS(z_score) DESC
    )                                       AS anomaly_rank_in_category

FROM scored
WHERE ABS(z_score) > 2      -- flag anything beyond 2 standard deviations
ORDER BY ABS(z_score) DESC;


-- ── METHOD 2: IQR (INTERQUARTILE RANGE) DETECTION ───────────────────────────
-- More robust than z-score when data is skewed (common in revenue).
-- IQR fence: anything outside Q1 - 1.5*IQR or Q3 + 1.5*IQR is flagged.

WITH iqr_stats AS (
    SELECT
        product_category,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY net_revenue) AS q1,
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY net_revenue) AS median_val,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY net_revenue) AS q3
    FROM vw_sales_deduplicated
    GROUP BY product_category
),

iqr_calculated AS (
    SELECT
        *,
        q3 - q1                         AS iqr,
        q1 - 1.5 * (q3 - q1)           AS lower_fence,
        q3 + 1.5 * (q3 - q1)           AS upper_fence
    FROM iqr_stats
)

SELECT
    s.transaction_id,
    s.customer_id,
    s.product_category,
    s.transaction_date,
    s.net_revenue,
    ROUND(i.q1, 2)                      AS q1,
    ROUND(i.median_val, 2)             AS median_revenue,
    ROUND(i.q3, 2)                      AS q3,
    ROUND(i.iqr, 2)                     AS iqr,
    ROUND(i.lower_fence, 2)            AS lower_fence,
    ROUND(i.upper_fence, 2)            AS upper_fence,

    CASE
        WHEN s.net_revenue > i.upper_fence THEN '🔴 HIGH OUTLIER'
        WHEN s.net_revenue < i.lower_fence THEN '🔵 LOW OUTLIER'
        ELSE                                    '🟢 WITHIN RANGE'
    END                                 AS iqr_flag,

    -- How far outside the fence?
    CASE
        WHEN s.net_revenue > i.upper_fence
            THEN ROUND(s.net_revenue - i.upper_fence, 2)
        WHEN s.net_revenue < i.lower_fence
            THEN ROUND(i.lower_fence - s.net_revenue, 2)
        ELSE 0
    END                                 AS distance_from_fence

FROM vw_sales_deduplicated  s
JOIN iqr_calculated          i ON s.product_category = i.product_category
WHERE s.net_revenue > i.upper_fence
   OR s.net_revenue < i.lower_fence
ORDER BY distance_from_fence DESC;


-- ── COMBINED: FLAGGED BY EITHER METHOD ───────────────────────────────────────
-- Transactions flagged by BOTH methods are the highest priority.

WITH z_flagged AS (
    SELECT transaction_id, 'Z_SCORE' AS method
    FROM (
        SELECT
            transaction_id,
            net_revenue,
            product_category,
            AVG(net_revenue) OVER (PARTITION BY product_category) AS mean_val,
            STDEV(net_revenue) OVER (PARTITION BY product_category) AS std_val
        FROM vw_sales_deduplicated
    ) z
    WHERE ABS((net_revenue - mean_val) / NULLIF(std_val, 0)) > 2
),
iqr_flagged AS (
    SELECT s.transaction_id, 'IQR' AS method
    FROM vw_sales_deduplicated s
    JOIN (
        SELECT
            product_category,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY net_revenue) AS q1,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY net_revenue) AS q3
        FROM vw_sales_deduplicated
        GROUP BY product_category
    ) i ON s.product_category = i.product_category
    WHERE s.net_revenue > i.q3 + 1.5*(i.q3-i.q1)
       OR s.net_revenue < i.q1 - 1.5*(i.q3-i.q1)
),
both_flagged AS (
    SELECT transaction_id FROM z_flagged
    INTERSECT
    SELECT transaction_id FROM iqr_flagged
)
SELECT
    s.transaction_id,
    s.customer_id,
    s.product_category,
    s.transaction_date,
    s.net_revenue,
    '⚠️ FLAGGED BY BOTH METHODS — HIGHEST PRIORITY' AS investigation_priority
FROM vw_sales_deduplicated s
WHERE transaction_id IN (SELECT transaction_id FROM both_flagged)
ORDER BY net_revenue DESC;
