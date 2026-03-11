/* ============================================================
   PROJECT 1: Revenue Reconciliation & Anomaly Detection
   FILE:      05_deferred_revenue.sql
   PURPOSE:   Identify and calculate deferred revenue —
              income received in one period that belongs
              to a future period.

   BUSINESS CONTEXT:
   Under accrual accounting (IFRS 15 / ASC 606), revenue
   can only be recognised when the performance obligation
   is fulfilled. For subscriptions, licences, and advance
   payments, cash is received upfront but revenue must be
   spread across the service period.

   Example: A 12-month software licence sold on 1 March
   for £2,400 should recognise £200/month — not £2,400
   in March. The remaining £2,200 is "deferred revenue".

   THIS SCRIPT:
   1. Identifies transactions with future service periods
   2. Calculates the deferred vs recognised split
   3. Produces a period-level deferred revenue schedule
   4. Flags material misstatements
   ============================================================ */


-- ── STEP 1: IDENTIFY DEFERRED REVENUE TRANSACTIONS ───────────────────────────

WITH revenue_timing AS (
    SELECT
        transaction_id,
        customer_id,
        product_category,
        transaction_date,
        net_revenue,
        service_start_date,
        service_end_date,

        -- How many months does this contract span?
        DATEDIFF(month, service_start_date, service_end_date) + 1
                                            AS contract_months,

        -- Monthly recognised amount (straight-line, pro-rata)
        net_revenue /
            NULLIF(DATEDIFF(month, service_start_date, service_end_date) + 1, 0)
                                            AS monthly_rev_amount,

        -- Revenue correctly recognised in transaction month
        net_revenue /
            NULLIF(DATEDIFF(month, service_start_date, service_end_date) + 1, 0)
                                            AS current_period_revenue,

        -- Amount that belongs to future periods
        CASE
            WHEN service_end_date > EOMONTH(transaction_date)
            THEN net_revenue * (
                CAST(DATEDIFF(month, EOMONTH(transaction_date), service_end_date) AS FLOAT)
                / NULLIF(DATEDIFF(month, service_start_date, service_end_date) + 1, 0)
            )
            ELSE 0
        END                                 AS deferred_amount,

        CASE
            WHEN service_end_date > EOMONTH(transaction_date) THEN 'DEFERRED'
            ELSE 'FULLY_RECOGNISED'
        END                                 AS recognition_status
    FROM vw_sales_deduplicated
    WHERE service_start_date IS NOT NULL
      AND service_end_date   IS NOT NULL
)

SELECT
    transaction_id,
    customer_id,
    product_category,
    transaction_date,
    net_revenue                             AS total_contract_value,
    service_start_date,
    service_end_date,
    contract_months,
    ROUND(monthly_rev_amount, 2)           AS monthly_recognised,
    ROUND(current_period_revenue, 2)       AS recognised_this_period,
    ROUND(deferred_amount, 2)              AS deferred_to_future,
    recognition_status,

    -- Material misstatement flag (>10% overstated if all recognised now)
    CASE
        WHEN deferred_amount / NULLIF(net_revenue, 0) > 0.5
            THEN '🔴 MATERIAL — >50% deferred'
        WHEN deferred_amount / NULLIF(net_revenue, 0) > 0.1
            THEN '🟡 REVIEW   — >10% deferred'
        ELSE '🟢 IMMATERIAL'
    END                                     AS materiality_flag

FROM revenue_timing
ORDER BY deferred_amount DESC;


-- ── STEP 2: DEFERRED REVENUE SCHEDULE BY FUTURE PERIOD ───────────────────────
-- For each future month, how much revenue will be recognised?

WITH revenue_timing AS (
    SELECT
        transaction_id,
        customer_id,
        product_category,
        net_revenue,
        service_start_date,
        service_end_date,
        DATEDIFF(month, service_start_date, service_end_date) + 1 AS contract_months,
        net_revenue /
            NULLIF(DATEDIFF(month, service_start_date, service_end_date) + 1, 0)
                                            AS monthly_amount
    FROM vw_sales_deduplicated
    WHERE service_start_date IS NOT NULL
      AND service_end_date   IS NOT NULL
      AND service_end_date   > EOMONTH(GETDATE())  -- has future periods
),

-- Generate a row per month per contract using recursive CTE
monthly_schedule AS (
    SELECT
        transaction_id,
        customer_id,
        product_category,
        monthly_amount,
        service_start_date                  AS recognition_month_start,
        service_end_date
    FROM revenue_timing

    UNION ALL

    SELECT
        transaction_id,
        customer_id,
        product_category,
        monthly_amount,
        DATEADD(month, 1, recognition_month_start),
        service_end_date
    FROM monthly_schedule
    WHERE DATEADD(month, 1, recognition_month_start) <= service_end_date
)

SELECT
    FORMAT(recognition_month_start, 'yyyy-MM')  AS recognition_period,
    COUNT(DISTINCT transaction_id)              AS contracts_recognising,
    COUNT(DISTINCT customer_id)                 AS customers,
    ROUND(SUM(monthly_amount), 2)               AS revenue_to_recognise
FROM monthly_schedule
WHERE recognition_month_start > EOMONTH(GETDATE())   -- future periods only
GROUP BY FORMAT(recognition_month_start, 'yyyy-MM')
ORDER BY recognition_period
OPTION (MAXRECURSION 500);


-- ── STEP 3: PERIOD SUMMARY — RECOGNISED VS DEFERRED ──────────────────────────

WITH timing AS (
    SELECT
        FORMAT(transaction_date, 'yyyy-MM')     AS period,
        SUM(net_revenue)                        AS total_billed,
        SUM(
            CASE
                WHEN service_end_date > EOMONTH(transaction_date)
                THEN net_revenue * (
                    CAST(DATEDIFF(month, EOMONTH(transaction_date), service_end_date) AS FLOAT)
                    / NULLIF(DATEDIFF(month, service_start_date, service_end_date) + 1, 0)
                )
                ELSE 0
            END
        )                                       AS deferred_revenue,
        SUM(
            CASE
                WHEN service_start_date IS NULL THEN net_revenue
                ELSE net_revenue /
                    NULLIF(DATEDIFF(month, service_start_date, service_end_date) + 1, 0)
            END
        )                                       AS recognised_revenue
    FROM vw_sales_deduplicated
    GROUP BY FORMAT(transaction_date, 'yyyy-MM')
)
SELECT
    period,
    ROUND(total_billed, 2)                  AS total_billed,
    ROUND(recognised_revenue, 2)            AS recognised_this_period,
    ROUND(deferred_revenue, 2)              AS deferred_to_future,
    ROUND(100.0 * deferred_revenue / NULLIF(total_billed, 0), 1)
                                            AS pct_deferred
FROM timing
ORDER BY period;
