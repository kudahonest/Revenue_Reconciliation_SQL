/* ============================================================
   PROJECT: Revenue Reconciliation & Anomaly Detection
   FILE:    01_data_profiling.sql
   PURPOSE: Initial data quality assessment and profiling
            before any reconciliation work begins.
            
   BUSINESS CONTEXT:
   Before reconciling revenue across systems, we need to
   understand the shape, completeness, and quality of the
   source data. This script profiles two source tables:
     - sales_transactions  (point-of-sale / order system)
     - gl_postings         (general ledger / finance system)
   
   TECHNIQUES USED:
   - Aggregate functions (COUNT, SUM, MIN, MAX, AVG)
   - NULL detection and completeness scoring
   - Duplicate detection using GROUP BY + HAVING
   - Date range validation
   - Cross-system row count comparison
   ============================================================ */


-- ── SECTION 1: ROW COUNTS & DATE RANGES ─────────────────────────────────────
-- Always start by understanding the volume and timespan of each dataset.
-- Mismatched date ranges are a common source of reconciliation breaks.

SELECT
    'sales_transactions'        AS source_system,
    COUNT(*)                    AS total_rows,
    COUNT(DISTINCT transaction_id) AS distinct_transactions,
    MIN(transaction_date)       AS earliest_date,
    MAX(transaction_date)       AS latest_date,
    SUM(gross_revenue)          AS total_gross_revenue,
    SUM(net_revenue)            AS total_net_revenue,
    SUM(tax_amount)             AS total_tax
FROM sales_transactions

UNION ALL

SELECT
    'gl_postings'               AS source_system,
    COUNT(*)                    AS total_rows,
    COUNT(DISTINCT posting_id)  AS distinct_transactions,
    MIN(posting_date)           AS earliest_date,
    MAX(posting_date)           AS latest_date,
    SUM(debit_amount)           AS total_gross_revenue,
    SUM(net_amount)             AS total_net_revenue,
    SUM(tax_posted)             AS total_tax
FROM gl_postings
WHERE account_type = 'REVENUE';


-- ── SECTION 2: NULL / COMPLETENESS CHECK ────────────────────────────────────
-- Identify which fields have missing values and what % of records are affected.
-- Critical fields (transaction_id, date, amount) should be 0% null.

SELECT
    COUNT(*)                                        AS total_rows,

    -- Key identifiers
    SUM(CASE WHEN transaction_id  IS NULL THEN 1 ELSE 0 END) AS null_transaction_id,
    SUM(CASE WHEN customer_id     IS NULL THEN 1 ELSE 0 END) AS null_customer_id,
    SUM(CASE WHEN product_code    IS NULL THEN 1 ELSE 0 END) AS null_product_code,

    -- Dates
    SUM(CASE WHEN transaction_date IS NULL THEN 1 ELSE 0 END) AS null_transaction_date,

    -- Amounts
    SUM(CASE WHEN gross_revenue IS NULL THEN 1 ELSE 0 END) AS null_gross_revenue,
    SUM(CASE WHEN net_revenue   IS NULL THEN 1 ELSE 0 END) AS null_net_revenue,
    SUM(CASE WHEN tax_amount    IS NULL THEN 1 ELSE 0 END) AS null_tax_amount,

    -- Completeness % for critical fields
    ROUND(
        100.0 * SUM(CASE WHEN transaction_id IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2
    )                                               AS pct_complete_transaction_id,

    ROUND(
        100.0 * SUM(CASE WHEN gross_revenue IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2
    )                                               AS pct_complete_gross_revenue

FROM sales_transactions;


-- ── SECTION 3: DUPLICATE DETECTION ──────────────────────────────────────────
-- Duplicates inflate revenue totals and are a common audit finding.
-- Flag any transaction_id appearing more than once.

SELECT
    transaction_id,
    COUNT(*)            AS occurrence_count,
    SUM(gross_revenue)  AS total_duplicated_revenue,
    MIN(transaction_date) AS first_seen,
    MAX(transaction_date) AS last_seen
FROM sales_transactions
GROUP BY transaction_id
HAVING COUNT(*) > 1
ORDER BY occurrence_count DESC, total_duplicated_revenue DESC;


-- ── SECTION 4: NEGATIVE & ZERO VALUE DETECTION ──────────────────────────────
-- Negative revenues may be legitimate (refunds/credits) but need to be
-- understood and quantified before reconciliation.

SELECT
    CASE
        WHEN gross_revenue > 0  THEN 'Positive Revenue'
        WHEN gross_revenue = 0  THEN 'Zero Revenue'
        WHEN gross_revenue < 0  THEN 'Negative / Credit'
        ELSE 'NULL'
    END                         AS revenue_category,
    COUNT(*)                    AS transaction_count,
    SUM(gross_revenue)          AS total_value,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_total
FROM sales_transactions
GROUP BY
    CASE
        WHEN gross_revenue > 0  THEN 'Positive Revenue'
        WHEN gross_revenue = 0  THEN 'Zero Revenue'
        WHEN gross_revenue < 0  THEN 'Negative / Credit'
        ELSE 'NULL'
    END
ORDER BY total_value DESC;


-- ── SECTION 5: REVENUE BY PERIOD (PRE-RECONCILIATION BASELINE) ───────────────
-- Establish a monthly revenue baseline per source system.
-- This becomes the reference point for the reconciliation variance analysis.

SELECT
    FORMAT(transaction_date, 'yyyy-MM')     AS period,
    COUNT(*)                                AS transaction_count,
    SUM(gross_revenue)                      AS gross_revenue,
    SUM(net_revenue)                        AS net_revenue,
    SUM(tax_amount)                         AS tax_amount,
    AVG(gross_revenue)                      AS avg_transaction_value
FROM sales_transactions
GROUP BY FORMAT(transaction_date, 'yyyy-MM')
ORDER BY period;


-- ── SECTION 6: DATA QUALITY SUMMARY SCORECARD ───────────────────────────────
-- Single-row summary suitable for reporting to stakeholders.

WITH quality_checks AS (
    SELECT
        COUNT(*)                                                        AS total_rows,
        SUM(CASE WHEN transaction_id IS NULL  THEN 1 ELSE 0 END)       AS null_ids,
        SUM(CASE WHEN gross_revenue  IS NULL  THEN 1 ELSE 0 END)       AS null_amounts,
        SUM(CASE WHEN transaction_date IS NULL THEN 1 ELSE 0 END)      AS null_dates,
        COUNT(DISTINCT transaction_id)                                  AS distinct_ids
    FROM sales_transactions
),
duplicate_check AS (
    SELECT COUNT(*) AS duplicate_count
    FROM (
        SELECT transaction_id
        FROM sales_transactions
        GROUP BY transaction_id
        HAVING COUNT(*) > 1
    ) d
)
SELECT
    q.total_rows,
    q.null_ids,
    q.null_amounts,
    q.null_dates,
    d.duplicate_count,
    q.total_rows - q.distinct_ids                   AS duplicate_rows,
    ROUND(100.0 * q.null_ids     / q.total_rows, 2) AS pct_null_ids,
    ROUND(100.0 * q.null_amounts / q.total_rows, 2) AS pct_null_amounts,
    CASE
        WHEN q.null_ids = 0 AND q.null_amounts = 0 AND d.duplicate_count = 0
            THEN 'PASS — Data quality acceptable for reconciliation'
        WHEN q.null_ids > 0 OR q.null_amounts > 0
            THEN 'FAIL — Critical nulls detected. Investigate before proceeding.'
        WHEN d.duplicate_count > 0
            THEN 'WARNING — Duplicates detected. Review before reconciliation.'
        ELSE 'REVIEW REQUIRED'
    END                                             AS quality_status
FROM quality_checks q
CROSS JOIN duplicate_check d;
