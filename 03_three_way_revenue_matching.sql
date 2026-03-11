/* ============================================================
   PROJECT: Revenue Reconciliation & Anomaly Detection
   FILE:    02_three_way_revenue_matching.sql
   PURPOSE: Match revenue across three systems:
              1. sales_transactions  (source / order system)
              2. gl_postings         (general ledger)
              3. bank_settlements    (bank / payment processor)
   
   BUSINESS CONTEXT:
   In audit and financial analytics, a "three-way match" 
   confirms that what was sold (source), what was posted to 
   the books (GL), and what was received in cash (bank) all 
   agree. Variances between these three systems indicate 
   potential errors, timing differences, or fraud risks.
   
   The output categorises every transaction as:
     MATCHED         — all three systems agree
     GL_VARIANCE     — source vs GL discrepancy
     BANK_VARIANCE   — GL vs bank discrepancy
     MISSING_IN_GL   — in source but not posted to GL
     MISSING_IN_BANK — posted to GL but not settled
     TIMING_DIFF     — amounts match but dates differ
   
   TECHNIQUES USED:
   - CTEs for readable, step-by-step logic
   - LEFT JOINs to surface missing records
   - CASE statements for variance categorisation
   - Window functions for running totals
   - COALESCE for NULL-safe comparisons
   ============================================================ */


-- ── STEP 1: AGGREGATE SOURCE TRANSACTIONS ────────────────────────────────────
-- Some source systems create multiple rows per order (line items).
-- Aggregate to transaction level before matching.

WITH source_aggregated AS (
    SELECT
        transaction_id,
        customer_id,
        transaction_date,
        product_category,
        SUM(gross_revenue)  AS gross_revenue,
        SUM(net_revenue)    AS net_revenue,
        SUM(tax_amount)     AS tax_amount,
        COUNT(*)            AS line_item_count
    FROM sales_transactions
    WHERE transaction_date IS NOT NULL
      AND transaction_id   IS NOT NULL
    GROUP BY
        transaction_id,
        customer_id,
        transaction_date,
        product_category
),

-- ── STEP 2: AGGREGATE GL POSTINGS ────────────────────────────────────────────
-- GL may have multiple journals per transaction (debit/credit pairs).
-- Filter to revenue account types only.

gl_aggregated AS (
    SELECT
        source_reference        AS transaction_id,
        posting_date,
        SUM(net_amount)         AS gl_net_amount,
        SUM(tax_posted)         AS gl_tax_amount,
        SUM(debit_amount)       AS gl_gross_amount,
        COUNT(DISTINCT journal_id) AS journal_count
    FROM gl_postings
    WHERE account_type = 'REVENUE'
      AND source_reference IS NOT NULL
    GROUP BY
        source_reference,
        posting_date
),

-- ── STEP 3: AGGREGATE BANK SETTLEMENTS ───────────────────────────────────────

bank_aggregated AS (
    SELECT
        reference_id            AS transaction_id,
        settlement_date,
        SUM(settled_amount)     AS bank_settled_amount,
        SUM(fee_deducted)       AS bank_fees,
        COUNT(*)                AS settlement_count
    FROM bank_settlements
    WHERE reference_id IS NOT NULL
    GROUP BY
        reference_id,
        settlement_date
),

-- ── STEP 4: JOIN ALL THREE SOURCES ───────────────────────────────────────────
-- Use LEFT JOIN from source to capture records missing in GL or bank.

three_way_join AS (
    SELECT
        s.transaction_id,
        s.customer_id,
        s.transaction_date,
        s.product_category,
        s.gross_revenue                         AS source_gross,
        s.net_revenue                           AS source_net,
        s.tax_amount                            AS source_tax,

        g.gl_gross_amount,
        g.gl_net_amount,
        g.gl_tax_amount,
        g.posting_date,

        b.bank_settled_amount,
        b.bank_fees,
        b.settlement_date,

        -- Variance calculations
        COALESCE(s.net_revenue, 0)
            - COALESCE(g.gl_net_amount, 0)      AS source_vs_gl_variance,

        COALESCE(g.gl_net_amount, 0)
            - COALESCE(b.bank_settled_amount, 0) AS gl_vs_bank_variance,

        COALESCE(s.net_revenue, 0)
            - COALESCE(b.bank_settled_amount, 0) AS source_vs_bank_variance,

        -- Date lag between transaction and GL posting
        DATEDIFF(day, s.transaction_date, g.posting_date) AS days_to_post,

        -- Date lag between GL posting and bank settlement
        DATEDIFF(day, g.posting_date, b.settlement_date)  AS days_to_settle

    FROM source_aggregated      s
    LEFT JOIN gl_aggregated     g ON s.transaction_id = g.transaction_id
    LEFT JOIN bank_aggregated   b ON s.transaction_id = b.transaction_id
),

-- ── STEP 5: CATEGORISE EACH TRANSACTION ──────────────────────────────────────
-- Apply business rules to classify reconciliation status.
-- Tolerance of £0.01 accounts for rounding differences.

categorised AS (
    SELECT
        *,
        CASE
            WHEN gl_gross_amount    IS NULL
                THEN 'MISSING_IN_GL'
            WHEN bank_settled_amount IS NULL
                THEN 'MISSING_IN_BANK'
            WHEN ABS(source_vs_gl_variance)   > 0.01
             AND ABS(gl_vs_bank_variance)      > 0.01
                THEN 'MULTI_SYSTEM_VARIANCE'
            WHEN ABS(source_vs_gl_variance)   > 0.01
                THEN 'GL_VARIANCE'
            WHEN ABS(gl_vs_bank_variance)      > 0.01
                THEN 'BANK_VARIANCE'
            WHEN ABS(source_vs_gl_variance)   <= 0.01
             AND ABS(gl_vs_bank_variance)      <= 0.01
             AND days_to_post                  > 5
                THEN 'TIMING_DIFFERENCE'
            WHEN ABS(source_vs_gl_variance)   <= 0.01
             AND ABS(gl_vs_bank_variance)      <= 0.01
                THEN 'MATCHED'
            ELSE 'REVIEW_REQUIRED'
        END AS reconciliation_status
    FROM three_way_join
)

-- ── FINAL OUTPUT: FULL TRANSACTION-LEVEL RECONCILIATION ──────────────────────

SELECT
    transaction_id,
    customer_id,
    transaction_date,
    product_category,
    source_gross,
    source_net,
    source_tax,
    gl_gross_amount,
    gl_net_amount,
    gl_tax_amount,
    bank_settled_amount,
    bank_fees,
    source_vs_gl_variance,
    gl_vs_bank_variance,
    source_vs_bank_variance,
    days_to_post,
    days_to_settle,
    reconciliation_status,

    -- Running total of unreconciled variance for audit trail
    SUM(ABS(source_vs_gl_variance))
        OVER (ORDER BY transaction_date
              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                                    AS cumulative_gl_variance,

    -- Flag high-value variances for priority review
    CASE
        WHEN ABS(source_vs_gl_variance) > 1000 THEN 'HIGH PRIORITY'
        WHEN ABS(source_vs_gl_variance) > 100  THEN 'MEDIUM PRIORITY'
        WHEN ABS(source_vs_gl_variance) > 0.01 THEN 'LOW PRIORITY'
        ELSE ''
    END                             AS variance_priority_flag

FROM categorised
ORDER BY
    CASE reconciliation_status
        WHEN 'MULTI_SYSTEM_VARIANCE' THEN 1
        WHEN 'GL_VARIANCE'           THEN 2
        WHEN 'BANK_VARIANCE'         THEN 3
        WHEN 'MISSING_IN_GL'         THEN 4
        WHEN 'MISSING_IN_BANK'       THEN 5
        WHEN 'TIMING_DIFFERENCE'     THEN 6
        WHEN 'MATCHED'               THEN 7
        ELSE 8
    END,
    ABS(source_vs_gl_variance) DESC;
