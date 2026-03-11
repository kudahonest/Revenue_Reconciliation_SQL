/* ============================================================
   PROJECT 1: Revenue Reconciliation & Anomaly Detection
   FILE:      06_executive_scorecard.sql
   PURPOSE:   Produce the final management-facing outputs:
              period scorecard, trend analysis, and
              one-page executive summary.

   AUDIENCE:  Finance Director, Audit Manager, CFO
   FORMAT:    Designed to feed directly into Power BI
              or a management report slide
   ============================================================ */


-- ── OUTPUT 1: PERIOD-LEVEL RAG SCORECARD ─────────────────────────────────────

WITH recon AS (
    SELECT * FROM vw_three_way_reconciliation   -- output of script 03
),
period_summary AS (
    SELECT
        FORMAT(transaction_date, 'yyyy-MM')         AS period,
        COUNT(*)                                    AS total_transactions,
        SUM(source_net)                             AS total_revenue_source,
        SUM(CASE WHEN reconciliation_status = 'MATCHED'
                 THEN source_net ELSE 0 END)        AS matched_revenue,
        SUM(CASE WHEN reconciliation_status != 'MATCHED'
                 THEN ABS(source_vs_gl_variance) ELSE 0 END) AS unreconciled_variance,
        SUM(CASE WHEN reconciliation_status = 'MISSING_IN_GL'
                 THEN source_net ELSE 0 END)        AS missing_in_gl,
        SUM(CASE WHEN reconciliation_status = 'GL_VARIANCE'
                 THEN ABS(source_vs_gl_variance) ELSE 0 END) AS gl_variance,
        SUM(CASE WHEN reconciliation_status = 'BANK_VARIANCE'
                 THEN ABS(gl_vs_bank_variance) ELSE 0 END)   AS bank_variance,
        COUNT(CASE WHEN reconciliation_status != 'MATCHED'
                   THEN 1 END)                      AS exception_count
    FROM recon
    GROUP BY FORMAT(transaction_date, 'yyyy-MM')
)
SELECT
    period,
    total_transactions,
    ROUND(total_revenue_source, 2)          AS total_revenue,
    ROUND(matched_revenue, 2)              AS matched_revenue,
    ROUND(unreconciled_variance, 2)        AS unreconciled_variance,
    ROUND(missing_in_gl, 2)               AS missing_in_gl,
    ROUND(gl_variance, 2)                  AS gl_variance,
    ROUND(bank_variance, 2)                AS bank_variance,
    exception_count,
    ROUND(
        100.0 * matched_revenue / NULLIF(total_revenue_source, 0), 2
    )                                       AS reconciliation_rate_pct,

    -- Period-on-period change in unreconciled amount
    ROUND(
        unreconciled_variance -
        LAG(unreconciled_variance) OVER (ORDER BY period),
    2)                                      AS variance_change_vs_prior,

    -- RAG Status
    CASE
        WHEN 100.0 * matched_revenue / NULLIF(total_revenue_source,0) >= 99.0
            THEN 'GREEN'
        WHEN 100.0 * matched_revenue / NULLIF(total_revenue_source,0) >= 95.0
            THEN 'AMBER'
        ELSE 'RED'
    END                                     AS rag_status,

    -- Exception rate
    ROUND(100.0 * exception_count / NULLIF(total_transactions, 0), 2)
                                            AS exception_rate_pct

FROM period_summary
ORDER BY period;


-- ── OUTPUT 2: TOP 10 EXCEPTIONS BY VALUE ─────────────────────────────────────

SELECT TOP 10
    transaction_id,
    customer_id,
    product_category,
    transaction_date,
    ROUND(source_net, 2)                    AS source_amount,
    ROUND(gl_net_amount, 2)                AS gl_amount,
    ROUND(bank_settled_amount, 2)          AS bank_amount,
    ROUND(ABS(source_vs_gl_variance), 2)   AS variance_amount,
    reconciliation_status,
    CASE
        WHEN ABS(source_vs_gl_variance) > 10000 THEN 'HIGH'
        WHEN ABS(source_vs_gl_variance) > 1000  THEN 'MEDIUM'
        ELSE 'LOW'
    END                                     AS priority
FROM vw_three_way_reconciliation
WHERE reconciliation_status != 'MATCHED'
ORDER BY ABS(source_vs_gl_variance) DESC;


-- ── OUTPUT 3: ONE-LINE EXECUTIVE SUMMARY ─────────────────────────────────────

SELECT
    FORMAT(MIN(transaction_date), 'dd MMM yyyy') AS period_start,
    FORMAT(MAX(transaction_date), 'dd MMM yyyy') AS period_end,
    FORMAT(COUNT(*), 'N0')                        AS transactions_tested,
    '£' + FORMAT(SUM(source_net), 'N2')           AS total_revenue_tested,
    FORMAT(
        ROUND(100.0 * SUM(CASE WHEN reconciliation_status = 'MATCHED'
                               THEN source_net ELSE 0 END)
              / NULLIF(SUM(source_net), 0), 2), 'N2') + '%'
                                                  AS reconciliation_rate,
    '£' + FORMAT(
        SUM(CASE WHEN reconciliation_status != 'MATCHED'
                 THEN ABS(source_vs_gl_variance) ELSE 0 END), 'N2')
                                                  AS total_unreconciled,
    FORMAT(SUM(CASE WHEN reconciliation_status != 'MATCHED'
                    THEN 1 ELSE 0 END), 'N0')     AS exception_count
FROM vw_three_way_reconciliation;
