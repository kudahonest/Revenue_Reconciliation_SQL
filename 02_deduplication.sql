/* ============================================================
   PROJECT 1: Revenue Reconciliation & Anomaly Detection
   FILE:      02_deduplication.sql
   PURPOSE:   Safely remove duplicate records before
              reconciliation. Always run BEFORE matching.

   APPROACH:
   - Identify exact duplicates (same transaction_id)
   - Apply tie-breaking rules to decide which row to keep
   - Log all removed records for audit trail
   - Produce a clean, deduplicated working dataset

   BUSINESS CONTEXT:
   Source systems often create duplicates through:
   - Retry logic when a transaction fails mid-process
   - Manual re-entry by staff
   - ETL pipeline runs that overlap
   Deduplication must be explicit and auditable.
   ============================================================ */


-- ── STEP 1: IDENTIFY DUPLICATES ───────────────────────────────────────────────
-- How many transaction_ids appear more than once?

SELECT
    transaction_id,
    COUNT(*)                        AS occurrence_count,
    MIN(last_modified_date)         AS first_seen,
    MAX(last_modified_date)         AS last_seen,
    COUNT(DISTINCT net_revenue)     AS distinct_amounts,
    SUM(net_revenue)                AS total_duplicated_value
FROM sales_transactions
GROUP BY transaction_id
HAVING COUNT(*) > 1
ORDER BY occurrence_count DESC, total_duplicated_value DESC;


-- ── STEP 2: PREVIEW WHICH ROWS WILL BE KEPT VS REMOVED ───────────────────────
-- Before deleting anything, show exactly what deduplication will do.
-- KEEP = row_number = 1 | REMOVE = row_number > 1

WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY transaction_id
            ORDER BY
                -- Tie-breaking rule 1: prefer rows with a GL reference (posted records)
                CASE WHEN EXISTS (
                    SELECT 1 FROM gl_postings g
                    WHERE g.source_reference = sales_transactions.transaction_id
                ) THEN 0 ELSE 1 END ASC,
                -- Tie-breaking rule 2: prefer most recently modified
                last_modified_date DESC,
                -- Tie-breaking rule 3: prefer source system A
                CASE source_system WHEN 'SYSTEM_A' THEN 0 ELSE 1 END ASC
        ) AS rn,
        COUNT(*) OVER (PARTITION BY transaction_id) AS total_copies
    FROM sales_transactions
)
SELECT
    transaction_id,
    customer_id,
    transaction_date,
    net_revenue,
    source_system,
    last_modified_date,
    total_copies,
    CASE WHEN rn = 1 THEN 'KEEP' ELSE 'REMOVE' END AS dedup_action,
    rn AS row_number
FROM ranked
WHERE total_copies > 1
ORDER BY transaction_id, rn;


-- ── STEP 3: AUDIT LOG — RECORD WHAT IS BEING REMOVED ─────────────────────────
-- Best practice: never silently discard records.
-- In production, INSERT this into a permanent audit table.

WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY transaction_id
            ORDER BY last_modified_date DESC
        ) AS rn,
        COUNT(*) OVER (PARTITION BY transaction_id) AS total_copies
    FROM sales_transactions
)
SELECT
    GETDATE()               AS removed_at,
    transaction_id,
    customer_id,
    transaction_date,
    net_revenue,
    source_system,
    last_modified_date,
    total_copies,
    'REMOVED AS DUPLICATE'  AS removal_reason,
    'Revenue Reconciliation Project' AS removed_by_process
FROM ranked
WHERE rn > 1
ORDER BY transaction_id;
-- In production: INSERT INTO dedup_audit_log SELECT ...


-- ── STEP 4: CREATE CLEAN DEDUPLICATED VIEW ────────────────────────────────────
-- Use this view as the input to all downstream reconciliation scripts.
-- Using a view (not DELETE) preserves the raw data.

CREATE OR ALTER VIEW vw_sales_deduplicated AS
WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY transaction_id
            ORDER BY last_modified_date DESC
        ) AS rn
    FROM sales_transactions
)
SELECT
    transaction_id,
    customer_id,
    product_code,
    product_category,
    transaction_date,
    quantity,
    unit_price,
    gross_revenue,
    net_revenue,
    tax_amount,
    service_start_date,
    service_end_date,
    source_system,
    last_modified_date
FROM ranked
WHERE rn = 1;


-- ── STEP 5: VALIDATION — CONFIRM DEDUPLICATION RESULT ─────────────────────────

SELECT
    'Before dedup' AS stage,
    COUNT(*)       AS total_rows,
    COUNT(DISTINCT transaction_id) AS distinct_ids
FROM sales_transactions

UNION ALL

SELECT
    'After dedup',
    COUNT(*),
    COUNT(DISTINCT transaction_id)
FROM vw_sales_deduplicated;
