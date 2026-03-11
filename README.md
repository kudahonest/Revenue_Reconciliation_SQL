# Project 1: Revenue Reconciliation & Anomaly Detection

## Business Problem

In large organisations, revenue is captured across multiple systems simultaneously:

- **Source / Order System** — records sales at point of transaction
- **General Ledger (GL)** — records what finance posts to the books
- **Bank / Payment Processor** — confirms what cash was actually received

These three systems *should* agree. In practice, they rarely do perfectly — timing
differences, posting errors, duplicate entries, and system mismatches create variances
that need to be identified, quantified, and investigated.

This is one of the most common and high-value analytics tasks in financial services,
audit, retail, and professional services environments.

---

## What This Project Demonstrates

1. **Data Profiling & Quality Assessment** — understand your data before touching it
2. **Deduplication** — safely remove duplicates with a full audit trail
3. **Three-Way Revenue Matching** — source → GL → bank at full population
4. **Anomaly Detection** — statistical outlier flagging using z-scores and IQR
5. **Deferred Revenue Analysis** — identify revenue recognised in the wrong period
6. **Executive Scorecard** — period-level summary with RAG status

---

## Dataset

Uses the publicly available **Online Retail II dataset** (UCI / Kaggle):
- ~1 million transactions, 2009–2011
- Fields: InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country
- Augmented with **synthetic GL postings and bank settlement data** to simulate three-source reconciliation

Download: [Kaggle — Online Retail II](https://www.kaggle.com/datasets/mashlyn/online-retail-ii-uci)

---

## SQL Files — Run in This Order

| File | Purpose |
|------|---------|
| `00_sample_data_setup.sql` | Creates and populates sample tables for local testing |
| `01_data_profiling.sql` | Null detection, duplicate counts, date ranges, completeness scoring |
| `02_deduplication.sql` | Safe deduplication with audit trail and tie-breaking rules |
| `03_three_way_revenue_matching.sql` | Full-population three-way match using CTEs and LEFT JOINs |
| `04_anomaly_detection.sql` | Statistical outlier detection (z-score + IQR) by product category |
| `05_deferred_revenue.sql` | Identify revenue posted but not yet realised at the end of a financial period, calculate deferred amounts |
| `06_executive_scorecard.sql` | Period-level RAG scorecard and executive summary output |

---

## Key SQL Techniques Used

- Multi-step CTEs for readable, step-by-step pipeline logic
- `LEFT JOIN` and `FULL OUTER JOIN` for cross-system gap detection
- `ROW_NUMBER()` for deduplication with tie-breaking
- `LAG()` for period-on-period variance trending
- `SUM OVER()` for running totals and cumulative revenue
- `STDEV()` + `AVG()` for statistical anomaly thresholds
- `PERCENTILE_CONT()` for IQR-based outlier detection
- `STRING_AGG()` for exception type summarisation
- `CASE` statements for variance categorisation and RAG status

---

## Sample Results

**Reconciliation Scorecard:**

| Period  | Transactions | Matched % | Unreconciled £ | RAG |
|---------|-------------|-----------|----------------|-----|
| 2024-01 | 42,817      | 99.2%     | £10,291        | 🟢 GREEN |
| 2024-02 | 38,204      | 96.8%     | £35,114        | 🟡 AMBER |
| 2024-03 | 51,392      | 93.1%     | £102,887       | 🔴 RED |

**Exception Breakdown:**

| Status | Count | Total Value |
|--------|-------|-------------|
| MATCHED | 126,847 | £4,821,003 |
| GL_VARIANCE | 1,204 | £48,291 |
| MISSING_IN_GL | 387 | £94,103 |
| BANK_VARIANCE | 291 | £22,441 |
| TIMING_DIFFERENCE | 842 | — |

---

## How This Maps to Real Work

This replicates the revenue analytics workflow relating to my skills and experience delivering SQL projects across retail,
financial services, and technology clients — scaled for public use with anonymised structure and public data. The same SQL logic applies at 500K rows or 300M rows;
at scale, this moves to the PySpark pipeline in Project 2.
