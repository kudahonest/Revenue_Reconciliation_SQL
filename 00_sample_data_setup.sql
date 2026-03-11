/* ============================================================
   PROJECT 1: Revenue Reconciliation & Anomaly Detection
   FILE:      00_sample_data_setup.sql
   PURPOSE:   Create and populate sample tables for local
              testing. Run this first before any other script.

   NOTE: This uses synthetic data that mirrors the structure
   of a real retail/financial analytics environment.
   All values are illustrative only.
   ============================================================ */

-- ── CREATE TABLES ────────────────────────────────────────────────────────────

-- Drop if exists (safe re-run)
IF OBJECT_ID('sales_transactions', 'U') IS NOT NULL DROP TABLE sales_transactions;
IF OBJECT_ID('gl_postings',        'U') IS NOT NULL DROP TABLE gl_postings;
IF OBJECT_ID('bank_settlements',   'U') IS NOT NULL DROP TABLE bank_settlements;
IF OBJECT_ID('dim_customers',      'U') IS NOT NULL DROP TABLE dim_customers;
IF OBJECT_ID('dim_products',       'U') IS NOT NULL DROP TABLE dim_products;

-- Source system: sales transactions
CREATE TABLE sales_transactions (
    transaction_id      VARCHAR(20)     NOT NULL,
    customer_id         VARCHAR(10),
    product_code        VARCHAR(10),
    product_category    VARCHAR(50),
    transaction_date    DATE,
    quantity            INT,
    unit_price          DECIMAL(10,2),
    gross_revenue       DECIMAL(12,2),
    net_revenue         DECIMAL(12,2),
    tax_amount          DECIMAL(12,2),
    service_start_date  DATE,
    service_end_date    DATE,
    source_system       VARCHAR(20)     DEFAULT 'SYSTEM_A',
    last_modified_date  DATETIME        DEFAULT GETDATE()
);

-- General ledger postings
CREATE TABLE gl_postings (
    posting_id          VARCHAR(20)     NOT NULL,
    journal_id          VARCHAR(20),
    source_reference    VARCHAR(20),        -- links to transaction_id
    account_type        VARCHAR(20),
    posting_date        DATE,
    debit_amount        DECIMAL(12,2),
    net_amount          DECIMAL(12,2),
    tax_posted          DECIMAL(12,2),
    posted_by           VARCHAR(50)
);

-- Bank settlements
CREATE TABLE bank_settlements (
    settlement_id       VARCHAR(20)     NOT NULL,
    reference_id        VARCHAR(20),        -- links to transaction_id
    settlement_date     DATE,
    settled_amount      DECIMAL(12,2),
    fee_deducted        DECIMAL(12,2),
    payment_method      VARCHAR(20)
);

-- Customer dimension
CREATE TABLE dim_customers (
    customer_id         VARCHAR(10)     NOT NULL,
    customer_name       VARCHAR(100),
    customer_segment    VARCHAR(50),
    region              VARCHAR(50),
    country             VARCHAR(50)
);

-- Product dimension
CREATE TABLE dim_products (
    product_code        VARCHAR(10)     NOT NULL,
    product_name        VARCHAR(100),
    product_category    VARCHAR(50),
    unit_cost           DECIMAL(10,2)
);

-- ── POPULATE SAMPLE DATA ──────────────────────────────────────────────────────

-- Customers
INSERT INTO dim_customers VALUES ('C001','Acme Retail Ltd',       'Enterprise',  'Midlands', 'UK');
INSERT INTO dim_customers VALUES ('C002','Blue Sky Goods',         'Mid-Market',  'London',   'UK');
INSERT INTO dim_customers VALUES ('C003','Northern Supplies Co',   'SME',         'North',    'UK');
INSERT INTO dim_customers VALUES ('C004','Southern Traders Ltd',   'Mid-Market',  'South',    'UK');
INSERT INTO dim_customers VALUES ('C005','Tech Distributors PLC',  'Enterprise',  'London',   'UK');

-- Products
INSERT INTO dim_products VALUES ('P001','Laptop Pro 15',    'Electronics', 650.00);
INSERT INTO dim_products VALUES ('P002','Office Chair Std', 'Furniture',    89.00);
INSERT INTO dim_products VALUES ('P003','Cloud Licence Yr', 'Software',    200.00);
INSERT INTO dim_products VALUES ('P004','Wireless Mouse',   'Electronics',  12.00);
INSERT INTO dim_products VALUES ('P005','Annual Support',   'Services',    500.00);

-- Sales transactions (mix of normal, anomalous, and problem records)
INSERT INTO sales_transactions (transaction_id,customer_id,product_code,product_category,transaction_date,quantity,unit_price,gross_revenue,net_revenue,tax_amount,service_start_date,service_end_date,source_system,last_modified_date)
VALUES
-- Normal matched transactions
('TXN-001','C001','P001','Electronics', '2024-01-05', 2, 1200.00, 2400.00, 2000.00, 400.00, NULL, NULL, 'SYSTEM_A', '2024-01-05 09:00:00'),
('TXN-002','C002','P002','Furniture',   '2024-01-08', 5,  120.00,  600.00,  500.00, 100.00, NULL, NULL, 'SYSTEM_A', '2024-01-08 10:30:00'),
('TXN-003','C003','P003','Software',    '2024-01-10',10,  240.00, 2400.00, 2000.00, 400.00, '2024-01-10', '2024-12-31', 'SYSTEM_A', '2024-01-10 11:00:00'),
('TXN-004','C001','P004','Electronics', '2024-01-15',20,   14.40,  288.00,  240.00,  48.00, NULL, NULL, 'SYSTEM_A', '2024-01-15 14:00:00'),
('TXN-005','C004','P005','Services',    '2024-01-20', 1,  600.00,  600.00,  500.00, 100.00, '2024-01-20', '2025-01-19', 'SYSTEM_A', '2024-01-20 09:15:00'),
('TXN-006','C005','P001','Electronics', '2024-02-03', 5, 1200.00, 6000.00, 5000.00,1000.00, NULL, NULL, 'SYSTEM_A', '2024-02-03 08:00:00'),
('TXN-007','C002','P003','Software',    '2024-02-07', 3,  240.00,  720.00,  600.00, 120.00, '2024-02-07', '2025-02-06', 'SYSTEM_A', '2024-02-07 10:00:00'),
('TXN-008','C003','P002','Furniture',   '2024-02-12', 2,  120.00,  240.00,  200.00,  40.00, NULL, NULL, 'SYSTEM_A', '2024-02-12 13:45:00'),
-- Anomaly: very high value transaction
('TXN-009','C001','P001','Electronics', '2024-02-20',50, 1200.00,60000.00,50000.00,10000.00, NULL, NULL, 'SYSTEM_A', '2024-02-20 09:00:00'),
-- GL variance: amount mismatch
('TXN-010','C004','P004','Electronics', '2024-03-01', 8,   14.40,  115.20,   96.00,  19.20, NULL, NULL, 'SYSTEM_A', '2024-03-01 10:00:00'),
-- Missing in GL
('TXN-011','C005','P005','Services',    '2024-03-05', 1,  600.00,  600.00,  500.00, 100.00, NULL, NULL, 'SYSTEM_A', '2024-03-05 11:00:00'),
-- Duplicate (will be removed in script 02)
('TXN-001','C001','P001','Electronics', '2024-01-05', 2, 1200.00, 2400.00, 2000.00, 400.00, NULL, NULL, 'SYSTEM_A', '2024-01-04 08:00:00'),
-- Missing in bank
('TXN-012','C002','P001','Electronics', '2024-03-10', 1, 1200.00, 1200.00, 1000.00, 200.00, NULL, NULL, 'SYSTEM_A', '2024-03-10 09:00:00'),
-- Deferred revenue (subscription)
('TXN-013','C003','P003','Software',    '2024-03-15', 5,  240.00, 1200.00, 1000.00, 200.00, '2024-03-15', '2025-03-14', 'SYSTEM_A', '2024-03-15 10:00:00');

-- GL postings (deliberate gaps and variances to demonstrate reconciliation)
INSERT INTO gl_postings (posting_id,journal_id,source_reference,account_type,posting_date,debit_amount,net_amount,tax_posted,posted_by)
VALUES
('GL-001','JNL-001','TXN-001','REVENUE','2024-01-05', 2400.00, 2000.00, 400.00, 'auto_post'),
('GL-002','JNL-002','TXN-002','REVENUE','2024-01-08',  600.00,  500.00, 100.00, 'auto_post'),
('GL-003','JNL-003','TXN-003','REVENUE','2024-01-10', 2400.00, 2000.00, 400.00, 'auto_post'),
('GL-004','JNL-004','TXN-004','REVENUE','2024-01-15',  288.00,  240.00,  48.00, 'auto_post'),
('GL-005','JNL-005','TXN-005','REVENUE','2024-01-20',  600.00,  500.00, 100.00, 'auto_post'),
('GL-006','JNL-006','TXN-006','REVENUE','2024-02-03', 6000.00, 5000.00,1000.00, 'auto_post'),
('GL-007','JNL-007','TXN-007','REVENUE','2024-02-07',  720.00,  600.00, 120.00, 'auto_post'),
('GL-008','JNL-008','TXN-008','REVENUE','2024-02-12',  240.00,  200.00,  40.00, 'auto_post'),
('GL-009','JNL-009','TXN-009','REVENUE','2024-02-20',60000.00,50000.00,10000.00,'auto_post'),
-- TXN-010: GL variance — posted wrong amount
('GL-010','JNL-010','TXN-010','REVENUE','2024-03-01',  100.00,   83.33,  16.67, 'manual'),
-- TXN-011: MISSING — not posted to GL (error)
-- TXN-012: posted but not settled in bank
('GL-012','JNL-012','TXN-012','REVENUE','2024-03-10', 1200.00, 1000.00, 200.00, 'auto_post'),
('GL-013','JNL-013','TXN-013','REVENUE','2024-03-15', 1200.00, 1000.00, 200.00, 'auto_post');

-- Bank settlements
INSERT INTO bank_settlements (settlement_id,reference_id,settlement_date,settled_amount,fee_deducted,payment_method)
VALUES
('BNK-001','TXN-001','2024-01-07', 2000.00, 20.00, 'CARD'),
('BNK-002','TXN-002','2024-01-10',  500.00,  5.00, 'BACS'),
('BNK-003','TXN-003','2024-01-12', 2000.00, 20.00, 'CARD'),
('BNK-004','TXN-004','2024-01-17',  240.00,  2.40, 'CARD'),
('BNK-005','TXN-005','2024-01-22',  500.00,  5.00, 'BACS'),
('BNK-006','TXN-006','2024-02-05', 5000.00, 50.00, 'BACS'),
('BNK-007','TXN-007','2024-02-09',  600.00,  6.00, 'CARD'),
('BNK-008','TXN-008','2024-02-14',  200.00,  2.00, 'CARD'),
('BNK-009','TXN-009','2024-02-22',50000.00,500.00, 'BACS'),
('BNK-010','TXN-010','2024-03-03',   96.00,  0.96, 'CARD'),
('BNK-011','TXN-011','2024-03-07',  500.00,  5.00, 'CARD'),
-- TXN-012: MISSING from bank — not settled
('BNK-013','TXN-013','2024-03-17', 1000.00, 10.00, 'BACS');

SELECT 'Setup complete. Tables created and populated.' AS status;
SELECT 'sales_transactions' AS tbl, COUNT(*) AS rows FROM sales_transactions UNION ALL
SELECT 'gl_postings',                                   COUNT(*) FROM gl_postings UNION ALL
SELECT 'bank_settlements',                              COUNT(*) FROM bank_settlements;
