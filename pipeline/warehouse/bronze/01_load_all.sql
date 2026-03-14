-- ============================================================
-- BRONZE LAYER: 1-to-1 raw load from Parquet into DuckDB
-- No transformations — exact copy of source data
-- ============================================================

-- postgres sources
CREATE OR REPLACE TABLE bronze.orders AS
SELECT * FROM read_parquet('{{RAW_ROOT}}/postgres/orders/dt={{PARTITION_DATE}}/data.parquet');

CREATE OR REPLACE TABLE bronze.order_items AS
SELECT * FROM read_parquet('{{RAW_ROOT}}/postgres/order_items/dt={{PARTITION_DATE}}/data.parquet');

CREATE OR REPLACE TABLE bronze.customers AS
SELECT * FROM read_parquet('{{RAW_ROOT}}/postgres/customers/dt={{PARTITION_DATE}}/data.parquet');

CREATE OR REPLACE TABLE bronze.products AS
SELECT * FROM read_parquet('{{RAW_ROOT}}/postgres/products/dt={{PARTITION_DATE}}/data.parquet');

-- payments source
CREATE OR REPLACE TABLE bronze.payment_transactions AS
SELECT * FROM read_parquet('{{RAW_ROOT}}/payments/transactions/dt={{PARTITION_DATE}}/data.parquet');

-- marketing sources
CREATE OR REPLACE TABLE bronze.campaign_performance AS
SELECT * FROM read_parquet('{{RAW_ROOT}}/marketing/campaign_performance/dt={{PARTITION_DATE}}/data.parquet');

CREATE OR REPLACE TABLE bronze.email_sends AS
SELECT * FROM read_parquet('{{RAW_ROOT}}/marketing/email_sends/dt={{PARTITION_DATE}}/data.parquet');

-- inventory sources
CREATE OR REPLACE TABLE bronze.inventory_snapshot AS
SELECT * FROM read_parquet('{{RAW_ROOT}}/inventory/inventory_snapshot/dt={{PARTITION_DATE}}/data.parquet');

CREATE OR REPLACE TABLE bronze.purchase_orders AS
SELECT * FROM read_parquet('{{RAW_ROOT}}/inventory/purchase_orders/dt={{PARTITION_DATE}}/data.parquet');
