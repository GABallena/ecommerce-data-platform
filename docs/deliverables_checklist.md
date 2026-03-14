# E-Commerce Data Platform — Deliverables Checklist & Gold Layer Design

---

## Part 1: Client Deliverables Checklist

### 1. Data Ingestion Layer

| # | Task | Status |
|---|------|--------|
| 1.1 | Build incremental ingestion from PostgreSQL (orders, order_items, customers, products) | ✅ |
| 1.2 | Build ingestion from payment provider API (→ JSON transactions) | ✅ |
| 1.3 | Build CSV ingest for marketing exports (campaign_performance, email_sends) | ✅ |
| 1.4 | Build ingestion from inventory/warehouse system (inventory_snapshot, purchase_orders) | ✅ |
| 1.5 | Implement schema detection on each source | ✅ |
| 1.6 | Implement ingestion logging (source, row counts, timestamps, status) | ✅ |
| 1.7 | Implement retry logic for transient failures | ✅ |
| 1.8 | Land raw files into partitioned storage (`raw/<source>/<entity>/dt=YYYY-MM-DD/`) | ✅ |
| 1.9 | Create metadata tracking table (ingestion_log) | ✅ |

### 2. Data Warehouse (Bronze → Silver → Gold)

| # | Task | Status |
|---|------|--------|
| 2.1 | Define Bronze (raw) layer — 1-to-1 copies of source tables | ✅ |
| 2.2 | Define Silver (staging) layer — cleaned, typed, deduplicated | ✅ |
| 2.3 | Define Gold (analytics) layer — star schema (facts + dims) | ✅ |
| 2.4 | Build `dim_customers` | ✅ |
| 2.5 | Build `dim_products` | ✅ |
| 2.6 | Build `dim_date` | ✅ |
| 2.7 | Build `fact_orders` | ✅ |
| 2.8 | Build `fact_order_items` | ✅ |
| 2.9 | Build `fact_payments` | ✅ |
| 2.10 | Build `fact_inventory_daily` | ✅ |
| 2.11 | Build `fact_campaign_performance` | ✅ |
| 2.12 | Build `fact_email_engagement` | ✅ |
| 2.13 | Build `fact_purchase_orders` | ✅ |

### 3. Transformation Layer

| # | Task | Status |
|---|------|--------|
| 3.1 | Write SQL models to clean messy data (trim, null handling, type casting) | ✅ |
| 3.2 | Enforce schemas and reject/quarantine bad rows | ✅ |
| 3.3 | Handle duplicate records (e.g. customer 88201/88202 duplicate email) | ✅ |
| 3.4 | Handle inconsistent category values in products | ✅ |
| 3.5 | Handle multi-currency amounts (PHP vs USD) | ✅ |
| 3.6 | Compute `customer_lifetime_value` | ✅ |
| 3.7 | Compute `order_conversion_rate` | ✅ |
| 3.8 | Compute `revenue_by_campaign` | ✅ |
| 3.9 | Compute `inventory_turnover` | ✅ |

### 4. Pipeline Orchestration

| # | Task | Status |
|---|------|--------|
| 4.1 | Define DAG: ingest → clean → build facts → generate metrics | ✅ |
| 4.2 | Schedule daily runs | ✅ |
| 4.3 | Implement failure alerts (email/Slack) | ✅ |
| 4.4 | Implement job dependency management (task ordering) | ✅ |
| 4.5 | Implement retries with exponential back-off | ✅ |

### 5. Data Quality System

| # | Task | Status |
|---|------|--------|
| 5.1 | Null/missing-value checks on required columns | ✅ |
| 5.2 | Schema drift detection (column additions/removals/type changes) | ✅ |
| 5.3 | Duplicate transaction detection (`transaction_id`, `order_id`) | ✅ |
| 5.4 | Referential integrity checks (order → customer, item → product) | ✅ |
| 5.5 | Currency consistency checks (order currency ↔ payment currency) | ✅ |
| 5.6 | Arithmetic checks (subtotal − discount + shipping + tax = total) | ✅ |
| 5.7 | Generate `data_quality_report` per run | ✅ |

### 6. Monitoring & Logging

| # | Task | Status |
|---|------|--------|
| 6.1 | Pipeline run history table/log | ✅ |
| 6.2 | Runtime metrics (duration per task) | ✅ |
| 6.3 | Job failure tracking and alerting | ✅ |
| 6.4 | Ingestion volume tracking (rows ingested per source per day) | ✅ |
| 6.5 | Dashboard or log viewer | ✅ |

### 7. Documentation

| # | Task | Status |
|---|------|--------|
| 7.1 | Architecture diagram (full system) | ✅ |
| 7.2 | Pipeline documentation (ingestion logic, transformation logic, dependencies) | ✅ |
| 7.3 | Data dictionary for every Gold table | ✅ |
| 7.4 | Source-to-target mapping document | ✅ |

### 8. Infrastructure

| # | Task | Status |
|---|------|--------|
| 8.1 | Terraform modules (S3 buckets, IAM roles, compute) | ✅ |
| 8.2 | Docker images (Airflow, dbt, quality checks) | ✅ |
| 8.3 | Config files (connections, variables, environments) | ✅ |
| 8.4 | CI/CD deployment scripts | ✅ |

---

## Part 2: Gold Layer Schema Design

### Design Principles
- **Star schema** optimized for analytical queries
- **Grain** explicitly defined per table
- All monetary amounts stored as `DECIMAL(18,2)`
- All timestamps stored as `TIMESTAMP WITH TIME ZONE`
- Surrogate keys (`_sk`) used for slowly-changing dimensions; natural keys retained
- Multi-currency handled via `currency` column + separate `_usd` converted columns where applicable
- `_loaded_at` audit column on every table

---

### Dimension Tables

#### `dim_customers`
Deduplicates customers by email; tracks latest state.

| Column | Type | Source | Notes |
|--------|------|--------|-------|
| customer_sk | INT (surrogate) | generated | Primary key |
| customer_id | INT | customers.customer_id | Natural key (may map N:1 on email) |
| email | VARCHAR | customers.email | |
| first_name | VARCHAR | customers.first_name | |
| last_name | VARCHAR | customers.last_name | |
| phone | VARCHAR | customers.phone | Nullable |
| country_code | CHAR(2) | customers.country_code | |
| status | VARCHAR | customers.status | |
| marketing_opt_in | BOOLEAN | customers.marketing_opt_in | |
| created_at | TIMESTAMP | customers.created_at | Earliest record for this email |
| is_current | BOOLEAN | derived | SCD Type 2 flag |
| valid_from | TIMESTAMP | derived | |
| valid_to | TIMESTAMP | derived | NULL = current |

**Known issue addressed:** customer_id 88201 and 88202 share the same email (`sam.lee@example.com`). The staging layer will flag these; `dim_customers` will group them under one `customer_sk` using the earliest `customer_id` as canonical, preserving both raw IDs for traceability.

---

#### `dim_products`
One row per product; standardized categories.

| Column | Type | Source | Notes |
|--------|------|--------|-------|
| product_sk | INT (surrogate) | generated | Primary key |
| product_id | INT | products.product_id | Natural key |
| sku | VARCHAR | products.sku | |
| product_name | VARCHAR | products.product_name | |
| category_raw | VARCHAR | products.category | Original messy value |
| category_standardized | VARCHAR | derived | Mapped via lookup (e.g. "bags / urban" → "Bags") |
| brand | VARCHAR | products.brand | |
| unit_cost | DECIMAL(18,2) | products.unit_cost | |
| list_price | DECIMAL(18,2) | products.list_price | |
| gross_margin_pct | DECIMAL(5,2) | derived | `(list_price - unit_cost) / list_price * 100` |
| is_active | BOOLEAN | products.is_active | |
| created_at | TIMESTAMP | products.created_at | |

**Known issue addressed:** Category values are inconsistent across systems ("running shoes", "bags / urban", "apparel", "homewares"). A `category_mapping` seed/lookup table will standardize them into a controlled taxonomy.

---

#### `dim_date`
Pre-generated calendar dimension (covers 2024-01-01 through 2027-12-31).

| Column | Type | Notes |
|--------|------|-------|
| date_key | INT | YYYYMMDD |
| full_date | DATE | |
| day_of_week | INT | 1=Mon … 7=Sun |
| day_name | VARCHAR | |
| week_of_year | INT | ISO week |
| month_num | INT | |
| month_name | VARCHAR | |
| quarter | INT | |
| year | INT | |
| is_weekend | BOOLEAN | |
| fiscal_quarter | INT | Configurable offset |
| fiscal_year | INT | |

---

### Fact Tables

#### `fact_orders`
**Grain:** One row per order.

| Column | Type | Source | Notes |
|--------|------|--------|-------|
| order_sk | INT | generated | Primary key |
| order_id | INT | orders.order_id | Natural key |
| customer_sk | INT → dim_customers | joined via customer_id | FK |
| order_date_key | INT → dim_date | orders.order_date | FK |
| order_status | VARCHAR | orders.order_status | |
| currency | CHAR(3) | orders.currency | |
| subtotal_amount | DECIMAL(18,2) | orders.subtotal_amount | |
| discount_amount | DECIMAL(18,2) | orders.discount_amount | |
| shipping_amount | DECIMAL(18,2) | orders.shipping_amount | |
| tax_amount | DECIMAL(18,2) | orders.tax_amount | |
| total_amount | DECIMAL(18,2) | orders.total_amount | |
| total_amount_usd | DECIMAL(18,2) | derived | Converted via exchange rate (PHP→USD etc.) |
| payment_status | VARCHAR | orders.payment_status | |
| is_cancelled | BOOLEAN | derived | `order_status = 'cancelled'` |
| is_paid | BOOLEAN | derived | `payment_status = 'paid'` |
| updated_at | TIMESTAMP | orders.updated_at | |

---

#### `fact_order_items`
**Grain:** One row per order line item.

| Column | Type | Source | Notes |
|--------|------|--------|-------|
| order_item_sk | INT | generated | Primary key |
| order_item_id | INT | order_items.order_item_id | Natural key |
| order_sk | INT → fact_orders | joined via order_id | FK |
| product_sk | INT → dim_products | joined via product_id | FK |
| quantity | INT | order_items.quantity | |
| unit_price | DECIMAL(18,2) | order_items.unit_price | |
| discount_amount | DECIMAL(18,2) | order_items.discount_amount | |
| line_total | DECIMAL(18,2) | order_items.line_total | |
| unit_cost | DECIMAL(18,2) | dim_products.unit_cost | Snapshot at time of sale |
| gross_profit | DECIMAL(18,2) | derived | `line_total - (unit_cost * quantity)` |

---

#### `fact_payments`
**Grain:** One row per payment transaction.

| Column | Type | Source | Notes |
|--------|------|--------|-------|
| payment_sk | INT | generated | Primary key |
| transaction_id | VARCHAR | payments.transaction_id | Natural key |
| order_sk | INT → fact_orders | joined via order_id | FK |
| customer_sk | INT → dim_customers | joined via customer_id | FK |
| payment_date_key | INT → dim_date | payments.provider_created_at | FK |
| payment_method | VARCHAR | payments.payment_method | card / ewallet |
| transaction_type | VARCHAR | payments.transaction_type | capture / void / authorization |
| status | VARCHAR | payments.status | |
| currency | CHAR(3) | payments.currency | |
| gross_amount | DECIMAL(18,2) | payments.gross_amount | |
| fee_amount | DECIMAL(18,2) | payments.fee_amount | |
| net_amount | DECIMAL(18,2) | payments.net_amount | |
| gross_amount_usd | DECIMAL(18,2) | derived | Converted |
| card_brand | VARCHAR | payments.card_brand | Nullable |
| card_country | CHAR(2) | payments.card_country | Nullable |
| is_refund | BOOLEAN | payments.refund_flag | |
| is_successful | BOOLEAN | derived | `status = 'succeeded'` |

---

#### `fact_inventory_daily`
**Grain:** One row per SKU per warehouse per day.

| Column | Type | Source | Notes |
|--------|------|--------|-------|
| inventory_sk | INT | generated | Primary key |
| snapshot_date_key | INT → dim_date | inventory.snapshot_date | FK |
| product_sk | INT → dim_products | joined via product_id | FK |
| warehouse_id | INT | inventory.warehouse_id | |
| on_hand_qty | INT | inventory.on_hand_qty | |
| reserved_qty | INT | inventory.reserved_qty | |
| available_qty | INT | inventory.available_qty | |
| reorder_point | INT | inventory.reorder_point | |
| is_below_reorder | BOOLEAN | derived | `available_qty < reorder_point` |
| inventory_value | DECIMAL(18,2) | derived | `on_hand_qty * unit_cost` |
| days_of_supply | DECIMAL(8,1) | derived | `available_qty / avg_daily_sales` (30-day rolling) |

---

#### `fact_campaign_performance`
**Grain:** One row per campaign per platform per country per device per day.

| Column | Type | Source | Notes |
|--------|------|--------|-------|
| campaign_perf_sk | INT | generated | Primary key |
| campaign_date_key | INT → dim_date | campaign.campaign_date | FK |
| platform | VARCHAR | campaign.platform | meta / google / tiktok |
| campaign_id | VARCHAR | campaign.campaign_id | |
| campaign_name | VARCHAR | campaign.campaign_name | |
| channel | VARCHAR | campaign.channel | paid_social / paid_search |
| country_code | CHAR(2) | campaign.country_code | |
| device_type | VARCHAR | campaign.device_type | |
| impressions | INT | campaign.impressions | |
| clicks | INT | campaign.clicks | |
| spend | DECIMAL(18,2) | campaign.spend | |
| conversions | INT | campaign.conversions | |
| reported_revenue | DECIMAL(18,2) | campaign.reported_revenue | |
| ctr | DECIMAL(8,4) | derived | `clicks / NULLIF(impressions, 0)` |
| cpc | DECIMAL(18,2) | derived | `spend / NULLIF(clicks, 0)` |
| cpa | DECIMAL(18,2) | derived | `spend / NULLIF(conversions, 0)` |
| roas | DECIMAL(8,2) | derived | `reported_revenue / NULLIF(spend, 0)` |

---

#### `fact_email_engagement`
**Grain:** One row per email send per recipient.

| Column | Type | Source | Notes |
|--------|------|--------|-------|
| email_sk | INT | generated | Primary key |
| send_date_key | INT → dim_date | email.send_date | FK |
| campaign_id | VARCHAR | email.campaign_id | |
| campaign_name | VARCHAR | email.campaign_name | |
| customer_sk | INT → dim_customers | joined via customer_id | FK |
| delivery_status | VARCHAR | email.delivery_status | delivered / bounced |
| is_delivered | BOOLEAN | derived | `delivery_status = 'delivered'` |
| is_opened | BOOLEAN | email.open_flag | |
| is_clicked | BOOLEAN | email.click_flag | |
| is_unsubscribed | BOOLEAN | email.unsubscribe_flag | |

---

#### `fact_purchase_orders`
**Grain:** One row per purchase order line.

| Column | Type | Source | Notes |
|--------|------|--------|-------|
| po_sk | INT | generated | Primary key |
| po_id | INT | po.po_id | Natural key |
| supplier_id | INT | po.supplier_id | |
| product_sk | INT → dim_products | joined via sku | FK |
| created_date_key | INT → dim_date | po.created_at | FK |
| expected_delivery_date | DATE | po.expected_delivery_date | |
| ordered_qty | INT | po.ordered_qty | |
| received_qty | INT | po.received_qty | |
| unit_cost | DECIMAL(18,2) | po.unit_cost | |
| po_total | DECIMAL(18,2) | derived | `ordered_qty * unit_cost` |
| po_status | VARCHAR | po.po_status | open / partial / received |
| fill_rate | DECIMAL(5,2) | derived | `received_qty / NULLIF(ordered_qty, 0)` |
| is_overdue | BOOLEAN | derived | `expected_delivery_date < CURRENT_DATE AND po_status != 'received'` |

---

### Derived Metric Models (built on top of Gold facts)

| Metric Model | Logic | Grain |
|---|---|---|
| `metric_customer_lifetime_value` | `SUM(total_amount_usd)` per `customer_sk` for paid, non-cancelled orders | per customer |
| `metric_order_conversion_rate` | Completed+paid orders / total orders per day | per day |
| `metric_revenue_by_campaign` | Join `fact_orders` → `fact_payments` → `fact_campaign_performance` via attribution window; compare `reported_revenue` vs actual `net_amount` | per campaign per day |
| `metric_inventory_turnover` | `COGS sold in period / avg inventory value` per SKU | per SKU per month |

---

## Part 3: Pipeline Design (DAG)

```
                        ┌─────────────────────────────────────┐
                        │         DAILY TRIGGER (T+1)         │
                        └──────────────┬──────────────────────┘
                                       │
              ┌────────────────────────┼────────────────────────┐
              ▼                        ▼                        ▼
   ┌──────────────────┐   ┌──────────────────┐   ┌──────────────────┐
   │  ingest_postgres  │   │  ingest_payments  │   │  ingest_csv_mktg │
   │ (orders, items,   │   │  (JSON API pull)  │   │ (campaigns,      │
   │  customers, prods)│   │                   │   │  email_sends)    │
   └────────┬─────────┘   └────────┬──────────┘   └────────┬─────────┘
            │                      │                        │
            │    ┌─────────────────┤                        │
            │    │  ┌──────────────────┐                    │
            │    │  │ ingest_inventory  │                    │
            │    │  │ (snapshot + POs)  │                    │
            │    │  └────────┬─────────┘                    │
            │    │           │                              │
            ▼    ▼           ▼                              ▼
   ┌───────────────────────────────────────────────────────────┐
   │                 BRONZE  (raw, append-only)                │
   │     raw/<source>/<entity>/dt=YYYY-MM-DD/file.parquet      │
   └──────────────────────────┬────────────────────────────────┘
                              │
                    ┌─────────▼──────────┐
                    │  schema_validation  │ ← detect drift, quarantine bad rows
                    └─────────┬──────────┘
                              │
                              ▼
   ┌───────────────────────────────────────────────────────────┐
   │              SILVER  (cleaned, typed, deduped)            │
   │  stg_orders · stg_order_items · stg_customers            │
   │  stg_products · stg_payments · stg_campaigns             │
   │  stg_email_sends · stg_inventory · stg_purchase_orders   │
   └──────────────────────────┬────────────────────────────────┘
                              │
                    ┌─────────▼──────────┐
                    │  data_quality_tests │ ← nulls, referential integrity,
                    │                     │   arithmetic, duplicates, currency
                    └─────────┬──────────┘
                              │
                              ▼
   ┌───────────────────────────────────────────────────────────┐
   │                GOLD  (star schema)                        │
   │  dim_customers · dim_products · dim_date                  │
   │  fact_orders · fact_order_items · fact_payments            │
   │  fact_inventory_daily · fact_campaign_performance          │
   │  fact_email_engagement · fact_purchase_orders              │
   └──────────────────────────┬────────────────────────────────┘
                              │
                    ┌─────────▼──────────┐
                    │  gold_quality_tests │ ← row-count, KPI sanity, freshness
                    └─────────┬──────────┘
                              │
                              ▼
   ┌───────────────────────────────────────────────────────────┐
   │             METRICS / MARTS                               │
   │  metric_customer_lifetime_value                           │
   │  metric_order_conversion_rate                             │
   │  metric_revenue_by_campaign                               │
   │  metric_inventory_turnover                                │
   └──────────────────────────┬────────────────────────────────┘
                              │
                              ▼
                    ┌──────────────────┐
                    │  notify_complete  │ → Slack / email
                    └──────────────────┘
```

### Suggested Tech Stack

| Layer | Tool | Why |
|-------|------|-----|
| Ingestion | Python scripts + Airbyte (or Singer taps) | Handles Postgres CDC, API pulls, CSV file watches |
| Storage | S3 (Parquet, partitioned by date) | Cost-effective, scalable raw lake |
| Warehouse | PostgreSQL / DuckDB (dev) → Snowflake or BigQuery (prod) | Matches client's existing Postgres expertise |
| Transforms | **dbt** (SQL models) | Bronze→Silver→Gold; built-in testing, docs, lineage |
| Quality | dbt tests + Great Expectations | Schema, null, uniqueness, custom SQL checks |
| Orchestration | **Apache Airflow** | DAG dependencies, retries, alerting, scheduling |
| Monitoring | Airflow UI + custom `pipeline_runs` table | Run history, durations, failure logs |
| Infra | Terraform + Docker Compose | Reproducible deployments |
| Docs | dbt docs generate + ERD diagrams | Auto-generated data dictionary + lineage |
