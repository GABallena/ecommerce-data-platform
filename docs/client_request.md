# Client Request: E-Commerce Data Platform

## Context

Mid-sized e-commerce platform with data spread across multiple operational systems. Reporting is manual — analysts download CSVs and build Excel reports. The goal is a proper layered data platform with a trusted gold layer that the analytics team can use without reconciling raw data.

## Data Sources

### 1. PostgreSQL — Transactional Database

| Table | Key Columns | Notes |
|-------|------------|-------|
| `customers` | `customer_id (PK)`, `email`, `first_name`, `last_name`, `phone`, `created_at`, `country_code`, `status`, `marketing_opt_in` | Email not unique in historical rows |
| `orders` | `order_id (PK)`, `customer_id (FK)`, `order_date`, `order_status`, `currency`, `subtotal_amount`, `discount_amount`, `shipping_amount`, `tax_amount`, `total_amount`, `payment_status`, `updated_at` | `total_amount` sometimes mismatches line-item sums on old rows |
| `order_items` | `order_item_id (PK)`, `order_id (FK)`, `product_id`, `sku`, `product_name`, `quantity`, `unit_price`, `discount_amount`, `line_total` | |
| `products` | `product_id (PK)`, `sku`, `product_name`, `category`, `brand`, `unit_cost`, `list_price`, `is_active`, `created_at` | `category` is free-text and inconsistent |

### 2. Payment Provider API

JSON payloads pulled from an external Stripe-like API.

| Field | Type | Notes |
|-------|------|-------|
| `transaction_id (PK)` | TEXT | |
| `order_id` | BIGINT | Some orders have multiple transactions |
| `customer_id` | BIGINT | |
| `payment_method` | TEXT | |
| `transaction_type` | TEXT | authorization, capture, refund |
| `status` | TEXT | succeeded, failed, pending |
| `currency` | TEXT | |
| `gross_amount` | NUMERIC(12,2) | |
| `fee_amount` | NUMERIC(12,2) | |
| `net_amount` | NUMERIC(12,2) | |
| `provider_created_at` | TIMESTAMP | |
| `card_brand`, `card_country` | TEXT | |
| `failure_reason` | TEXT | nullable |
| `refund_flag` | BOOLEAN | Newer data uses separate transactions for refunds; older data uses this flag |

### 3. Marketing CSV Exports

Manually downloaded files from email and ad platforms.

**campaign_performance** — `campaign_date`, `platform`, `campaign_id`, `campaign_name`, `channel`, `impressions`, `clicks`, `spend`, `conversions`, `reported_revenue`, `country_code`, `device_type`

**email_sends** — `send_date`, `campaign_id`, `campaign_name`, `recipient_email`, `customer_id`, `delivery_status`, `open_flag`, `click_flag`, `unsubscribe_flag`

> `campaign_id` not always consistent across platforms. `reported_revenue` does not reconcile perfectly with order revenue.

### 4. Inventory System (SQL Server, read-only)

**inventory_snapshot** — `snapshot_date`, `warehouse_id`, `sku`, `product_id`, `on_hand_qty`, `reserved_qty`, `available_qty`, `reorder_point`, `unit_cost`

**purchase_orders** — `po_id (PK)`, `supplier_id`, `sku`, `ordered_qty`, `received_qty`, `unit_cost`, `po_status`, `created_at`, `expected_delivery_date`

> SKU is more reliable than `product_id` for inventory-side joins. Snapshots are daily but some warehouses miss a day.

## Architecture Requirements

### Bronze Layer (Raw Landing)

Store data exactly as it arrives. Allow reprocessing and preserve source fidelity. Only add ingestion timestamp and file metadata.

### Silver Layer (Cleaned + Normalized)

Clean messy data, standardize schemas, deduplicate, enforce business rules.

Handle: duplicate customer emails, inconsistent product categories, currency normalization, transaction lifecycle, invalid order states, schema changes.

### Gold Layer (Analytics-Ready)

The business-facing dataset. Analysts should only use this layer.

**Fact tables:** `fact_orders`, `fact_order_items`, `fact_payments`, `fact_inventory_daily`, `fact_campaign_performance`, `fact_customer_activity_daily`

**Dimension tables:** `dim_customers`, `dim_products`, `dim_date`

## Business Questions the Gold Layer Must Support

| Domain | Questions |
|--------|-----------|
| Revenue | Daily revenue, revenue by product / campaign / country |
| Customer | Lifetime value, cohort retention, acquisition channel performance |
| Marketing | Campaign ROI, conversion rate by channel, paid revenue vs reported conversions |
| Operations | Stockout risk, inventory velocity, supplier lead-time issues |
| Finance | Net revenue after fees, refunds, payment success/failure rates |

## Data Quality Requirements

- **Integrity:** `order_id` and `transaction_id` uniqueness
- **Reconciliation:** `order_total ≈ sum(order_items) + tax + shipping − discount`
- **Referential integrity:** `orders.customer_id` exists in `customers`; `order_items.order_id` exists in `orders`
- **Freshness:** daily ingestion completed

## Pipeline Requirements

- Run automatically on a schedule
- Support retries with backoff
- Log failures
- Allow backfills
- Job dependency management (DAG)

## Known Data Quirks

1. Duplicate customer identity by email
2. `orders.total_amount` may not reconcile to line-item sums
3. Pending vs paid order ambiguity
4. Mixed currencies
5. Weak attribution between marketing conversions and actual paid orders
6. SKU more reliable than `product_id` for inventory joins
7. Product category standardization issues
8. Bounced emails with customer still present
9. Multiple transactions per order lifecycle

## Deliverables

1. Layered architecture (Bronze / Silver / Gold)
2. Data quality findings and automated checks
3. Join strategy across source systems
4. Initial gold model with fact and dimension tables
5. Pipeline with orchestration, retries, and alerting
6. Documentation: architecture diagram, data dictionary, business rules
7. Infrastructure: reproducible deployment (Terraform, Docker, CI/CD)

## Definition of Success

- Data ingests automatically
- Pipelines run daily
- Gold layer is stable and trusted
- Analysts stop manually joining raw tables
- Metrics are consistent across teams

## Evaluation Criteria

| Area | What Is Evaluated |
|------|-------------------|
| Architecture | Logical layered design |
| Data modeling | Clean fact/dimension structure |
| Data quality | Catching inconsistencies proactively |
| Practicality | Not overengineered, fast MVP approach |
| Reliability | Failure handling and retries |
| Automation | Minimal manual steps |
| Documentation | Understandable system |
