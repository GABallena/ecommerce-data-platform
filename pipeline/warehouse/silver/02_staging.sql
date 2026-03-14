-- ============================================================
-- SILVER LAYER: Cleaned, typed, deduplicated staging tables
-- ============================================================

-- ---------------------------------------------------------
-- stg_customers: trim strings, handle duplicates, type casts
-- ---------------------------------------------------------
CREATE OR REPLACE TABLE silver.stg_customers AS
WITH deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY created_at DESC
        ) AS _row_num
    FROM bronze.customers
)
SELECT
    CAST(customer_id AS INTEGER)        AS customer_id,
    TRIM(LOWER(CAST(email AS VARCHAR)))  AS email,
    TRIM(CAST(first_name AS VARCHAR))   AS first_name,
    TRIM(CAST(last_name AS VARCHAR))    AS last_name,
    NULLIF(TRIM(REGEXP_REPLACE(CAST(phone AS VARCHAR), '\.0$', '')), '') AS phone,
    CAST(created_at AS TIMESTAMP)       AS created_at,
    UPPER(TRIM(CAST(country_code AS VARCHAR))) AS country_code,
    LOWER(TRIM(CAST(status AS VARCHAR)))       AS status,
    CAST(marketing_opt_in AS BOOLEAN)   AS marketing_opt_in,
    -- Flag duplicate emails
    CASE WHEN COUNT(*) OVER (PARTITION BY TRIM(LOWER(CAST(email AS VARCHAR)))) > 1
         THEN TRUE ELSE FALSE
    END                                 AS is_duplicate_email
FROM deduplicated
WHERE _row_num = 1;

-- ---------------------------------------------------------
-- stg_products: standardize categories, compute margin
-- ---------------------------------------------------------
CREATE OR REPLACE TABLE silver.stg_products AS
SELECT
    CAST(p.product_id AS INTEGER)       AS product_id,
    TRIM(CAST(p.sku AS VARCHAR))         AS sku,
    TRIM(CAST(p.product_name AS VARCHAR)) AS product_name,
    LOWER(TRIM(CAST(p.category AS VARCHAR))) AS category_raw,
    COALESCE(cm.category_standardized, 'Uncategorized')
                                        AS category_standardized,
    TRIM(CAST(p.brand AS VARCHAR))       AS brand,
    CAST(p.unit_cost AS DECIMAL(18,2))  AS unit_cost,
    CAST(p.list_price AS DECIMAL(18,2)) AS list_price,
    ROUND((p.list_price - p.unit_cost) / NULLIF(p.list_price, 0) * 100, 2)
                                        AS gross_margin_pct,
    CAST(p.is_active AS BOOLEAN)        AS is_active,
    CAST(p.created_at AS TIMESTAMP)     AS created_at
FROM bronze.products p
LEFT JOIN silver.seed_category_mapping cm
    ON LOWER(TRIM(CAST(p.category AS VARCHAR))) = cm.category_raw;

-- ---------------------------------------------------------
-- stg_orders: type cast, validate totals
-- ---------------------------------------------------------
CREATE OR REPLACE TABLE silver.stg_orders AS
SELECT
    CAST(order_id AS INTEGER)                   AS order_id,
    CAST(customer_id AS INTEGER)                AS customer_id,
    CAST(order_date AS TIMESTAMP)               AS order_date,
    LOWER(TRIM(CAST(order_status AS VARCHAR)))   AS order_status,
    UPPER(TRIM(CAST(currency AS VARCHAR)))       AS currency,
    CAST(subtotal_amount AS DECIMAL(18,2))      AS subtotal_amount,
    CAST(discount_amount AS DECIMAL(18,2))      AS discount_amount,
    CAST(shipping_amount AS DECIMAL(18,2))      AS shipping_amount,
    CAST(tax_amount AS DECIMAL(18,2))           AS tax_amount,
    CAST(total_amount AS DECIMAL(18,2))         AS total_amount,
    -- Arithmetic validation flag
    ABS(
        (subtotal_amount - discount_amount + shipping_amount + tax_amount)
        - total_amount
    ) > 0.01                                    AS has_total_mismatch,
    LOWER(TRIM(CAST(payment_status AS VARCHAR))) AS payment_status,
    CAST(shipping_address_id AS INTEGER)        AS shipping_address_id,
    CAST(billing_address_id AS INTEGER)         AS billing_address_id,
    CAST(updated_at AS TIMESTAMP)               AS updated_at
FROM bronze.orders;

-- ---------------------------------------------------------
-- stg_order_items: type cast, validate line_total
-- ---------------------------------------------------------
CREATE OR REPLACE TABLE silver.stg_order_items AS
SELECT
    CAST(order_item_id AS INTEGER)              AS order_item_id,
    CAST(order_id AS INTEGER)                   AS order_id,
    CAST(product_id AS INTEGER)                 AS product_id,
    TRIM(CAST(sku AS VARCHAR))                   AS sku,
    TRIM(CAST(product_name AS VARCHAR))          AS product_name,
    CAST(quantity AS INTEGER)                    AS quantity,
    CAST(unit_price AS DECIMAL(18,2))           AS unit_price,
    CAST(discount_amount AS DECIMAL(18,2))      AS discount_amount,
    CAST(line_total AS DECIMAL(18,2))           AS line_total,
    ABS((unit_price * quantity - discount_amount) - line_total) > 0.01
                                                AS has_line_total_mismatch
FROM bronze.order_items;

-- ---------------------------------------------------------
-- stg_payment_transactions: type cast, clean nulls
-- ---------------------------------------------------------
CREATE OR REPLACE TABLE silver.stg_payment_transactions AS
SELECT
    TRIM(CAST(transaction_id AS VARCHAR))        AS transaction_id,
    CAST(order_id AS INTEGER)                   AS order_id,
    CAST(customer_id AS INTEGER)                AS customer_id,
    LOWER(TRIM(CAST(payment_method AS VARCHAR))) AS payment_method,
    LOWER(TRIM(CAST(transaction_type AS VARCHAR))) AS transaction_type,
    LOWER(TRIM(CAST(status AS VARCHAR)))         AS status,
    UPPER(TRIM(CAST(currency AS VARCHAR)))       AS currency,
    CAST(gross_amount AS DECIMAL(18,2))         AS gross_amount,
    CAST(fee_amount AS DECIMAL(18,2))           AS fee_amount,
    CAST(net_amount AS DECIMAL(18,2))           AS net_amount,
    CAST(provider_created_at AS TIMESTAMP)      AS provider_created_at,
    LOWER(NULLIF(TRIM(CAST(card_brand AS VARCHAR)), ''))   AS card_brand,
    UPPER(NULLIF(TRIM(CAST(card_country AS VARCHAR)), '')) AS card_country,
    NULLIF(TRIM(CAST(failure_reason AS VARCHAR)), '')      AS failure_reason,
    CAST(refund_flag AS BOOLEAN)                AS refund_flag
FROM bronze.payment_transactions;

-- ---------------------------------------------------------
-- stg_campaign_performance: type cast
-- ---------------------------------------------------------
CREATE OR REPLACE TABLE silver.stg_campaign_performance AS
SELECT
    CAST(campaign_date AS DATE)                 AS campaign_date,
    LOWER(TRIM(CAST(platform AS VARCHAR)))       AS platform,
    TRIM(CAST(campaign_id AS VARCHAR))           AS campaign_id,
    TRIM(CAST(campaign_name AS VARCHAR))         AS campaign_name,
    LOWER(TRIM(CAST(channel AS VARCHAR)))        AS channel,
    CAST(impressions AS INTEGER)                AS impressions,
    CAST(clicks AS INTEGER)                     AS clicks,
    CAST(spend AS DECIMAL(18,2))                AS spend,
    CAST(conversions AS INTEGER)                AS conversions,
    CAST(reported_revenue AS DECIMAL(18,2))     AS reported_revenue,
    UPPER(TRIM(CAST(country_code AS VARCHAR)))   AS country_code,
    LOWER(TRIM(CAST(device_type AS VARCHAR)))    AS device_type,
    TRIM(CAST(load_filename AS VARCHAR))         AS load_filename,
    CAST(loaded_at AS TIMESTAMP)                AS loaded_at
FROM bronze.campaign_performance;

-- ---------------------------------------------------------
-- stg_email_sends: type cast, normalize booleans
-- ---------------------------------------------------------
CREATE OR REPLACE TABLE silver.stg_email_sends AS
SELECT
    CAST(send_date AS DATE)                     AS send_date,
    TRIM(CAST(campaign_id AS VARCHAR))           AS campaign_id,
    TRIM(CAST(campaign_name AS VARCHAR))         AS campaign_name,
    TRIM(LOWER(CAST(recipient_email AS VARCHAR))) AS recipient_email,
    CAST(customer_id AS INTEGER)                AS customer_id,
    LOWER(TRIM(CAST(delivery_status AS VARCHAR))) AS delivery_status,
    CAST(open_flag AS BOOLEAN)                  AS open_flag,
    CAST(click_flag AS BOOLEAN)                 AS click_flag,
    CAST(unsubscribe_flag AS BOOLEAN)           AS unsubscribe_flag,
    TRIM(CAST(load_filename AS VARCHAR))         AS load_filename,
    CAST(loaded_at AS TIMESTAMP)                AS loaded_at
FROM bronze.email_sends;

-- ---------------------------------------------------------
-- stg_inventory_snapshot: type cast
-- ---------------------------------------------------------
CREATE OR REPLACE TABLE silver.stg_inventory_snapshot AS
SELECT
    CAST(snapshot_date AS DATE)                  AS snapshot_date,
    CAST(warehouse_id AS INTEGER)                AS warehouse_id,
    TRIM(CAST(sku AS VARCHAR))                   AS sku,
    CAST(product_id AS INTEGER)                  AS product_id,
    CAST(on_hand_qty AS INTEGER)                 AS on_hand_qty,
    CAST(reserved_qty AS INTEGER)                AS reserved_qty,
    CAST(available_qty AS INTEGER)               AS available_qty,
    CAST(reorder_point AS INTEGER)               AS reorder_point,
    CAST(unit_cost AS DECIMAL(18,2))             AS unit_cost
FROM bronze.inventory_snapshot;

-- ---------------------------------------------------------
-- stg_purchase_orders: type cast, compute fill rate
-- ---------------------------------------------------------
CREATE OR REPLACE TABLE silver.stg_purchase_orders AS
SELECT
    CAST(po_id AS INTEGER)                       AS po_id,
    CAST(supplier_id AS INTEGER)                 AS supplier_id,
    TRIM(CAST(sku AS VARCHAR))                    AS sku,
    CAST(ordered_qty AS INTEGER)                 AS ordered_qty,
    CAST(received_qty AS INTEGER)                AS received_qty,
    CAST(unit_cost AS DECIMAL(18,2))             AS unit_cost,
    LOWER(TRIM(CAST(po_status AS VARCHAR)))       AS po_status,
    CAST(created_at AS TIMESTAMP)                AS created_at,
    CAST(expected_delivery_date AS DATE)          AS expected_delivery_date
FROM bronze.purchase_orders;
