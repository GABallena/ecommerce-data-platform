-- ============================================================
-- GOLD LAYER: Fact tables
-- ============================================================

-- ---------------------------------------------------------
-- fact_orders: One row per order
-- ---------------------------------------------------------
CREATE OR REPLACE TABLE gold.fact_orders AS
SELECT
    ROW_NUMBER() OVER (ORDER BY o.order_id)::INTEGER    AS order_sk,
    o.order_id,
    dc.customer_sk,
    dd.date_key                                          AS order_date_key,
    o.order_status,
    o.currency,
    o.subtotal_amount,
    o.discount_amount,
    o.shipping_amount,
    o.tax_amount,
    o.total_amount,
    ROUND(o.total_amount * COALESCE(fx.rate_to_usd, 1.0), 2)
                                                         AS total_amount_usd,
    o.payment_status,
    o.order_status = 'cancelled'                         AS is_cancelled,
    o.payment_status = 'paid'                            AS is_paid,
    o.has_total_mismatch,
    o.updated_at,
    CURRENT_TIMESTAMP                                    AS _loaded_at
FROM silver.stg_orders o
LEFT JOIN gold.dim_customers dc
    ON o.customer_id = dc.customer_id AND dc.is_current = TRUE
LEFT JOIN gold.dim_date dd
    ON CAST(o.order_date AS DATE) = dd.full_date
LEFT JOIN silver.seed_exchange_rates fx
    ON o.currency = fx.currency;

-- ---------------------------------------------------------
-- fact_order_items: One row per order line item
-- ---------------------------------------------------------
CREATE OR REPLACE TABLE gold.fact_order_items AS
SELECT
    ROW_NUMBER() OVER (ORDER BY oi.order_item_id)::INTEGER AS order_item_sk,
    oi.order_item_id,
    fo.order_sk,
    dp.product_sk,
    oi.quantity,
    oi.unit_price,
    oi.discount_amount,
    oi.line_total,
    dp.unit_cost,
    ROUND(oi.line_total - (dp.unit_cost * oi.quantity), 2) AS gross_profit,
    oi.has_line_total_mismatch,
    CURRENT_TIMESTAMP                                      AS _loaded_at
FROM silver.stg_order_items oi
LEFT JOIN gold.fact_orders fo
    ON oi.order_id = fo.order_id
LEFT JOIN gold.dim_products dp
    ON oi.product_id = dp.product_id;

-- ---------------------------------------------------------
-- fact_payments: One row per payment transaction
-- ---------------------------------------------------------
CREATE OR REPLACE TABLE gold.fact_payments AS
SELECT
    ROW_NUMBER() OVER (ORDER BY pt.transaction_id)::INTEGER AS payment_sk,
    pt.transaction_id,
    fo.order_sk,
    dc.customer_sk,
    dd.date_key                                             AS payment_date_key,
    pt.payment_method,
    pt.transaction_type,
    pt.status,
    pt.currency,
    pt.gross_amount,
    pt.fee_amount,
    pt.net_amount,
    ROUND(pt.gross_amount * COALESCE(fx.rate_to_usd, 1.0), 2)
                                                            AS gross_amount_usd,
    pt.card_brand,
    pt.card_country,
    pt.failure_reason,
    pt.refund_flag                                          AS is_refund,
    pt.status = 'succeeded'                                 AS is_successful,
    CURRENT_TIMESTAMP                                       AS _loaded_at
FROM silver.stg_payment_transactions pt
LEFT JOIN gold.fact_orders fo
    ON pt.order_id = fo.order_id
LEFT JOIN gold.dim_customers dc
    ON pt.customer_id = dc.customer_id AND dc.is_current = TRUE
LEFT JOIN gold.dim_date dd
    ON CAST(pt.provider_created_at AS DATE) = dd.full_date
LEFT JOIN silver.seed_exchange_rates fx
    ON pt.currency = fx.currency;

-- ---------------------------------------------------------
-- fact_inventory_daily: One row per SKU × warehouse × day
-- ---------------------------------------------------------
CREATE OR REPLACE TABLE gold.fact_inventory_daily AS
SELECT
    ROW_NUMBER() OVER (ORDER BY inv.snapshot_date, inv.warehouse_id, inv.sku)::INTEGER
                                                        AS inventory_sk,
    dd.date_key                                          AS snapshot_date_key,
    dp.product_sk,
    inv.warehouse_id,
    inv.on_hand_qty,
    inv.reserved_qty,
    inv.available_qty,
    inv.reorder_point,
    inv.available_qty < inv.reorder_point                AS is_below_reorder,
    ROUND(inv.on_hand_qty * inv.unit_cost, 2)            AS inventory_value,
    CURRENT_TIMESTAMP                                    AS _loaded_at
FROM silver.stg_inventory_snapshot inv
LEFT JOIN gold.dim_products dp
    ON inv.product_id = dp.product_id
LEFT JOIN gold.dim_date dd
    ON inv.snapshot_date = dd.full_date;

-- ---------------------------------------------------------
-- fact_campaign_performance: One row per campaign×platform×country×device×day
-- ---------------------------------------------------------
CREATE OR REPLACE TABLE gold.fact_campaign_performance AS
SELECT
    ROW_NUMBER() OVER (ORDER BY cp.campaign_date, cp.campaign_id)::INTEGER
                                                        AS campaign_perf_sk,
    dd.date_key                                          AS campaign_date_key,
    cp.platform,
    cp.campaign_id,
    cp.campaign_name,
    cp.channel,
    cp.country_code,
    cp.device_type,
    cp.impressions,
    cp.clicks,
    cp.spend,
    cp.conversions,
    cp.reported_revenue,
    ROUND(cp.clicks::DOUBLE / NULLIF(cp.impressions, 0), 4)
                                                         AS ctr,
    ROUND(cp.spend / NULLIF(cp.clicks, 0), 2)            AS cpc,
    ROUND(cp.spend / NULLIF(cp.conversions, 0), 2)        AS cpa,
    ROUND(cp.reported_revenue / NULLIF(cp.spend, 0), 2)   AS roas,
    CURRENT_TIMESTAMP                                     AS _loaded_at
FROM silver.stg_campaign_performance cp
LEFT JOIN gold.dim_date dd
    ON cp.campaign_date = dd.full_date;

-- ---------------------------------------------------------
-- fact_email_engagement: One row per email send per recipient
-- ---------------------------------------------------------
CREATE OR REPLACE TABLE gold.fact_email_engagement AS
SELECT
    ROW_NUMBER() OVER (ORDER BY es.send_date, es.campaign_id, es.customer_id)::INTEGER
                                                         AS email_sk,
    dd.date_key                                           AS send_date_key,
    es.campaign_id,
    es.campaign_name,
    dc.customer_sk,
    es.delivery_status,
    es.delivery_status = 'delivered'                      AS is_delivered,
    es.open_flag                                          AS is_opened,
    es.click_flag                                         AS is_clicked,
    es.unsubscribe_flag                                   AS is_unsubscribed,
    CURRENT_TIMESTAMP                                     AS _loaded_at
FROM silver.stg_email_sends es
LEFT JOIN gold.dim_customers dc
    ON es.customer_id = dc.customer_id AND dc.is_current = TRUE
LEFT JOIN gold.dim_date dd
    ON es.send_date = dd.full_date;

-- ---------------------------------------------------------
-- fact_purchase_orders: One row per PO line
-- ---------------------------------------------------------
CREATE OR REPLACE TABLE gold.fact_purchase_orders AS
SELECT
    ROW_NUMBER() OVER (ORDER BY po.po_id)::INTEGER       AS po_sk,
    po.po_id,
    po.supplier_id,
    dp.product_sk,
    dd.date_key                                           AS created_date_key,
    po.expected_delivery_date,
    po.ordered_qty,
    po.received_qty,
    po.unit_cost,
    ROUND(po.ordered_qty * po.unit_cost, 2)               AS po_total,
    po.po_status,
    ROUND(po.received_qty::DOUBLE / NULLIF(po.ordered_qty, 0), 2)
                                                          AS fill_rate,
    po.expected_delivery_date < CURRENT_DATE
        AND po.po_status != 'received'                    AS is_overdue,
    CURRENT_TIMESTAMP                                     AS _loaded_at
FROM silver.stg_purchase_orders po
LEFT JOIN gold.dim_products dp
    ON po.sku = dp.sku
LEFT JOIN gold.dim_date dd
    ON CAST(po.created_at AS DATE) = dd.full_date;
