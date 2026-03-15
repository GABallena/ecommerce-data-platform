
CREATE OR REPLACE TABLE gold.metric_customer_lifetime_value AS
WITH customer_orders AS (
    SELECT
        dc.canonical_customer_id,
        MIN(dc.first_name)                          AS first_name,
        MIN(dc.last_name)                           AS last_name,
        MIN(dc.email)                               AS email,
        MIN(dc.country_code)                        AS country_code,
        COUNT(DISTINCT fo.order_id)                  AS total_orders,
        COUNT(DISTINCT CASE WHEN NOT fo.is_cancelled
                             THEN fo.order_id END)   AS completed_orders,
        SUM(CASE WHEN NOT fo.is_cancelled
                 THEN fo.total_amount_usd ELSE 0 END)
                                                     AS total_revenue_usd,
        SUM(CASE WHEN NOT fo.is_cancelled
                 THEN fo.discount_amount ELSE 0 END)
                                                     AS total_discount,
        MIN(dd_first.full_date)                      AS first_order_date,
        MAX(dd_last.full_date)                       AS last_order_date
    FROM gold.dim_customers dc
    LEFT JOIN gold.fact_orders fo
        ON dc.customer_sk = fo.customer_sk
    LEFT JOIN gold.dim_date dd_first
        ON fo.order_date_key = dd_first.date_key
    LEFT JOIN gold.dim_date dd_last
        ON fo.order_date_key = dd_last.date_key
    GROUP BY dc.canonical_customer_id
)
SELECT
    canonical_customer_id,
    first_name,
    last_name,
    email,
    country_code,
    total_orders,
    completed_orders,
    ROUND(total_revenue_usd, 2)                     AS lifetime_revenue_usd,
    ROUND(total_revenue_usd / NULLIF(completed_orders, 0), 2)
                                                     AS avg_order_value_usd,
    total_discount                                   AS lifetime_discount,
    first_order_date,
    last_order_date,
    CASE WHEN first_order_date IS NOT NULL
         THEN DATE_DIFF('day', first_order_date, last_order_date)
         ELSE 0
    END                                              AS customer_tenure_days,
    CASE WHEN last_order_date IS NOT NULL
         THEN DATE_DIFF('day', last_order_date, CURRENT_DATE)
         ELSE NULL
    END                                              AS days_since_last_order,
    ROUND(
        (total_revenue_usd / NULLIF(completed_orders, 0))
        * (completed_orders::DOUBLE
           / NULLIF(GREATEST(DATE_DIFF('day', first_order_date, last_order_date), 1), 0)
           * 90),
    2)                                               AS estimated_clv_90d,
    CURRENT_TIMESTAMP                                AS _computed_at
FROM customer_orders;

CREATE OR REPLACE TABLE gold.metric_order_conversion_rate AS
WITH order_funnel AS (
    SELECT
        dd.full_date                                 AS order_date,
        COUNT(*)                                     AS total_orders,
        COUNT(CASE WHEN NOT fo.is_cancelled
                   THEN 1 END)                       AS non_cancelled_orders,
        COUNT(CASE WHEN fo.is_paid
                   THEN 1 END)                       AS paid_orders,
        COUNT(CASE WHEN fo.is_cancelled
                   THEN 1 END)                       AS cancelled_orders,
        SUM(fo.total_amount_usd)                     AS total_gmv_usd,
        SUM(CASE WHEN fo.is_paid
                 THEN fo.total_amount_usd ELSE 0 END)
                                                     AS paid_revenue_usd
    FROM gold.fact_orders fo
    JOIN gold.dim_date dd
        ON fo.order_date_key = dd.date_key
    GROUP BY dd.full_date
)
SELECT
    order_date,
    total_orders,
    non_cancelled_orders,
    paid_orders,
    cancelled_orders,
    ROUND(total_gmv_usd, 2)                          AS total_gmv_usd,
    ROUND(paid_revenue_usd, 2)                        AS paid_revenue_usd,
    ROUND(non_cancelled_orders::DOUBLE
          / NULLIF(total_orders, 0), 4)               AS non_cancel_rate,
    ROUND(paid_orders::DOUBLE
          / NULLIF(total_orders, 0), 4)               AS payment_conversion_rate,
    ROUND(cancelled_orders::DOUBLE
          / NULLIF(total_orders, 0), 4)               AS cancellation_rate,
    CURRENT_TIMESTAMP                                 AS _computed_at
FROM order_funnel
ORDER BY order_date;

CREATE OR REPLACE TABLE gold.metric_revenue_by_campaign AS
WITH campaign_reach AS (
    SELECT
        fe.campaign_id,
        MIN(fe.campaign_name)                        AS campaign_name,
        COUNT(*)                                     AS total_sends,
        SUM(CASE WHEN fe.is_delivered THEN 1 ELSE 0 END) AS delivered,
        SUM(CASE WHEN fe.is_opened   THEN 1 ELSE 0 END) AS opened,
        SUM(CASE WHEN fe.is_clicked  THEN 1 ELSE 0 END) AS clicked
    FROM gold.fact_email_engagement fe
    GROUP BY fe.campaign_id
),
campaign_attributed_revenue AS (
    SELECT
        fe.campaign_id,
        SUM(fo.total_amount_usd)                     AS attributed_revenue_usd,
        COUNT(DISTINCT fo.order_id)                   AS attributed_orders
    FROM gold.fact_email_engagement fe
    JOIN gold.fact_orders fo
        ON fe.customer_sk = fo.customer_sk
    JOIN gold.dim_date dd_send ON fe.send_date_key = dd_send.date_key
    JOIN gold.dim_date dd_order ON fo.order_date_key = dd_order.date_key
    WHERE dd_order.full_date BETWEEN dd_send.full_date
                                 AND dd_send.full_date + INTERVAL '7 days'
      AND NOT fo.is_cancelled
    GROUP BY fe.campaign_id
),
paid_spend AS (
    SELECT
        campaign_id,
        MIN(campaign_name)                           AS campaign_name,
        SUM(spend)                                   AS total_spend,
        SUM(impressions)                             AS total_impressions,
        SUM(clicks)                                  AS total_clicks,
        SUM(conversions)                             AS total_conversions,
        SUM(reported_revenue)                        AS reported_revenue
    FROM gold.fact_campaign_performance
    GROUP BY campaign_id
)
SELECT
    COALESCE(cr.campaign_id, ps.campaign_id)         AS campaign_id,
    COALESCE(cr.campaign_name, ps.campaign_name)     AS campaign_name,
    cr.total_sends,
    cr.delivered,
    cr.opened,
    cr.clicked,
    ps.total_spend,
    ps.total_impressions,
    ps.total_clicks                                  AS paid_clicks,
    ps.total_conversions,
    ps.reported_revenue                              AS paid_reported_revenue,
    COALESCE(car.attributed_orders, 0)               AS attributed_orders,
    ROUND(COALESCE(car.attributed_revenue_usd, 0), 2)
                                                     AS attributed_revenue_usd,
    ROUND(COALESCE(car.attributed_revenue_usd, 0)
          / NULLIF(ps.total_spend, 0), 2)            AS attributed_roas,
    CURRENT_TIMESTAMP                                AS _computed_at
FROM campaign_reach cr
FULL OUTER JOIN paid_spend ps
    ON cr.campaign_id = ps.campaign_id
LEFT JOIN campaign_attributed_revenue car
    ON COALESCE(cr.campaign_id, ps.campaign_id) = car.campaign_id;

CREATE OR REPLACE TABLE gold.metric_inventory_turnover AS
WITH product_sales AS (
    SELECT
        dp.product_sk,
        dp.product_id,
        dp.sku,
        dp.product_name,
        dp.category_standardized,
        dp.unit_cost,
        SUM(fi.quantity)                             AS units_sold,
        SUM(fi.line_total)                           AS total_sales_revenue,
        SUM(fi.gross_profit)                         AS total_gross_profit,
        COUNT(DISTINCT fi.order_item_sk)             AS line_item_count
    FROM gold.fact_order_items fi
    JOIN gold.fact_orders fo
        ON fi.order_sk = fo.order_sk
    JOIN gold.dim_products dp
        ON fi.product_sk = dp.product_sk
    WHERE NOT fo.is_cancelled
    GROUP BY dp.product_sk, dp.product_id, dp.sku,
             dp.product_name, dp.category_standardized, dp.unit_cost
),
current_inventory AS (
    SELECT
        product_sk,
        SUM(on_hand_qty)                             AS total_on_hand,
        SUM(available_qty)                           AS total_available,
        SUM(reserved_qty)                            AS total_reserved,
        MAX(is_below_reorder)                        AS any_below_reorder,
        SUM(inventory_value)                         AS total_inventory_value
    FROM gold.fact_inventory_daily
    GROUP BY product_sk
),
pending_supply AS (
    SELECT
        product_sk,
        SUM(ordered_qty - received_qty)              AS incoming_qty,
        COUNT(*)                                     AS open_po_count
    FROM gold.fact_purchase_orders
    WHERE po_status != 'received'
    GROUP BY product_sk
)
SELECT
    ps.product_sk,
    ps.product_id,
    ps.sku,
    ps.product_name,
    ps.category_standardized,
    ps.units_sold,
    ps.total_sales_revenue,
    ps.total_gross_profit,
    ci.total_on_hand,
    ci.total_available,
    ci.total_reserved,
    ci.any_below_reorder,
    ci.total_inventory_value,
    ROUND((ps.units_sold * ps.unit_cost)
          / NULLIF(ci.total_inventory_value, 0), 4)  AS inventory_turnover_ratio,
    ROUND(ci.total_on_hand::DOUBLE
          / NULLIF(ps.units_sold, 0), 1)             AS days_of_supply,
    COALESCE(sup.incoming_qty, 0)                    AS incoming_supply_qty,
    COALESCE(sup.open_po_count, 0)                   AS open_po_count,
    CURRENT_TIMESTAMP                                AS _computed_at
FROM product_sales ps
LEFT JOIN current_inventory ci
    ON ps.product_sk = ci.product_sk
LEFT JOIN pending_supply sup
    ON ps.product_sk = sup.product_sk;
