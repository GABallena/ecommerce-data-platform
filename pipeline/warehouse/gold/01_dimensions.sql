-- ============================================================
-- GOLD LAYER: Dimension tables
-- ============================================================

-- ---------------------------------------------------------
-- dim_date: Pre-generated calendar (2024-01-01 to 2027-12-31)
-- ---------------------------------------------------------
CREATE OR REPLACE TABLE gold.dim_date AS
WITH date_spine AS (
    SELECT UNNEST(generate_series(DATE '2024-01-01', DATE '2027-12-31', INTERVAL 1 DAY))::DATE AS full_date
)
SELECT
    CAST(strftime(full_date, '%Y%m%d') AS INTEGER)        AS date_key,
    full_date,
    EXTRACT(ISODOW FROM full_date)::INTEGER               AS day_of_week,
    strftime(full_date, '%A')                             AS day_name,
    EXTRACT(WEEK FROM full_date)::INTEGER                 AS week_of_year,
    EXTRACT(MONTH FROM full_date)::INTEGER                AS month_num,
    strftime(full_date, '%B')                             AS month_name,
    EXTRACT(QUARTER FROM full_date)::INTEGER              AS quarter,
    EXTRACT(YEAR FROM full_date)::INTEGER                 AS year,
    CASE WHEN EXTRACT(ISODOW FROM full_date) IN (6, 7)
         THEN TRUE ELSE FALSE END                         AS is_weekend,
    -- Fiscal year: assume starts in July (configurable)
    CASE WHEN EXTRACT(MONTH FROM full_date) >= 7
         THEN EXTRACT(QUARTER FROM full_date)::INTEGER - 2
         ELSE EXTRACT(QUARTER FROM full_date)::INTEGER + 2
    END                                                   AS fiscal_quarter,
    CASE WHEN EXTRACT(MONTH FROM full_date) >= 7
         THEN EXTRACT(YEAR FROM full_date)::INTEGER + 1
         ELSE EXTRACT(YEAR FROM full_date)::INTEGER
    END                                                   AS fiscal_year
FROM date_spine;

-- ---------------------------------------------------------
-- dim_customers: Deduplicated by email, SCD Type 2 ready
-- ---------------------------------------------------------
CREATE OR REPLACE TABLE gold.dim_customers AS
WITH canonical AS (
    -- Pick the earliest customer_id per email as canonical
    SELECT
        email,
        MIN(customer_id) AS canonical_customer_id
    FROM silver.stg_customers
    GROUP BY email
),
enriched AS (
    SELECT
        c.*,
        can.canonical_customer_id
    FROM silver.stg_customers c
    JOIN canonical can ON c.email = can.email
)
SELECT
    ROW_NUMBER() OVER (ORDER BY canonical_customer_id, customer_id)::INTEGER
                                        AS customer_sk,
    customer_id,
    canonical_customer_id,
    email,
    first_name,
    last_name,
    phone,
    country_code,
    status,
    marketing_opt_in,
    created_at,
    is_duplicate_email,
    -- SCD Type 2 fields (initial load: all current)
    TRUE                                AS is_current,
    created_at                          AS valid_from,
    NULL::TIMESTAMP                     AS valid_to,
    CURRENT_TIMESTAMP                   AS _loaded_at
FROM enriched;

-- ---------------------------------------------------------
-- dim_products: Standardized categories, gross margin
-- ---------------------------------------------------------
CREATE OR REPLACE TABLE gold.dim_products AS
SELECT
    ROW_NUMBER() OVER (ORDER BY product_id)::INTEGER AS product_sk,
    product_id,
    sku,
    product_name,
    category_raw,
    category_standardized,
    brand,
    unit_cost,
    list_price,
    gross_margin_pct,
    is_active,
    created_at,
    CURRENT_TIMESTAMP                   AS _loaded_at
FROM silver.stg_products;
