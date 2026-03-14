-- Category mapping seed: raw → standardized
-- Used by stg_products to normalize inconsistent category values
CREATE OR REPLACE TABLE silver.seed_category_mapping AS
SELECT * FROM (VALUES
    ('running shoes',  'Footwear'),
    ('apparel',        'Apparel'),
    ('bags / urban',   'Bags'),
    ('homewares',      'Homewares'),
    ('bags',           'Bags'),
    ('shoes',          'Footwear'),
    ('accessories',    'Accessories'),
    ('electronics',    'Electronics')
) AS t(category_raw, category_standardized);

-- Exchange rate seed: for multi-currency conversion
CREATE OR REPLACE TABLE silver.seed_exchange_rates AS
SELECT * FROM (VALUES
    ('USD', 1.0000),
    ('PHP', 0.0178)
) AS t(currency, rate_to_usd);
