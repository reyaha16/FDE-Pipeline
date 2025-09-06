CREATE OR REPLACE VIEW Landing.V_LND_PRODUCTS_API AS
SELECT
     l.loaded_at,
    jsonb_array_elements(l.raw_data->'products') AS product_json
FROM Landing.LND_PRODUCTS_API l
WHERE l.raw_data IS NOT NULL;


CREATE OR REPLACE VIEW Landing.V_LND_PRODUCTS_JSON AS
SELECT
     l.loaded_at,
    jsonb_array_elements(l.raw_data->'products') AS product_json
FROM Landing.LND_PRODUCTS_JSON l
WHERE l.raw_data IS NOT NULL;

CREATE OR REPLACE VIEW Landing.V_LND_USERS_API AS
SELECT
     l.loaded_at,
    jsonb_array_elements(l.raw_data->'users') AS user_json
FROM Landing.LND_USERS_API l
WHERE l.raw_data IS NOT NULL;


CREATE OR REPLACE VIEW Landing.V_LND_SALES_JSON AS
SELECT
     l.loaded_at,
    jsonb_array_elements(l.raw_data->'sales') AS sales_json
FROM Landing.LND_SALES_JSON l
WHERE l.raw_data IS NOT NULL;



CREATE SCHEMA IF NOT EXISTS Staging;


CREATE OR REPLACE VIEW Staging.STG_USERS AS
SELECT
    'CUS' || LPAD(CAST(user_json ->> 'id' AS TEXT), 3, '0') AS user_id,
    user_json ->> 'firstName' AS first_name,
    user_json ->> 'lastName' AS last_name,
    user_json ->> 'email' AS email,
    user_json ->> 'phone' AS phone,
    CAST(user_json ->> 'age' AS INTEGER) AS age,
    user_json ->> 'gender' AS gender,
    user_json -> 'address' ->> 'city' AS city,
    user_json -> 'address' ->> 'state' AS state,
    user_json -> 'address' ->> 'postalCode' AS postal_code,
    user_json -> 'address' ->> 'country' AS country,
    'API' AS source_system,
    loaded_at AS source_loaded_at
FROM landing.v_lnd_users_api

UNION ALL

SELECT
    customer_key AS user_id,
    (regexp_split_to_array(name, ' '))[1] AS first_name,
    (regexp_split_to_array(name, ' '))[2] AS last_name,
    NULL AS email,
    NULL AS phone,
    DATE_PART('year', AGE(birthday))::INTEGER AS age,
    gender,
    city,
    state,
    zip_code AS postal_code,
    country,
    'CSV' AS source_system,
    CURRENT_TIMESTAMP AS source_loaded_at
FROM landing.lnd_customers_csv;


-- SALES VIEW
CREATE OR REPLACE VIEW Staging.STG_SALES AS
SELECT
    sales_json ->> 'order_id'  AS sale_id,
    CAST(sales_json ->> 'order_date' AS DATE) AS sale_date,
    NULL AS store_id,
    item ->> 'product_id' AS product_id,
    sales_json -> 'customer' ->> 'id' AS customer_id,
    CAST(item ->> 'quantity' AS INTEGER) AS quantity,
    CAST(item ->> 'unit_price' AS DECIMAL(10,2)) AS unit_price,
    CAST(item ->> 'total_price' AS DECIMAL(12,2)) AS total_amount,
    'JSON' AS source_system,
    loaded_at AS source_loaded_at
FROM landing.v_lnd_sales_json,
     jsonb_array_elements(sales_json -> 'items') AS item

UNION ALL

SELECT
    order_number AS sale_id,
    order_date AS sale_date,
    store_key AS store_id,
    product_key AS product_id,
    customer_key AS customer_id,
    quantity,
    NULL AS unit_price,
    NULL AS total_amount,
    'CSV' AS source_system,
    CURRENT_TIMESTAMP AS source_loaded_at
FROM landing.lnd_sales_csv;


-- CREATE PRODUCTS VIEW YOURSELf

CREATE OR REPLACE VIEW Staging.STG_PRODUCTS AS
SELECT
    'PRD' || LPAD(CAST(product_json ->> 'id' AS TEXT), 3, '0') AS product_id,
    product_json ->> 'title' AS product_name,
    product_json ->> 'category' AS category,
    product_json ->> 'brand' AS brand,
    CAST(product_json ->> 'price' AS DECIMAL(10,2)) AS price,
    CAST(product_json ->> 'stock' AS INTEGER) AS stock_quantity,
    product_json ->> 'sku' AS sku,
    'API' AS source_system,
    loaded_at AS source_loaded_at
FROM landing.v_lnd_products_api

UNION ALL

SELECT
    product_json ->> 'id' AS product_id,
    product_json ->> 'title' AS product_name,
    product_json ->> 'category' AS category,
    product_json ->> 'brand' AS brand,
    CAST(product_json ->> 'price' AS DECIMAL(10,2)) AS price,
    CAST(product_json ->> 'stock' AS INTEGER) AS stock_quantity,
    NULL AS sku,
    'JSON' AS source_system,
    loaded_at AS source_loaded_at
FROM landing.v_lnd_products_json

UNION ALL

SELECT
    product_key AS product_id,
    product_name,
    category,
    brand,
    unit_price_usd AS price,
    NULL AS stock_quantity,
    NULL AS sku,
    'CSV' AS source_system,
    CURRENT_TIMESTAMP AS source_loaded_at
FROM landing.lnd_products_csv;


