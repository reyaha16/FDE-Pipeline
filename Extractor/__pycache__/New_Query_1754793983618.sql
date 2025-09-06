CREATE SCHEMA IF NOT EXISTS Transform;

CREATE TABLE IF NOT EXISTS Transform.TMP_PRODUCTS (
    product_id VARCHAR(20),
    product_name VARCHAR(255),
    category VARCHAR(100),
    brand VARCHAR(100),
    price DECIMAL(10,2),
    stock_quantity INTEGER,
    sku VARCHAR(100),
    source_system VARCHAR(50),
    source_loaded_at TIMESTAMP,
    PRIMARY KEY (product_id, source_system)
);

CREATE TABLE IF NOT EXISTS Transform.TMP_USERS (
    user_id VARCHAR(20),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    phone VARCHAR(50),
    age INTEGER,
    gender VARCHAR(20),
    city VARCHAR(100),
    state VARCHAR(100),
    postal_code VARCHAR(20),
    country VARCHAR(100),
    source_system VARCHAR(50),
    source_loaded_at TIMESTAMP,
    PRIMARY KEY (user_id, source_system)
);

CREATE TABLE IF NOT EXISTS Transform.TMP_SALES (
    sale_id VARCHAR(20),
    sale_date DATE,
    store_id VARCHAR(20),
    product_id VARCHAR(20),
    customer_id VARCHAR(20),
    quantity INTEGER,
    unit_price DECIMAL(10,2),
    total_amount DECIMAL(12,2),
    source_system VARCHAR(50),
    source_loaded_at TIMESTAMP
);
