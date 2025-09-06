CREATE SCHEMA IF NOT EXISTS Target;

CREATE TABLE IF NOT EXISTS Target.DIM_PRODUCTS (
    product_id VARCHAR(20) PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    brand VARCHAR(100),
    price DECIMAL(10,2),
    stock_quantity INTEGER,
    sku VARCHAR(100),
    source_system VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Do DIM_USERS yourself. Similar to DIM_PRODUCTS
CREATE TABLE IF NOT EXISTS Target.DIM_USERS (
    user_id VARCHAR(20) PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
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
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS Target.FACT_SALES (
    sales_key BIGSERIAL PRIMARY KEY, -- Auto-generated surrogate key
    sale_id VARCHAR(20) NOT NULL,
    sale_date DATE NOT NULL,
    store_id VARCHAR(20),
    product_id VARCHAR(20) NOT NULL,
    customer_id VARCHAR(20),
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10,2),
    total_amount DECIMAL(12,2),
    source_system VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (sale_id, product_id),
    FOREIGN KEY (product_id) REFERENCES Target.DIM_PRODUCTS(product_id),
    FOREIGN KEY (customer_id) REFERENCES Target.DIM_USERS(user_id)
);
