-- Drop the customers table
DROP TABLE IF EXISTS `potent-app-439210-c8.radusStore.customers`;

-- Drop the orders table
DROP TABLE IF EXISTS `potent-app-439210-c8.radusStore.products`;

-- Drop the products table
DROP TABLE IF EXISTS `potent-app-439210-c8.radusStore.orders`;


CREATE TABLE IF NOT EXISTS potent-app-439210-c8.radusStore.customers(
    customer_id INT64 NOT NULL,
    first_name STRING, 
    last_name STRING,
    city STRING,
    country STRING,
    phone STRING,
    email STRING,
    created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS potent-app-439210-c8.radusStore.products(
    product_id INT64 NOT NULL,
    product_name STRING, 
    description STRING,
    stock_quantity INT64,
    category STRING,
    price FLOAT64,
    supplier STRING
);

CREATE TABLE IF NOT EXISTS potent-app-439210-c8.radusStore.orders (
    order_id INT64,
    customer_id INT64,
    product_id INT64,
    order_date TIMESTAMP,
    status STRING,
    total_amount FLOAT64
);

CREATE TABLE IF NOT EXISTS potent-app-439210-c8.radusStore.customer_behaviour (
    behavior_id INT64,
    customer_id INT64,
    product_id INT64,
    event_type STRING,  -- The type of event (e.g., 'view', 'click', 'add_to_cart', 'purchase')  -- The product associated with the behavior (if applicable)
    event_timestamp TIMESTAMP
    );

CREATE TABLE IF NOT EXISTS potent-app-439210-c8.radusStore.product_reviews (
    review_id INT64,
    customer_id INT64,
    product_id INT64,
    rating INT64,  -- from 1 to 5
    review_timestamp TIMESTAMP,  -- When the review was submitted
    verified_purchase BOOLEAN  -- Indicates whether the review came from a verified purchase
    );

CREATE TABLE IF NOT EXISTS `potent-app-439210-c8.radusStore.fact_product_popularity` (
    product_id INT64,
    customer_id INT64,
    product_name STRING,
    product_category STRING,
    interaction_type STRING,
    was_ordered STRING,
    rating FLOAT64,
    customer_country STRING,
    timestamp TIMESTAMP
);