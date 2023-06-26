ALTER TABLE dim_product_details 
ADD COLUMN weight_class VARCHAR(255);

ALTER TABLE orders_table
ADD COLUMN country_code VARCHAR(10), 
ADD COLUMN card_key BIGINT,
ADD COLUMN date_key BIGINT,
ADD COLUMN product_key BIGINT,
ADD COLUMN store_key BIGINT,
ADD COLUMN user_key BIGINT,
ADD COLUMN currency_key BIGINT;

ALTER TABLE dim_currency
ADD COLUMN currency_conversion_key BIGINT;