ALTER TABLE dim_product_details 
ADD COLUMN weight_class VARCHAR(255);

ALTER TABLE orders_table 
ADD COLUMN card_key BIGINT,
ADD COLUMN date_key BIGINT,
ADD COLUMN product_key BIGINT,
ADD COLUMN store_key BIGINT,
ADD COLUMN user_key BIGINT;

