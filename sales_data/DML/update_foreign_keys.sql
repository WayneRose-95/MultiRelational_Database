-- 04.06.2023 WR Initial Draft 
-- 04.06.2023 WR 21:41PM GMT Updated script to match keys to the orders_table

-- Updating orders_table with the keys from the dim_card_details table 
UPDATE orders_table
SET card_key = dim_card_details.card_key
FROM dim_card_details
WHERE orders_table.card_number::varchar = dim_card_details.card_number;

-- Updating orders_table with the keys from the dim_date_times table
UPDATE orders_table
SET date_key = dim_date_times.date_key
FROM dim_date_times
WHERE orders_table.date_uuid::uuid = dim_date_times.date_uuid;

-- Updating orders_table with the keys from the dim_product_details table
UPDATE orders_table 
SET product_key = dim_product.product_key
FROM dim_product 
WHERE orders_table.product_code::varchar = dim_product.product_code;

-- Updating orders_table with the keys from the dim_store_details table
UPDATE orders_table 
SET store_key = dim_store_details.store_key 
FROM dim_store_details
WHERE orders_table.store_code::varchar = dim_store_details.store_code;

-- Updating orders_table with keys from the dim_users table 
UPDATE orders_table 
SET user_key = dim_users.user_key 
FROM dim_users 
WHERE orders_table.user_uuid::uuid = dim_users.user_uuid; 

-- Update the country_code column in orders_table using a subquery to fetch the corresponding value from dim_store_details
UPDATE orders_table
SET country_code = (
  SELECT d.country_code
  FROM dim_store_details AS d
  WHERE d.store_code = orders_table.store_code
);

-- Update the currency_key column in the orders_table using a subquery to fetch the corresponding value from dim_currency
UPDATE orders_table
SET currency_key = (
  SELECT c.currency_key
  FROM dim_currency AS c
  WHERE c.country_code = orders_table.country_code
);

-- Update the dim_currency key column in the dim_currency table with the keys from the dim_currency_conversion table 
UPDATE dim_currency
SET currency_conversion_key = dim_currency_conversion.currency_conversion_key
FROM dim_currency_conversion
WHERE dim_currency.currency_code = dim_currency_conversion.currency_code;