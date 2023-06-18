-- Updating orders table to fill null card_key values with 0s 
UPDATE orders_table
SET card_key = 0
WHERE card_key IS NULL
  AND NOT EXISTS (
    SELECT 1
    FROM dim_card_details
    WHERE dim_card_details.card_key = orders_table.card_key
  );

-- Updating orders table to fill null user_key values with 0s  
UPDATE orders_table
SET user_key = 0
WHERE user_key IS NULL
  AND NOT EXISTS (
    SELECT 1
    FROM dim_users
    WHERE dim_users.user_key = orders_table.user_key
  );

-- Updating orders table to fill null store_key values with 0s   
UPDATE orders_table
SET store_key = 0
WHERE store_key IS NULL
  AND NOT EXISTS (
    SELECT 1
    FROM dim_store_details
    WHERE dim_store_details.store_key = orders_table.store_key
  );

-- Updating orders table to fill null product_key values with 0s    
UPDATE orders_table
SET product_key = 0
WHERE product_key IS NULL
  AND NOT EXISTS (
    SELECT 1
    FROM dim_product_details
    WHERE dim_product_details.product_key = orders_table.product_key
  );  

-- date times do not need this change 