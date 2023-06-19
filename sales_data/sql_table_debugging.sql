-- Count of missing values 0 
SELECT COUNT(date_key) FROM orders_table
WHERE date_key = 0

-- Count of missing values 0 
SELECT COUNT(product_key) FROM orders_table
WHERE product_key = 0

-- Count of missing values = 2528 
SELECT COUNT(store_key) FROM orders_table 
WHERE store_key = 0 

-- Count of missing values 387 
SELECT COUNT(user_key) FROM orders_table
WHERE user_key = 0

-- Count of missing values 261 
SELECT COUNT(card_key) FROM orders_table
WHERE card_key = 0