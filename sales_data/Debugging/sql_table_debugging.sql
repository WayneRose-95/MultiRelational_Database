/* 
SQL Script used to debug the number of missing values within the data warehouse. 

When mapping the keys from the dim tables to the foreign keys of the orders table,
any unknown values will have a foreign key value of 0. 

This script counts the number of foreign keys in each of the foreign key columns, 
whose key is equal to 0. 

Each of these queries should return a value of 0, if not,
then there are some missing records within the dimension table,
which are not being populated inside the fact table 

*/

-- Count of missing values 0 
SELECT COUNT(date_key) FROM orders_table
WHERE date_key = 0

-- Count of missing values 0 
SELECT COUNT(product_key) FROM orders_table
WHERE product_key = 0

-- Count of missing values = 2528 (now 0)
SELECT COUNT(store_key) FROM orders_table 
WHERE store_key = 0 

-- Count of missing values 387 (now 0)
SELECT COUNT(user_key) FROM orders_table
WHERE user_key = 0

-- Count of missing values 261 (now 0)
SELECT COUNT(card_key) FROM orders_table
WHERE card_key = 0