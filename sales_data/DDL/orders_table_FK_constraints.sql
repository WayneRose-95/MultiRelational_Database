-- 04.06.2023 WR Initial Draft

-- Adding FK constraint to Orders Table on dim_card_details table 
ALTER TABLE orders_table
ADD CONSTRAINT fk_dim_card_details
FOREIGN KEY (card_key)
REFERENCES dim_card_details (card_key);

-- Adding FK constraint to Orders Table on dim_date_times table
ALTER TABLE orders_table
ADD CONSTRAINT fk_dim_date_times
FOREIGN KEY (date_key)
REFERENCES dim_date_times (time_key);

-- Adding FK constraint to Orders Table on dim_product_details table
ALTER TABLE orders_table
ADD CONSTRAINT fk_dim_product_details
FOREIGN KEY (product_key)
REFERENCES dim_product_details (product_key);

-- Adding FK constraint to Orders Table on dim_store_details table
ALTER TABLE orders_table
ADD CONSTRAINT fk_dim_store_details
FOREIGN KEY (store_key)
REFERENCES dim_store_details (store_key);

-- Adding FK constraint to Orders Table on dim_store_details table
ALTER TABLE orders_table
ADD CONSTRAINT fk_dim_users
FOREIGN KEY (user_key)
REFERENCES dim_users (user_key);