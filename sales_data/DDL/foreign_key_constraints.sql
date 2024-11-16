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
REFERENCES dim_date_times (date_key);

-- Adding FK constraint to Orders Table on dim_product_details table
ALTER TABLE orders_table
ADD CONSTRAINT fk_dim_product_details
FOREIGN KEY (product_key)
REFERENCES dim_product (product_key);

-- Adding FK constraint to Orders Table on dim_store_details table
ALTER TABLE orders_table
ADD CONSTRAINT fk_dim_store_details
FOREIGN KEY (store_key)
REFERENCES dim_store_details (store_key);

-- Adding FK constraint to Orders Table on dim_currency table
ALTER TABLE orders_table
ADD CONSTRAINT fk_dim_currency
FOREIGN KEY (currency_key)
REFERENCES dim_currency (currency_key);

-- Adding FK constraint to Orders Table on dim_users table
ALTER TABLE orders_table
ADD CONSTRAINT fk_dim_users
FOREIGN KEY (user_key)
REFERENCES dim_users (user_key);

-- Adding FK constraint to dim_currency table referencing dim_currency_conversion table
ALTER TABLE dim_currency
ADD CONSTRAINT fk_dim_currency_conversion
FOREIGN KEY (currency_conversion_key)
REFERENCES dim_currency_conversion (currency_conversion_key);