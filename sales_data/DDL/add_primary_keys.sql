ALTER TABLE dim_card_details
    ADD PRIMARY KEY (card_key);
	
ALTER TABLE dim_date_times
    ADD PRIMARY KEY (time_key);
	
ALTER TABLE dim_product_details
    ADD PRIMARY KEY (product_key);

ALTER TABLE dim_store_details
    ADD PRIMARY KEY (store_key);

ALTER TABLE dim_users
    ADD PRIMARY KEY (user_key);
	
ALTER TABLE orders_table
    ADD PRIMARY KEY (order_key);