ALTER TABLE dim_card_details
    ADD PRIMARY KEY (card_key);
	
ALTER TABLE dim_date_times
    ADD PRIMARY KEY (date_key);
	
ALTER TABLE dim_product_details
    ADD PRIMARY KEY (product_key);

ALTER TABLE dim_store_details
    ADD PRIMARY KEY (store_key);

ALTER TABLE dim_users
    ADD PRIMARY KEY (user_key);

ALTER TABLE dim_currency
    ADD PRIMARY KEY (currency_key);
    
ALTER TABLE orders_table
    ADD PRIMARY KEY (order_key);

ALTER TABLE dim_currency_conversion
    ADD PRIMARY KEY (currency_conversion_key);