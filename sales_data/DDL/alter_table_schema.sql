-- Altering Schema of dim_card_details table 

ALTER TABLE dim_card_details
ALTER COLUMN card_number TYPE VARCHAR(30),
ALTER COLUMN expiry_date TYPE VARCHAR(10),
ALTER COLUMN date_payment_confirmed TYPE DATE USING date_payment_confirmed::date;

-- Altering schema of dim_date_times table 

ALTER TABLE dim_date_times 
ALTER COLUMN month TYPE VARCHAR(30),
ALTER COLUMN year TYPE VARCHAR(30),
ALTER COLUMN day TYPE VARCHAR(30),
ALTER COLUMN time_period TYPE VARCHAR(40),
ALTER COLUMN date_uuid TYPE uuid USING date_uuid::uuid;

-- Altering schema for dim_store_details table 

ALTER TABLE dim_store_details
ALTER COLUMN longitude TYPE FLOAT USING longitude::double precision,
ALTER COLUMN latitude TYPE FLOAT USING latitude::double precision,
ALTER COLUMN store_code TYPE VARCHAR(20),
ALTER COLUMN city TYPE VARCHAR(255),
ALTER COLUMN number_of_staff TYPE SMALLINT USING number_of_staff::smallint,
ALTER COLUMN opening_date TYPE DATE USING opening_date::date,
ALTER COLUMN store_type TYPE VARCHAR(255),
ALTER COLUMN country_code TYPE VARCHAR(10),
ALTER COLUMN region TYPE VARCHAR(255);

-- Altering schema for dim_users table 

ALTER TABLE dim_users
ALTER COLUMN first_name TYPE VARCHAR(255), 
ALTER COLUMN last_name TYPE VARCHAR(255),
ALTER COLUMN birth_date TYPE DATE USING birth_date::date,
ALTER COLUMN country_index TYPE VARCHAR(10),
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid,
ALTER COLUMN join_date TYPE DATE USING join_date::date;

-- Altering schema for Orders_Table 

ALTER TABLE orders_table 
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid,
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid,
ALTER COLUMN card_number TYPE VARCHAR(30),
ALTER COLUMN store_code TYPE VARCHAR(30),
ALTER COLUMN product_code TYPE VARCHAR(30),
ALTER COLUMN product_quantity TYPE SMALLINT; 

-- Altering schema for dim_currency table 

ALTER TABLE dim_currency
ALTER COLUMN country_name TYPE VARCHAR(100),
ALTER COLUMN currency_code TYPE VARCHAR(10),
ALTER COLUMN country_code TYPE VARCHAR(5);

-- Altering schema for dim_currency_conversion table 
ALTER TABLE dim_currency_conversion 
ALTER COLUMN currency_conversion_key TYPE BIGINT,
ALTER COLUMN currency_name TYPE VARCHAR(50),
ALTER COLUMN currency_code TYPE VARCHAR(5),
ALTER COLUMN conversion_rate TYPE DECIMAL(20,6) USING conversion_rate::numeric(20,6),
ALTER COLUMN conversion_rate_percentage TYPE NUMERIC(20,6) USING conversion_rate_percentage::numeric(20,6),
ALTER COLUMN last_updated TYPE TIMESTAMP WITH TIME ZONE USING last_updated::timestamp with time zone;

-- Altering Schema of land_card_details table 

ALTER TABLE land_card_details
ALTER COLUMN card_number TYPE VARCHAR(30),
ALTER COLUMN expiry_date TYPE VARCHAR(10),
ALTER COLUMN date_payment_confirmed TYPE DATE USING date_payment_confirmed::date;

-- Altering schema of land_date_times table 

ALTER TABLE land_date_times 
ALTER COLUMN month TYPE VARCHAR(30),
ALTER COLUMN year TYPE VARCHAR(30),
ALTER COLUMN day TYPE VARCHAR(30),
ALTER COLUMN time_period TYPE VARCHAR(40),
ALTER COLUMN date_uuid TYPE uuid USING date_uuid::uuid;

-- Altering schema for land_store_details table 

ALTER TABLE land_store_details
ALTER COLUMN longitude TYPE FLOAT USING longitude::double precision,
ALTER COLUMN latitude TYPE FLOAT USING latitude::double precision,
ALTER COLUMN store_code TYPE VARCHAR(20),
ALTER COLUMN city TYPE VARCHAR(255),
ALTER COLUMN number_of_staff TYPE SMALLINT USING number_of_staff::smallint,
ALTER COLUMN opening_date TYPE DATE USING opening_date::date,
ALTER COLUMN store_type TYPE VARCHAR(255),
ALTER COLUMN country_code TYPE VARCHAR(10),
ALTER COLUMN region TYPE VARCHAR(255);

-- Altering schema for dim_users table 

ALTER TABLE land_user_data
ALTER COLUMN first_name TYPE VARCHAR(255), 
ALTER COLUMN last_name TYPE VARCHAR(255),
ALTER COLUMN birth_date TYPE DATE USING birth_date::date,
ALTER COLUMN country_index TYPE VARCHAR(10),
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid,
ALTER COLUMN join_date TYPE DATE USING join_date::date;


-- Altering schema for land_currency table 

ALTER TABLE land_currency
ALTER COLUMN country_name TYPE VARCHAR(100),
ALTER COLUMN currency_code TYPE VARCHAR(10),
ALTER COLUMN country_code TYPE VARCHAR(5);

-- Altering schema for land_currency_conversion table 
ALTER TABLE land_currency_conversion 
ALTER COLUMN currency_conversion_key TYPE BIGINT,
ALTER COLUMN currency_name TYPE VARCHAR(50),
ALTER COLUMN currency_code TYPE VARCHAR(5),
ALTER COLUMN conversion_rate TYPE NUMERIC(20,6) USING conversion_rate::numeric(20,6),
ALTER COLUMN conversion_rate_percentage TYPE NUMERIC(20,6) USING conversion_rate_percentage::numeric(20,6),
ALTER COLUMN last_updated TYPE TIMESTAMP WITH TIME ZONE USING last_updated::timestamp with time zone;