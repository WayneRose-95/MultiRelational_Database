-- Altering Schema of dim_card_details table 

ALTER TABLE dim_card_details
ALTER COLUMN card_number TYPE VARCHAR(30),
ALTER COLUMN expiry_date TYPE VARCHAR(10),
ALTER COLUMN card_provider TYPE character varying(255),
ALTER COLUMN date_payment_confirmed TYPE DATE USING date_payment_confirmed::date;

-- Altering schema of dim_date_times table 

ALTER TABLE dim_date_times 
ALTER COLUMN event_time TYPE TIME,
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

-- Altering schema for dim_products table 

ALTER TABLE dim_product_details 
ALTER COLUMN product_price TYPE FLOAT USING product_price::double precision,
ALTER COLUMN weight TYPE FLOAT, 
ALTER COLUMN "EAN" TYPE VARCHAR(50),
ALTER COLUMN product_code TYPE VARCHAR(50),
ALTER COLUMN date_added TYPE DATE, 
ALTER COLUMN uuid TYPE uuid USING uuid::uuid,
ALTER COLUMN availability TYPE BOOLEAN,
ALTER COLUMN weight_class TYPE VARCHAR(50);

-- Altering schema for dim_users table 

ALTER TABLE dim_users
ALTER COLUMN first_name TYPE VARCHAR(255), 
ALTER COLUMN last_name TYPE VARCHAR(255),
ALTER COLUMN birth_date TYPE DATE USING birth_date::date,
ALTER COLUMN company TYPE VARCHAR(255),
ALTER COLUMN email_address TYPE VARCHAR(255),
ALTER COLUMN address TYPE VARCHAR(500),
ALTER COLUMN country_index TYPE VARCHAR(10),
ALTER COLUMN phone_number TYPE VARCHAR(30),
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid,
ALTER COLUMN join_date TYPE DATE USING join_date::date;

-- Altering schema for Orders_Table 

ALTER TABLE orders_table 
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid,
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid,
ALTER COLUMN card_key TYPE BIGINT,
ALTER COLUMN date_key TYPE BIGINT, 
ALTER COLUMN product_key TYPE BIGINT, 
ALTER COLUMN store_key TYPE BIGINT,
ALTER COLUMN user_key TYPE BIGINT,
ALTER COLUMN currency_key TYPE BIGINT,
ALTER COLUMN card_number TYPE VARCHAR(30),
ALTER COLUMN store_code TYPE VARCHAR(30),
ALTER COLUMN product_code TYPE VARCHAR(30),
ALTER COLUMN product_quantity TYPE SMALLINT, 
ALTER COLUMN country_code TYPE VARCHAR(10); 

-- Altering schema for dim_currency table 

ALTER TABLE dim_currency
ALTER COLUMN country_name TYPE VARCHAR(100),
ALTER COLUMN currency_code TYPE VARCHAR(10),
ALTER COLUMN country_code TYPE VARCHAR(5),
ALTER COLUMN currency_symbol TYPE VARCHAR(5);

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
ALTER COLUMN card_provider TYPE character varying(255),
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
ALTER COLUMN company TYPE VARCHAR(255),
ALTER COLUMN email_address TYPE VARCHAR(255),
ALTER COLUMN address TYPE VARCHAR(500),
ALTER COLUMN country_index TYPE VARCHAR(10),
ALTER COLUMN phone_number TYPE VARCHAR(30),
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

-- Altering table names for dim_products_table and dim_date_times 
ALTER TABLE dim_products 
RENAME "EAN" TO EAN;

