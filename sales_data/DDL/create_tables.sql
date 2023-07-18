-- Table: public.dim_users

DROP TABLE IF EXISTS public.dim_users;

CREATE TABLE IF NOT EXISTS public.dim_users
(
    index bigint,
    user_key bigint NOT NULL,
    first_name character varying(255) COLLATE pg_catalog."default",
    last_name character varying(255) COLLATE pg_catalog."default",
    date_of_birth date,
    company character varying(255) COLLATE pg_catalog."default",
    email_address character varying(255) COLLATE pg_catalog."default",
    address character varying(500) COLLATE pg_catalog."default",
    country character varying(100) COLLATE pg_catalog."default",
    country_code character varying(20) COLLATE pg_catalog."default",
    phone_number character varying(40) COLLATE pg_catalog."default",
    join_date date,
    user_uuid uuid,
    CONSTRAINT dim_users_pkey PRIMARY KEY (user_key)
)

TABLESPACE pg_default;


DROP INDEX IF EXISTS public.ix_dim_users_index;

CREATE INDEX ix_dim_users_index
    ON public.dim_users USING btree
    (index ASC NULLS LAST)
    TABLESPACE pg_default;
	

-- Table: public.dim_card_details

DROP TABLE IF EXISTS public.dim_card_details;

CREATE TABLE IF NOT EXISTS public.dim_card_details
(
    index bigint,
    card_key bigint NOT NULL,
    card_number character varying(30) COLLATE pg_catalog."default",
    expiry_date character varying(20) COLLATE pg_catalog."default",
    card_provider character varying(255) COLLATE pg_catalog."default",
    date_payment_confirmed date,
    CONSTRAINT dim_card_details_pkey PRIMARY KEY (card_key)
)

TABLESPACE pg_default;

DROP INDEX IF EXISTS public.ix_dim_card_details_index;

CREATE INDEX ix_dim_card_details_index
    ON public.dim_card_details USING btree
    (index ASC NULLS LAST)
    TABLESPACE pg_default;


DROP TABLE IF EXISTS public.dim_currency_conversion;

CREATE TABLE IF NOT EXISTS public.dim_currency_conversion
(
    index bigint,
    currency_conversion_key bigint NOT NULL,
    currency_name character varying(50) COLLATE pg_catalog."default",
    currency_code character varying(5) COLLATE pg_catalog."default",
    conversion_rate numeric(20,6),
    percentage_change numeric(20,6),
    last_updated timestamp with time zone,
    CONSTRAINT dim_currency_conversion_pkey PRIMARY KEY (currency_conversion_key)
)

TABLESPACE pg_default;


-- Index: ix_dim_currency_conversion_index

DROP INDEX IF EXISTS public.ix_dim_currency_conversion_index;

CREATE INDEX ix_dim_currency_conversion_index
    ON public.dim_currency_conversion USING btree
    (index ASC NULLS LAST)
    TABLESPACE pg_default;
	


-- Table: public.dim_currency
DROP TABLE IF EXISTS public.dim_currency;

CREATE TABLE IF NOT EXISTS public.dim_currency
(
    index bigint,
    currency_key bigint NOT NULL,
	currency_conversion_key bigint,
    country_name character varying(100) COLLATE pg_catalog."default",
    currency_code character varying(20) COLLATE pg_catalog."default",
    country_code character varying(5) COLLATE pg_catalog."default",
    currency_symbol character varying(5) COLLATE pg_catalog."default",
    CONSTRAINT dim_currency_pkey PRIMARY KEY (currency_key)
    
)

TABLESPACE pg_default;
-- Index: ix_dim_currency_index

DROP INDEX IF EXISTS public.ix_dim_currency_index;

CREATE INDEX ix_dim_currency_index
    ON public.dim_currency USING btree
    (index ASC NULLS LAST)
    TABLESPACE pg_default;


-- Table: public.dim_date_times

DROP TABLE IF EXISTS public.dim_date_times;

CREATE TABLE IF NOT EXISTS public.dim_date_times
(
    index bigint,
    date_key bigint NOT NULL,
    event_time time without time zone,
    month character varying(30) COLLATE pg_catalog."default",
    year character varying(30) COLLATE pg_catalog."default",
    day character varying(30) COLLATE pg_catalog."default",
    time_period character varying(40) COLLATE pg_catalog."default",
    date_uuid uuid,
    CONSTRAINT dim_date_times_pkey PRIMARY KEY (date_key)
)

TABLESPACE pg_default;

-- Index: ix_dim_date_times_index

DROP INDEX IF EXISTS public.ix_dim_date_times_index;

CREATE INDEX ix_dim_date_times_index
    ON public.dim_date_times USING btree
    (index ASC NULLS LAST)
    TABLESPACE pg_default;


	
-- Table: public.dim_product_details 
DROP TABLE IF EXISTS public.dim_product_details;

CREATE TABLE IF NOT EXISTS public.dim_product_details
(
    index bigint,
    product_key bigint NOT NULL ,
    EAN character varying(50) COLLATE pg_catalog."default",
    product_name character varying(500) COLLATE pg_catalog."default",
    product_price double precision,
    weight double precision,
    weight_class character varying(50) COLLATE pg_catalog."default",
    category character varying(50) COLLATE pg_catalog."default",
    date_added date,
    uuid uuid,
    availability boolean,
    product_code character varying(50) COLLATE pg_catalog."default",
    CONSTRAINT dim_product_details_pkey PRIMARY KEY (product_key)
)

TABLESPACE pg_default;

-- Index: ix_dim_product_details_index
DROP INDEX IF EXISTS public.ix_dim_product_details_index;

CREATE INDEX ix_dim_product_details_index
    ON public.dim_product_details USING btree
    (index ASC NULLS LAST)
    TABLESPACE pg_default;
	
	
	
DROP TABLE IF EXISTS public.orders_table;

CREATE TABLE IF NOT EXISTS public.orders_table
(
    index bigint,
    order_key bigint NOT NULL,
    date_uuid uuid,
    user_uuid uuid,
	card_key bigint,
    date_key bigint,
    product_key bigint,
    store_key bigint,
    user_key bigint,
    currency_key bigint,
    card_number character varying(30) COLLATE pg_catalog."default",
    store_code character varying(30) COLLATE pg_catalog."default",
    product_code character varying(30) COLLATE pg_catalog."default",
    product_quantity smallint,
    country_code character varying(10) COLLATE pg_catalog."default",
    CONSTRAINT orders_table_pkey PRIMARY KEY (order_key)
)

TABLESPACE pg_default;

-- Index: ix_orders_table_index

DROP INDEX IF EXISTS public.ix_orders_table_index;

CREATE INDEX ix_orders_table_index
    ON public.orders_table USING btree
    (index ASC NULLS LAST)
    TABLESPACE pg_default;


DROP TABLE IF EXISTS public.dim_store_details;

CREATE TABLE IF NOT EXISTS public.dim_store_details
(
    index bigint,
    store_key bigint NOT NULL,
    store_address character varying (1000) COLLATE pg_catalog."default",
    longitude double precision,
    latitude double precision,
    city character varying(255) COLLATE pg_catalog."default",
    store_code character varying(20) COLLATE pg_catalog."default",
    number_of_staff smallint,
    opening_date date,
    store_type character varying(255) COLLATE pg_catalog."default",
    country_code character varying(10) COLLATE pg_catalog."default",
    region character varying(255) COLLATE pg_catalog."default",
    CONSTRAINT dim_store_details_pkey PRIMARY KEY (store_key)
)

TABLESPACE pg_default;


