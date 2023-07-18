-- 04.06.2023 WR Initial Draft 
-- 01.07.2023 WR Added currenty_key column for orders_table
-- 07.07.2023 WR Adjusted column order for orders table 
-- Table: public.orders_table

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
    country_code character varying(20) COLLATE pg_catalog."default",
    CONSTRAINT orders_table_pkey PRIMARY KEY (order_key)
)

TABLESPACE pg_default;

-- Index: ix_orders_table_index

DROP INDEX IF EXISTS public.ix_orders_table_index;

CREATE INDEX ix_orders_table_index
    ON public.orders_table USING btree
    (index ASC NULLS LAST)
    TABLESPACE pg_default;