-- 04.06.2023 WR Initial Draft 

-- Table: public.orders_table

DROP TABLE IF EXISTS public.orders_table;

CREATE TABLE IF NOT EXISTS public.orders_table
(
    index bigint,
    order_key bigint NOT NULL,
    date_uuid uuid,
    user_uuid uuid,
    card_number character varying(30) COLLATE pg_catalog."default",
    store_code character varying(30) COLLATE pg_catalog."default",
    product_code character varying(30) COLLATE pg_catalog."default",
    product_quantity smallint,
    card_key bigint,
    date_key bigint,
    product_key bigint,
    store_key bigint,
    user_key bigint,
    CONSTRAINT orders_table_pkey PRIMARY KEY (order_key),
    CONSTRAINT fk_dim_card_details FOREIGN KEY (card_key)
        REFERENCES public.dim_card_details (card_key) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_dim_date_times FOREIGN KEY (date_key)
        REFERENCES public.dim_date_times (time_key) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_dim_product_details FOREIGN KEY (product_key)
        REFERENCES public.dim_product_details (product_key) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_dim_store_details FOREIGN KEY (store_key)
        REFERENCES public.dim_store_details (store_key) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_dim_users FOREIGN KEY (user_key)
        REFERENCES public.dim_users (user_key) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
)

TABLESPACE pg_default;



DROP INDEX IF EXISTS public.ix_orders_table_index;

CREATE INDEX ix_orders_table_index
    ON public.orders_table USING btree
    (index ASC NULLS LAST)
    TABLESPACE pg_default;