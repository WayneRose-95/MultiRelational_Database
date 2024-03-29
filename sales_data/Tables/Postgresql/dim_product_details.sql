-- Table: public.dim_product_details

DROP TABLE IF EXISTS public.dim_product_details;

CREATE TABLE IF NOT EXISTS public.dim_product_details
(
    index bigint,
    product_key bigint NOT NULL,
    ean character varying(50) COLLATE pg_catalog."default",
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


DROP INDEX IF EXISTS public.ix_dim_product_details_index;

CREATE INDEX ix_dim_product_details_index
    ON public.dim_product_details USING btree
    (index ASC NULLS LAST)
    TABLESPACE pg_default;