-- Table: public.dim_product_details

DROP TABLE IF EXISTS public.dim_product_details;

CREATE TABLE IF NOT EXISTS public.dim_product_details
(
    index bigint,
    product_key bigint,
    "EAN" character varying(50) COLLATE pg_catalog."default",
    product_name character varying(500) COLLATE pg_catalog."default",
    product_price double precision,
    weight double precision,
    category character varying(50) COLLATE pg_catalog."default",
    date_added date,
    uuid uuid,
    availability boolean,
    product_code character varying(50) COLLATE pg_catalog."default",
    weight_class character varying(50) COLLATE pg_catalog."default"
)

TABLESPACE pg_default;


DROP INDEX IF EXISTS public.ix_dim_product_details_index;

CREATE INDEX ix_dim_product_details_index
    ON public.dim_product_details USING btree
    (index ASC NULLS LAST)
    TABLESPACE pg_default;