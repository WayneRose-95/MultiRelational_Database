-- Table: public.land_product_details

DROP TABLE IF EXISTS public.land_product_details;

CREATE TABLE IF NOT EXISTS public.land_product_details
(
    index bigint,
    product_key bigint,
    "EAN" character varying(50) COLLATE pg_catalog."default",
    product_name character varying(500) COLLATE pg_catalog."default",
    product_price double precision,
    weight double precision,
    category character varying(50) COLLATE pg_catalog."default",
    date_added timestamp without time zone,
    uuid uuid NOT NULL,
    availability text COLLATE pg_catalog."default",
    product_code character varying(50) COLLATE pg_catalog."default"
)

TABLESPACE pg_default;


-- Index: ix_land_product_details_index

DROP INDEX IF EXISTS public.ix_land_product_details_index;

CREATE INDEX ix_land_product_details_index
    ON public.land_product_details USING btree
    (index ASC NULLS LAST)
    TABLESPACE pg_default;