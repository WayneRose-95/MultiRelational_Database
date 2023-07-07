-- 04.06.2023 WR Initial Draft 
-- Table: public.dim_store_details

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



DROP INDEX IF EXISTS public.ix_dim_store_details_index;

CREATE INDEX ix_dim_store_details_index
    ON public.dim_store_details USING btree
    (index ASC NULLS LAST)
    TABLESPACE pg_default;