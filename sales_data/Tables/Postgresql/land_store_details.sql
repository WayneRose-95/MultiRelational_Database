-- Table: public.land_store_details

DROP TABLE IF EXISTS public.land_store_details;

CREATE TABLE IF NOT EXISTS public.land_store_details
(
    index bigint,
    store_key bigint,
    store_address text COLLATE pg_catalog."default",
    longitude double precision,
    latitude double precision,
    city character varying(255) COLLATE pg_catalog."default",
    store_code character varying(20) COLLATE pg_catalog."default",
    number_of_staff smallint,
    opening_date date,
    store_type character varying(255) COLLATE pg_catalog."default",
    country_code character varying(10) COLLATE pg_catalog."default",
    region character varying(255) COLLATE pg_catalog."default"
)

TABLESPACE pg_default;

-- Index: ix_land_store_details_index

DROP INDEX IF EXISTS public.ix_land_store_details_index;

CREATE INDEX ix_land_store_details_index
    ON public.land_store_details USING btree
    (index ASC NULLS LAST)
    TABLESPACE pg_default;