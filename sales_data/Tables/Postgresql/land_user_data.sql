-- Table: public.land_user_data

-- Table: public.land_user_data

DROP TABLE IF EXISTS public.land_user_data;

CREATE TABLE IF NOT EXISTS public.land_user_data
(
    index bigint,
    user_key bigint,
    first_name character varying(255) COLLATE pg_catalog."default",
    last_name character varying(255) COLLATE pg_catalog."default",
    birth_date date,
    company character varying(255) COLLATE pg_catalog."default",
    email_address character varying(255) COLLATE pg_catalog."default",
    address character varying(600) COLLATE pg_catalog."default",
    country character varying(100) COLLATE pg_catalog."default",
    country_index character varying(10) COLLATE pg_catalog."default",
    phone_number character varying(50) COLLATE pg_catalog."default",
    join_date date,
    user_uuid uuid 
)

TABLESPACE pg_default;

-- Index: ix_land_user_data_index

DROP INDEX IF EXISTS public.ix_land_user_data_index;

CREATE INDEX ix_land_user_data_index
    ON public.land_user_data USING btree
    (index ASC NULLS LAST)
    TABLESPACE pg_default;