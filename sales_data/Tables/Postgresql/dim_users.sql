-- Table: public.dim_users

DROP TABLE IF EXISTS public.dim_users;

CREATE TABLE IF NOT EXISTS public.dim_users
(
    index bigint,
    user_key bigint,
    first_name character varying(255) COLLATE pg_catalog."default",
    last_name character varying(255) COLLATE pg_catalog."default",
    date_of_birth date,
    company character varying(255) COLLATE pg_catalog."default",
    email_address character varying(255) COLLATE pg_catalog."default",
    address character varying(500) COLLATE pg_catalog."default",
    country character varying(100) COLLATE pg_catalog."default",
    country_code character varying(10) COLLATE pg_catalog."default",
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