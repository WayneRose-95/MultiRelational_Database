-- Table: public.dim_date_times

DROP TABLE IF EXISTS public.dim_date_times;

CREATE TABLE IF NOT EXISTS public.dim_date_times
(
    index bigint,
    "timestamp" text COLLATE pg_catalog."default",
    month character varying(30) COLLATE pg_catalog."default",
    year character varying(30) COLLATE pg_catalog."default",
    day character varying(30) COLLATE pg_catalog."default",
    time_period character varying(40) COLLATE pg_catalog."default",
    date_uuid uuid
)

TABLESPACE pg_default;

DROP INDEX IF EXISTS public.ix_dim_date_times_index;

CREATE INDEX ix_dim_date_times_index
    ON public.dim_date_times USING btree
    (index ASC NULLS LAST)
    TABLESPACE pg_default;