-- Table: public.land_date_times

DROP TABLE IF EXISTS public.land_date_times;

CREATE TABLE IF NOT EXISTS public.land_date_times
(
    index bigint,
    date_key bigint,
    "timestamp" time without time zone,
    day character varying(30) COLLATE pg_catalog."default",
    month character varying(30) COLLATE pg_catalog."default",
    year character varying(30) COLLATE pg_catalog."default",
    time_period character varying(40) COLLATE pg_catalog."default",
    date_uuid uuid NOT NULL
)

TABLESPACE pg_default;

-- Index: ix_land_date_times_index

DROP INDEX IF EXISTS public.ix_land_date_times_index;

CREATE INDEX ix_land_date_times_index
    ON public.land_date_times USING btree
    (index ASC NULLS LAST)
    TABLESPACE pg_default;