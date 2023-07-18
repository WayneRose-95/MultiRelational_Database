-- Table: public.land_currency

DROP TABLE IF EXISTS public.land_currency;

CREATE TABLE IF NOT EXISTS public.land_currency
(
    index bigint,
    currency_key bigint,
    country_name character varying(100) COLLATE pg_catalog."default",
    currency_code character varying(10) COLLATE pg_catalog."default",
    country_code character varying(5) COLLATE pg_catalog."default",
    currency_symbol character varying(5) COLLATE pg_catalog."default"
)

TABLESPACE pg_default;


-- Index: ix_land_currency_index

DROP INDEX IF EXISTS public.ix_land_currency_index;

CREATE INDEX ix_land_currency_index
    ON public.land_currency USING btree
    (index ASC NULLS LAST)
    TABLESPACE pg_default;