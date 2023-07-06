-- Table: public.dim_currency_conversion

DROP TABLE IF EXISTS public.dim_currency_conversion;

CREATE TABLE IF NOT EXISTS public.dim_currency_conversion
(
    index bigint,
    currency_conversion_key bigint NOT NULL,
    currency_name character varying(50) COLLATE pg_catalog."default",
    currency_code character varying(5) COLLATE pg_catalog."default",
    conversion_rate numeric(20,6),
    conversion_rate_percentage numeric(20,6),
    last_updated timestamp with time zone,
    CONSTRAINT dim_currency_conversion_pkey PRIMARY KEY (currency_conversion_key)
)

TABLESPACE pg_default;


-- Index: ix_dim_currency_conversion_index

DROP INDEX IF EXISTS public.ix_dim_currency_conversion_index;

CREATE INDEX ix_dim_currency_conversion_index
    ON public.dim_currency_conversion USING btree
    (index ASC NULLS LAST)
    TABLESPACE pg_default;