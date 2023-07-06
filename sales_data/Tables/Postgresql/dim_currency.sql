-- Table: public.dim_currency

DROP TABLE IF EXISTS public.dim_currency;

CREATE TABLE IF NOT EXISTS public.dim_currency
(
    index bigint,
    currency_key bigint NOT NULL,
    country_name character varying(100) COLLATE pg_catalog."default",
    currency_code character varying(10) COLLATE pg_catalog."default",
    country_code character varying(5) COLLATE pg_catalog."default",
    currency_symbol text COLLATE pg_catalog."default",
    currency_conversion_key bigint,
    CONSTRAINT dim_currency_pkey PRIMARY KEY (currency_key),
    CONSTRAINT fk_dim_currency_conversion FOREIGN KEY (currency_conversion_key)
        REFERENCES public.dim_currency_conversion (currency_conversion_key) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
)

TABLESPACE pg_default;

-- Index: ix_dim_currency_index

DROP INDEX IF EXISTS public.ix_dim_currency_index;

CREATE INDEX ix_dim_currency_index
    ON public.dim_currency USING btree
    (index ASC NULLS LAST)
    TABLESPACE pg_default;