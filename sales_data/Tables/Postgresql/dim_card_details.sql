-- Table: public.dim_card_details

DROP TABLE IF EXISTS public.dim_card_details;

CREATE TABLE IF NOT EXISTS public.dim_card_details
(
    index bigint,
    card_key bigint,
    card_number character varying(30) COLLATE pg_catalog."default",
    expiry_date character varying(10) COLLATE pg_catalog."default",
    card_provider character varying(255) COLLATE pg_catalog."default",
    date_payment_confirmed date
)

TABLESPACE pg_default;

DROP INDEX IF EXISTS public.ix_dim_card_details_index;

CREATE INDEX ix_dim_card_details_index
    ON public.dim_card_details USING btree
    (index ASC NULLS LAST)
    TABLESPACE pg_default;