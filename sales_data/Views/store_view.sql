-- View: public.store_view

-- DROP VIEW public.store_view;

CREATE OR REPLACE VIEW public.store_view
 AS
 SELECT dim_store_details.store_code,
    dim_store_details.store_type,
    dim_store_details.city,
    dim_store_details.store_address,
    dim_store_details.region,
        CASE
            WHEN dim_store_details.country_code::text = 'GB'::text THEN 'Great Britain'::text
            WHEN dim_store_details.country_code::text = 'DE'::text THEN 'Germany'::text
            WHEN dim_store_details.country_code::text = 'US'::text THEN 'United States'::text
            ELSE NULL::text
        END AS country_name,
    dim_store_details.longitude,
    dim_store_details.latitude,
    dim_store_details.number_of_staff
   FROM dim_store_details
  WHERE dim_store_details.country_code::text = ANY (ARRAY['GB'::character varying, 'DE'::character varying, 'US'::character varying]::text[]);

ALTER TABLE public.store_view
    OWNER TO postgres;
