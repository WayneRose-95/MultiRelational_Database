CREATE OR REPLACE VIEW public.customer_sales_ranked_view
 AS
 SELECT du.first_name,
    du.last_name,
    round((x.product_price * o.product_quantity::double precision)::numeric, 2) AS sales,
    d.day,
    d.month,
    d.year,
    d.time_period
   FROM dim_product_details x
     JOIN orders_table o ON x.product_key = o.product_key
     JOIN dim_date_times d ON o.date_key = d.date_key
     JOIN dim_users du ON o.user_key = du.user_key
  ORDER BY (round((x.product_price * o.product_quantity::double precision)::numeric, 2)) DESC;

ALTER TABLE public.customer_sales_ranked_view
    OWNER TO postgres;


CREATE OR REPLACE VIEW public.store_sales_breakdown_germany
 AS
 SELECT ds.store_type,
    ds.city,
    ds.store_address,
    dt.day,
    dt.month,
    dt.year,
    round(sum(dp.product_price * o.product_quantity::double precision)::numeric, 2) AS total_sales
   FROM dim_store_details ds
     JOIN orders_table o ON ds.store_key = o.store_key
     JOIN dim_product_details dp ON o.product_key = dp.product_key
     JOIN dim_date_times dt ON o.date_key = dt.date_key
  WHERE ds.country_code::text ~~ 'DE'::text
  GROUP BY ds.store_type, ds.country_code, ds.city, ds.store_address, dt.day, dt.month, dt.year
  ORDER BY (round(sum(dp.product_price * o.product_quantity::double precision)::numeric, 2)) DESC;

ALTER TABLE public.store_sales_breakdown_germany
    OWNER TO postgres;



CREATE OR REPLACE VIEW public.store_sales_breakdown
 AS
 SELECT ds.store_type,
    ds.city,
    ds.region,
    dt.time_period,
    dt.day,
    dt.month,
    dt.year,
    round(sum(dp.product_price * o.product_quantity::double precision)::numeric, 2) AS total_sales,
    round((sum(dp.product_price * o.product_quantity::double precision) / sum(sum(dp.product_price * o.product_quantity::double precision)) OVER ())::numeric * 100::numeric, 2) AS percentage_total
   FROM dim_store_details ds
     JOIN orders_table o ON ds.store_key = o.store_key
     JOIN dim_product_details dp ON o.product_key = dp.product_key
     JOIN dim_date_times dt ON o.date_key = dt.date_key
  GROUP BY ds.store_type, dp.product_price, o.product_quantity, ds.city, ds.region, dt.day, dt.month, dt.year, dt.time_period
  ORDER BY (round((sum(dp.product_price * o.product_quantity::double precision) / sum(sum(dp.product_price * o.product_quantity::double precision)) OVER ())::numeric * 100::numeric, 2)) DESC;

ALTER TABLE public.store_sales_breakdown
    OWNER TO postgres;


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
