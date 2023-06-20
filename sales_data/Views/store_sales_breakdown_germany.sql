-- View: public.store_sales_breakdown_germany

-- DROP VIEW public.store_sales_breakdown_germany;

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
     JOIN dim_date_times dt ON o.date_key = dt.time_key
  WHERE ds.country_code::text ~~ 'DE'::text
  GROUP BY ds.store_type, ds.country_code, ds.city, ds.store_address, dt.day, dt.month, dt.year
  ORDER BY (round(sum(dp.product_price * o.product_quantity::double precision)::numeric, 2)) DESC;

ALTER TABLE public.store_sales_breakdown_germany
    OWNER TO postgres;
