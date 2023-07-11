-- View: public.store_sales_breakdown

-- DROP VIEW public.store_sales_breakdown;

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
