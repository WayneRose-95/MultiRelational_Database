-- View: public.customer_sales_ranked_view

-- DROP VIEW public.customer_sales_ranked_view;

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