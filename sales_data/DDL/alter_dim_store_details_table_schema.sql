-- 04.06.2023 WR Fixed bugs for longitude and number_of_staff columns. 

ALTER TABLE dim_store_details_test
ALTER COLUMN longitude TYPE FLOAT USING longitude::double precision,
ALTER COLUMN latitude TYPE FLOAT USING latitude::double precision,
ALTER COLUMN store_code TYPE VARCHAR(20),
ALTER COLUMN city TYPE VARCHAR(255),
ALTER COLUMN number_of_staff TYPE SMALLINT USING number_of_staff::smallint,
ALTER COLUMN opening_date TYPE DATE USING opening_date::date,
ALTER COLUMN store_type TYPE VARCHAR(255),
ALTER COLUMN country_code TYPE VARCHAR(10),
ALTER COLUMN region TYPE VARCHAR(255);


