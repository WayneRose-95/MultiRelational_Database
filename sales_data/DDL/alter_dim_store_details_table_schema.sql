ALTER TABLE dim_store_details
ALTER COLUMN longitude TYPE FLOAT USING longitude::double precision,
ALTER COLUMN latitude TYPE FLOAT USING latitude::double precision,
ALTER COLUMN store_code TYPE VARCHAR(20),
ALTER COLUMN city TYPE VARCHAR(255),
ALTER COLUMN number_of_staff TYPE SMALLINT USING number_of_staff::smallint;
ALTER COLUMN opening_date TYPE DATE USING opening_date::date;,
ALTER COLUMN store_type TYPE VARCHAR(255),
ALTER COLUMN country_code TYPE VARCHAR(10),
ALTER COLUMN region TYPE VARCHAR(255);

/* NOTES / BUGS  
Currently the following lines do not work: 

1. ALTER COLUMN longitude TYPE FLOAT USING longitude::double precision;

Error produced 
ERROR:  invalid input syntax for type double precision: "N/A"
SQL state: 22P02

Solution: locate and change the value to NULL 

2. ALTER COLUMN number_of_staff TYPE SMALLINT USING number_of_staff::smallint;

Error produced
ERROR:  invalid input syntax for type smallint: "J78"
SQL state: 22P02

Solution: locate and change the value from J78 to 78 */
