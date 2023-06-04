ALTER TABLE dim_product_details 
ALTER COLUMN product_price TYPE FLOAT USING product_price::double precision,
ALTER COLUMN weight TYPE FLOAT, 
ALTER COLUMN "EAN" TYPE VARCHAR(50),
ALTER COLUMN product_code TYPE VARCHAR(50),
ALTER COLUMN date_added TYPE DATE, 
ALTER COLUMN uuid TYPE uuid USING uuid::uuid,
ALTER COLUMN availability TYPE BOOLEAN USING (
	CASE
	WHEN availability = 'Removed' THEN false
 	WHEN availability = 'Still_avaliable' THEN true 
	END
	),
ALTER COLUMN weight_class TYPE VARCHAR(50);