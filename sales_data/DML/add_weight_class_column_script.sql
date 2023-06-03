-- Script to add a new column: weight_class based on the weight of the item 

ALTER TABLE dim_product_details 
ADD COLUMN weight_class VARCHAR(255);

UPDATE dim_product_details
SET weight_class = 
  CASE
    WHEN weight < 2 THEN 'Light'
    WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized'
    WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
    WHEN weight >= 140 THEN 'Truck_Required'
    ELSE 'Unknown' -- Optional, if there are any unexpected values
  END;