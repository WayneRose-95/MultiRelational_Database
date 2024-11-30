-- 11.06.2023 WR Removed column addition to move into column_additions SQL Script

-- Script to add a new column: weight_class based on the weight of the item 

UPDATE dim_product
SET weight_class = 
  CASE
    WHEN weight < 2 THEN 'Light'
    WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized'
    WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
    WHEN weight >= 140 THEN 'Truck_Required'
    ELSE 'Unknown' -- Optional, if there are any unexpected values
  END;