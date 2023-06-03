ALTER TABLE dim_users
ALTER COLUMN first_name TYPE VARCHAR(255), 
ALTER COLUMN last_name TYPE VARCHAR(255),
ALTER COLUMN birth_date TYPE DATE USING birth_date::date,
ALTER COLUMN country_index TYPE VARCHAR(10),
ALTER COLUMN unique_id TYPE UUID USING unique_id::uuid,
ALTER COLUMN join_date TYPE DATE USING join_date::date;