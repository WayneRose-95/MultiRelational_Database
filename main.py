from sql_transformations import SQLAlterations
from data_cleaning import DataCleaning

# Script to Clean the data and upload it to the database using the specified config file 

cleaner = DataCleaning('sales_data_creds_dev.yaml')
cleaner.clean_user_data("legacy_users", 'db_creds.yaml', "dim_users",)
cleaner.clean_store_data("legacy_store_details", "db_creds.yaml", "dim_store_details")
cleaner.clean_card_details(
        "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf",
        "dim_card_details"
    ) 
cleaner.clean_orders_table("orders_table", "db_creds.yaml", "orders_table") 
cleaner.clean_time_event_table(
    "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json",
    "dim_date_times"
)
cleaner.clean_product_table(
    "s3://data-handling-public/products.csv",
    "dim_product_details"
) 

# Scripts to transform and update the tables schema, add columns to tables, add primary keys to tables, and map foreign key constraints to orders_table 

sql_statements = SQLAlterations('sales_data_creds_dev.yaml')
sql_statements.connect_to_database()
# Alter the table_schema of every table except dim_product_details 
sql_statements.alter_and_update(r'sales_data\DDL\alter_table_schema.sql') 
# Add in the necessary columns 
sql_statements.alter_and_update(r'sales_data\DDL\column_additions.sql') 
# SQL script to create the logic for the weight_class column added previously. 
sql_statements.alter_and_update(r'sales_data\DML\add_weight_class_column_script.sql')
# Alter the schema of the dim_product_details table 
sql_statements.alter_and_update(r'sales_data\DDL\alter_dim_product_details_table_schema.sql') 
# Next, add the primary keys to the tables 
sql_statements.alter_and_update(r'sales_data\DDL\add_primary_keys.sql')
# Then add the foreign key constraints to the orders_table 
sql_statements.alter_and_update(r'sales_data\DDL\orders_table_FK_constraints.sql')
# Lastly map the dimension keys in the dim tables to the foreign keys in the orders_table 
sql_statements.alter_and_update(r'sales_data\DML\update_orders_table_foreign_keys.sql')

sql_statements.alter_and_update(r'sales_data\DML\fill_null_keys_in_orders_table.sql')