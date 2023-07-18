from database_scripts.file_handler import get_absolute_file_path
from database_scripts.data_cleaning import DataCleaning
from database_scripts.sql_transformations import SQLAlterations
from sqlalchemy import   VARCHAR, DATE, FLOAT, SMALLINT, BOOLEAN, TIME, NUMERIC, TIMESTAMP
from sqlalchemy.dialects.postgresql import BIGINT, UUID
import time
import os
import logging


"""
LOG DEFINITION
"""
log_filename = get_absolute_file_path("main.log", "logs")  # "logs/main.log"
if not os.path.exists(log_filename):
    os.makedirs(os.path.dirname(log_filename), exist_ok=True)

main_logger = logging.getLogger(__name__)

# Set the default level as DEBUG
main_logger.setLevel(logging.DEBUG)

# Format the logs by time, filename, function_name, level_name and the message
format = logging.Formatter(
    "%(asctime)s:%(filename)s:%(funcName)s:%(levelname)s:%(message)s"
)
file_handler = logging.FileHandler(log_filename)

# Set the formatter to the variable format

file_handler.setFormatter(format)

main_logger.addHandler(file_handler)

# Setting file pathways to source files and credentials files
file_pathway_to_source_database = get_absolute_file_path("db_creds.yaml", "credentials")
file_pathway_to_datastore = get_absolute_file_path(
    "sales_data_creds_dev.yaml", "credentials"
)
file_pathway_to_source_text_file = get_absolute_file_path(
    "currency_code_mapping", "source_data_files"
)
file_pathway_to_json_source_file = get_absolute_file_path(
    "country_data.json", "source_data_files"
)
file_pathway_to_exported_csv_file = get_absolute_file_path(
    "currency_conversions", "source_data_files"
)

sql_transformations_file_path = get_absolute_file_path(
    "sales_data_creds_dev.yaml", "credentials"
)

start_time = time.time()

main_logger.info("Process started")

# Create instance of DataCleaning and SQLAlterations classes
cleaner = DataCleaning(file_pathway_to_datastore)
sql = SQLAlterations(get_absolute_file_path("sales_data_creds_dev.yaml", "credentials"))

# Create the database. If it already exists
sql.create_database("sales_data_dev")  # 'Sales_Data_Test', "Sales_Data_Admin"

# Main ETL Process to Extract, Transform and Load Data into Postgres
land_user_table = cleaner.clean_user_data(
    "legacy_users", 
    file_pathway_to_source_database, 
    "postgres"
)
cleaner._upload_to_database(
    land_user_table,
    cleaner.engine,
    "land_user_data",
    "replace",
    datastore_column_datatypes={
        "user_key": BIGINT,
        "first_name": VARCHAR(255),
        "last_name": VARCHAR(255),
        "date_of_birth": DATE,
        "company": VARCHAR(255),
        "email_address": VARCHAR(255),
        "address": VARCHAR(600),
        "country": VARCHAR(100),
        "country_code": VARCHAR(20),
        "phone_number": VARCHAR(50),
        "join_date": DATE,
        "user_uuid": UUID  
    }

)

dim_users_table = cleaner.load_dimension_table(
    land_user_table, 
    "dim_users",
    "append",
    dim_column_datatypes={
        "index": BIGINT,
        "user_key": BIGINT,
        "first_name": VARCHAR(255),
        "last_name": VARCHAR(255),
        "date_of_birth": DATE,
        "company": VARCHAR(255),
        "email_address": VARCHAR(255),
        "address": VARCHAR(600),
        "country": VARCHAR(100),
        "country_code": VARCHAR(20),
        "phone_number": VARCHAR(50),
        "join_date": DATE,
        "user_uuid": UUID,
    }
)

land_store_table = cleaner.clean_store_data(
    "legacy_store_details",
    file_pathway_to_source_database,
    "postgres"   
)

cleaner._upload_to_database(
    land_store_table,
    cleaner.engine,
    "land_store_details",
    "replace",
    datastore_column_datatypes={
    "index": BIGINT,
    "store_key": BIGINT,
    "store_address": VARCHAR(1000),
    "longitude": FLOAT,
    "latitude": FLOAT,
    "city": VARCHAR(255),
    "store_code": VARCHAR(20),
    "number_of_staff": SMALLINT,
    "opening_date": DATE,
    "store_type": VARCHAR(255),
    "country_code": VARCHAR(20),
    "region": VARCHAR(255)
}
)

dim_store_table = cleaner.load_dimension_table(
    land_store_table, 
    "dim_store_details",
    "append",
    dim_column_datatypes= {
    "index": BIGINT,
    "store_key": BIGINT,
    "store_address": VARCHAR(1000),
    "longitude": FLOAT,
    "latitude": FLOAT,
    "city": VARCHAR(255),
    "store_code": VARCHAR(20),
    "number_of_staff": SMALLINT,
    "opening_date": DATE,
    "store_type": VARCHAR(255),
    "country_code": VARCHAR(20),
    "region": VARCHAR(255)
}
    )

land_product_table = cleaner.clean_product_table(
    "s3://data-handling-public/products.csv"
)

cleaner._upload_to_database(
    land_product_table,
    cleaner.engine,
    "land_product_details",
    "replace",
    datastore_column_datatypes={
        "index": BIGINT,
        "product_key": BIGINT,
        "ean": VARCHAR(50),
        "product_name": VARCHAR(500),
        "product_price": FLOAT,
        "weight": FLOAT,
        "weight_class": VARCHAR(50),
        "category": VARCHAR(50),
        "date_added": DATE,
        "uuid": UUID,
        "availability": VARCHAR(30),
        "product_code": VARCHAR(50)
    } 
)

dim_product_details_table = cleaner.load_dimension_table(
    land_product_table,
    "dim_product_details",
    "append",
    mapping={"Still_avaliable": True, "Removed": False},
        dim_column_datatypes={
        "index": BIGINT,
        "product_key": BIGINT,
        "ean": VARCHAR(50),
        "product_name": VARCHAR(500),
        "product_price": FLOAT,
        "weight": FLOAT,
        "weight_class": VARCHAR(50),
        "category": VARCHAR(50),
        "date_added": DATE,
        "uuid": UUID,
        "availability": BOOLEAN,
        "product_code": VARCHAR(50)
    } 
)

land_time_events_table = cleaner.clean_time_event_table(
    "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json",
)

cleaner._upload_to_database(
    land_time_events_table,
    cleaner.engine,
    "land_date_times",
    "replace",
    datastore_column_datatypes={
    "index": BIGINT,
    "date_key": BIGINT,
    "event_time": TIME,
    "day": VARCHAR(30),
    "month": VARCHAR(30),
    "year": VARCHAR(30),
    "time_period": VARCHAR(40),
    "date_uuid": UUID
} 
)

dim_date_times_table = cleaner.load_dimension_table(
    land_time_events_table, 
    "dim_date_times",
    "append",
    dim_column_datatypes={
    "index": BIGINT,
    "date_key": BIGINT,
    "event_time": TIME,
    "day": VARCHAR(30),
    "month": VARCHAR(30),
    "year": VARCHAR(30),
    "time_period": VARCHAR(40),
    "date_uuid": UUID
} 
)


land_card_details_table = cleaner.clean_card_details(
    "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
)

cleaner._upload_to_database(
    land_card_details_table,
    cleaner.engine,
    "land_card_details",
    "replace",
    datastore_column_datatypes={
    "index": BIGINT,
    "card_key": BIGINT,
    "card_number": VARCHAR(30),
    "expiry_date": VARCHAR(20),
    "card_provider": VARCHAR(255),
    "date_payment_confirmed": DATE
} 
)

dim_card_details_table = cleaner.load_dimension_table(
    land_card_details_table, 
    "dim_card_details",
    "append",
    dim_column_datatypes={
    "index": BIGINT,
    "card_key": BIGINT,
    "card_number": VARCHAR(30),
    "expiry_date": VARCHAR(20),
    "card_provider": VARCHAR(255),
    "date_payment_confirmed": DATE
    }

)

land_currency_table = cleaner.clean_currency_table(
    file_pathway_to_json_source_file
) 

cleaner._upload_to_database(
    land_currency_table,
    cleaner.engine,
    "land_currency",
    "replace",
    datastore_column_datatypes={
    "index": BIGINT,
    "currency_key": BIGINT,
    "currency_conversion_key": BIGINT,
    "country_name": VARCHAR(100),
    "currency_code": VARCHAR(20),
    "country_code": VARCHAR(5),
    "currency_symbol": VARCHAR(5)
} 

) 


dim_currency_table = cleaner.load_dimension_table(
    land_currency_table,
    "dim_currency",
    subset=["US", "GB", "DE"],
    additional_rows=[
        {
            "currency_key": -1,
            "currency_conversion_key": -1,
            "currency_code": "Not Applicable",
        },
        {
            "currency_key": 0,
            "currency_conversion_key": 0,
            "currency_code": "Unknown",
        },
    ],
    dim_column_datatypes={
    "index": BIGINT,
    "currency_key": BIGINT,
    "currency_conversion_key": BIGINT,
    "country_name": VARCHAR(100),
    "currency_code": VARCHAR(20),
    "country_code": VARCHAR(5),
    "currency_symbol": VARCHAR(5)
} 

)

land_currency_conversions_table = cleaner.clean_currency_exchange_rates(
    "https://www.x-rates.com/table/?from=GBP&amount=1",
    '//table[@class="tablesorter ratesTable"]/tbody',
    '//*[@id="content"]/div[1]/div/div[1]/div[1]/span[2]',
    ["currency_name", "conversion_rate", "percentage_change"],
    file_pathway_to_exported_csv_file,
    file_pathway_to_source_text_file
)

cleaner._upload_to_database(
    land_currency_conversions_table,
    cleaner.engine,
    "land_currency_conversion",
    "replace",
    datastore_column_datatypes={
    "index": BIGINT,
    "currency_conversion_key": BIGINT,
    "currency_name": VARCHAR(50),
    "currency_code": VARCHAR(5),
    "conversion_rate": NUMERIC(20,6),
    "percentage_change": NUMERIC(20,6),
    "last_updated" : TIMESTAMP(timezone=True)

} 
) 

dim_currency_conversion_table = cleaner.load_dimension_table(
    land_currency_conversions_table,
    "dim_currency_conversion",
    subset=["USD", "GBP", "EUR"],
    additional_rows=[
        {"currency_conversion_key": -1, "currency_name": "Not Applicable"},
        {"currency_conversion_key": 0, "currency_name": "Unknown"}
    ],
    dim_column_datatypes={
    "index": BIGINT,
    "currency_conversion_key": BIGINT,
    "currency_name": VARCHAR(50),
    "currency_code": VARCHAR(5),
    "conversion_rate": NUMERIC(20,6),
    "percentage_change": NUMERIC(20,6),
    "last_updated" : TIMESTAMP(timezone=True)
    }
)

orders_table = cleaner.clean_orders_table(
    "orders_table", 
    file_pathway_to_source_database, 
    "postgres"
)

cleaner._upload_to_database(
    orders_table,
    cleaner.engine,
    "orders_table",
    "append",
    datastore_column_datatypes={
    "index": BIGINT,
    "order_key": BIGINT,
    "date_uuid": UUID,
    "user_uuid": UUID,
    "card_key": BIGINT,
    "date_key": BIGINT,
    "product_key": BIGINT,
    "store_key": BIGINT,
    "user_key": BIGINT,
    "currency_key": BIGINT,
    "card_number": VARCHAR(30),
    "store_code": VARCHAR(30),
    "product_code": VARCHAR(30),
    "product_quantity": SMALLINT,
    "country_code": VARCHAR(20)
}
)


# Altering the schema and forming the STAR Schema model using the uploaded database
sql.connect_to_database(
    get_absolute_file_path("sales_data_creds_dev.yaml", "credentials"), "sales_data_dev"
)

sql.alter_and_update(
    get_absolute_file_path("add_weight_class_column_script.sql", r"sales_data\DML")
)

sql.alter_and_update(
    get_absolute_file_path("add_primary_keys.sql", r"sales_data\DDL")
    )

sql.alter_and_update(
    get_absolute_file_path("foreign_key_constraints.sql", r"sales_data\DDL")
)

sql.alter_and_update(
    get_absolute_file_path(
        "update_foreign_keys.sql", r"sales_data\DML"
    )
)  


end_time = time.time()

execution_time = end_time - start_time

print(f"Execution time: {execution_time} seconds")
main_logger.info(f"Time elapsed : {execution_time} seconds")
