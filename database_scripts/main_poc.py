# Import the necessary modules for running the full process 
from database_scripts.database_utils import DatabaseConnector
from database_scripts.data_extraction import DatabaseExtractor
from database_scripts.data_cleaning import DataCleaning 
from database_scripts.sql_transformations import SQLAlterations
from database_scripts.currency_rate_extraction import CurrencyRateExtractor
from database_scripts.file_handler import get_absolute_file_path
from sqlalchemy import Column, VARCHAR, DATE, FLOAT, SMALLINT, BOOLEAN, TIME, NUMERIC, TIMESTAMP
from sqlalchemy.dialects.postgresql import BIGINT, UUID
from sqlalchemy.engine import Engine
# Built-in python module imports 
import logging 
import os 
import time 

"""
LOG DEFINITION
"""
log_filename = get_absolute_file_path("main_reworked.log", "logs")  # "logs/main_reworked.log"
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


# Step 1: Initialise the file paths to the credentials files, source data and .sql files
file_pathway_to_source_database = get_absolute_file_path("db_creds.yaml", "credentials")

file_pathway_to_datastore = get_absolute_file_path(
    "sales_data_creds_poc.yaml", "credentials"
)

file_pathway_to_source_text_file = get_absolute_file_path(
    "currency_code_mapping", "source_data_files"
)
file_pathway_to_json_source_file = get_absolute_file_path(
    "country_data.json", "source_data_files"
)

file_pathway_to_exported_csv_file = get_absolute_file_path(
    "currency_conversions_test", "source_data_files"
)

start_time = time.time()

main_logger.info("Process started")

# Step 2: Instantiate instances of the classes 

connector = DatabaseConnector() 
extractor = DatabaseExtractor() 
cleaner = DataCleaning()
sql_transformations = SQLAlterations(file_pathway_to_datastore)
currency_extractor = CurrencyRateExtractor(undetected_chrome=True)

# Step 3: Create the database.

create_sales_data_database = sql_transformations.create_database("sales_data_poc")
# Connecting to the database 
sales_data_database = sql_transformations.connect_to_database(file_pathway_to_datastore, "sales_data_poc")

# Step 4: Initialise the source and target engine objects from database_utils.py 

source_database_engine = connector.initialise_database_connection(file_pathway_to_source_database, True, 'postgres')
target_database_engine = connector.initialise_database_connection(file_pathway_to_datastore, True, 'sales_data_poc')

# Step 5 : List the database tables using the source_database_engine object 

# Reliant on the successful connection to the source and target database engine in order to work successfully. 
# As shown in step 3
source_database_table_names = connector.list_db_tables(source_database_engine)
target_database_table_names = connector.list_db_tables(target_database_engine)


# Step 6: Read in an RDS Table from the source database 

raw_user_data_table = extractor.read_rds_table('legacy_users', source_database_engine)
raw_orders_table = extractor.read_rds_table('orders_table', source_database_engine)
raw_store_details_table = extractor.read_rds_table('legacy_store_details', source_database_engine)

# Step 7: Retrieve details from other sources such as: 

# 7a. The pdf file which has the card details 
raw_card_details_table = extractor.retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")

# 7b. The s3 bucket which contains the .csv file on products 

raw_product_details_table = extractor.read_s3_bucket_to_dataframe("s3://data-handling-public/products.csv")

# 7c. The time even table which is hosted inside the s3 bucket 
raw_time_event_table = extractor.read_json_from_s3("https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json")

# 7d. The currency tables

# 7d (i) The currency table which contains currencies and their associated symbols 
raw_currency_data = extractor.read_json_local(file_pathway_to_json_source_file)

# 7d (ii) The currency conversion table which contains currencies and their exchange rates relative to GBP

#TODO: This method causes a traceback error with the Webdriver Manager package. 
# The traceback 
# ValueError: There is no such driver by url https://chromedriver.storage.googleapis.com/LATEST_RELEASE_119.0.6045
# Investigate this error by looking at the currency_rate_extractor.py script 
# For now, comment this out 

raw_currency_conversion_data, timestamp = currency_extractor.scrape_information(
        "https://www.x-rates.com/table/?from=GBP&amount=1",
        '//table[@class="tablesorter ratesTable"]/tbody',
        '//*[@id="content"]/div[1]/div/div[1]/div[1]/span[2]',
        ["currency_name", "conversion_rate", "conversion_rate_percentage"],
        file_pathway_to_exported_csv_file
)


# Step 8: Address the Cleaning Methods (BIG TASK! Will need to plan this out)

# == LAND TABLES == 

# Step 8a. Adjust the clean_user_table method to provide the cleaned land_user_data table 

cleaned_user_data_table = cleaner.clean_user_data(
    source_database_engine,
    raw_user_data_table,
    'legacy_users'
    )

# Step 8b. Adjust the clean_store_data method to produce the land_store_data table 

cleaned_store_data_table = cleaner.clean_store_data(
    source_database_engine, 
    raw_store_details_table,
    'legacy_store_details'
)

# Step 8c. Adjust the clean_card_details method to produce the land_card_details table 

cleaned_card_details_table = cleaner.clean_card_details(
     raw_card_details_table
)

# Step 8d. Adjust the clean_orders_table method to produce the orders_table 

cleaned_orders_table = cleaner.clean_orders_table(
    source_database_engine,
    raw_orders_table,
    'orders_table'
)

# Step 8e. Adjust the clean_time_event_table to produce the land_date_times table 

cleaned_time_event_table = cleaner.clean_time_event_table(
    raw_time_event_table
)

# Step 8f. Adjust the clean_product_table to producce the land_product_details table 

cleaned_product_table = cleaner.clean_product_table(
    raw_product_details_table
)

# Step 8g. Adjust the clean_currency_table to produce the land_currency table 

cleaned_currency_table = cleaner.clean_currency_table(
    raw_currency_data
)

# Step 8h. Adjust the clean_currency_exchange_rates method to produce the land_currency_exchange rates table

cleaned_currency_conversion_table = cleaner.clean_currency_exchange_rates(
    raw_currency_conversion_data,
    timestamp, 
    file_pathway_to_source_text_file

)

# == FOR LOADING DIMENSION TABLES == 

# Step 8i Adjust the upload_to_db method in database_utils to encapsulate the logic presented
#         within the upload_to_database and load_dimension_table methods 
# Reason? Slowly Changing Dimension Logic can be done within a seperate script, which can be imported into main.py

# Final Step? Once all methods are edited accordingly, create a loop which will upload the tables to the database. 

def upload_tables_and_log(
    connector : DatabaseConnector,
    tables_to_upload : list ,
    target_database_engine : Engine
):
    """
    Helper function to upload multiple tables to the database and log the results.

    Parameters:
    connector (DatabaseConnector): An instance of the DatabaseConnector class.
    tables_to_upload (list): A list of tuples where each tuple contains details for table upload.
        Each tuple should contain (cleaned_table, table_name, action_type, column_datatypes, subset, additional_rows).
    target_database_engine (sqlalchemy.engine.base.Engine): The engine for the target database.

    Returns:
    dict: A dictionary containing the uploaded tables where keys are table names.
    """
    uploaded_tables = {}

    for table_details in tables_to_upload:
        cleaned_table, target_database_engine, table_name, table_condition, mapping, subset, additional_rows, column_datatypes = table_details
        uploaded_table = connector.upload_to_db(
            cleaned_table,
            target_database_engine,
            table_name,
            table_condition,
            mapping=mapping,
            subset=subset,
            additional_rows=additional_rows,
            column_datatypes=column_datatypes
        )

        # Log the result
        log_message = f"{table_condition.capitalize()} {table_name} table uploaded successfully."
        main_logger.info(log_message)
        print(log_message)

        uploaded_tables[table_name] = uploaded_table

    return uploaded_tables

tables_to_upload = []

tables_to_upload.append((
    cleaned_user_data_table,
    target_database_engine,
    'land_user_data',
    'replace',
    None,
    None, 
    None, 
    {
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
))

tables_to_upload.append((
    cleaned_store_data_table,
    target_database_engine,
    'land_store_details',
    'replace',
    None, 
    None, 
    None,
    {
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

))

tables_to_upload.append((
    cleaned_product_table,
    target_database_engine,
    'land_product_details',
    "replace",
    None, 
    None, 
    None,
    {
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
))

tables_to_upload.append((
    cleaned_time_event_table,
    target_database_engine,
    'land_date_times',
    "replace",
    None,
    None,
    None,
    {
        "index": BIGINT,
        "date_key": BIGINT,
        "event_time": TIME,
        "day": VARCHAR(30),
        "month": VARCHAR(30),
        "year": VARCHAR(30),
        "time_period": VARCHAR(40),
        "date_uuid": UUID
    } 
))

tables_to_upload.append((
    cleaned_card_details_table,
    target_database_engine,
    "land_card_details",
    "replace",
    None,
    None,
    None,
    {
        "index": BIGINT,
        "card_key": BIGINT,
        "card_number": VARCHAR(30),
        "expiry_date": VARCHAR(20),
        "card_provider": VARCHAR(255),
        "date_payment_confirmed": DATE
    } 
))

tables_to_upload.append((
        cleaned_currency_table,
        target_database_engine,
        "land_currency",
        "replace",
        None,
        None,
        None,
        {
        "index": BIGINT,
        "currency_key": BIGINT,
        "currency_conversion_key": BIGINT,
        "country_name": VARCHAR(100),
        "currency_code": VARCHAR(20),
        "country_code": VARCHAR(5),
        "currency_symbol": VARCHAR(5)
    } 
))

tables_to_upload.append((
        cleaned_currency_conversion_table,
        target_database_engine,
        "land_currency_conversion",
        "replace",
        None,
        None, 
        None,
        {
        "index": BIGINT,
        "currency_conversion_key": BIGINT,
        "currency_name": VARCHAR(50),
        "currency_code": VARCHAR(5),
        "conversion_rate": NUMERIC(20,6),
        "conversion_rate_percentage": NUMERIC(20,6),
        "last_updated" : TIMESTAMP(timezone=True)

    } 
))

tables_to_upload.append((
        cleaned_orders_table,
        target_database_engine,
        "orders_table",
        "replace",
        None,
        None,
        None,
        {
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
))

## ========== LOADING DIMENSION TABLES INTO LIST 

tables_to_upload.append((
    cleaned_user_data_table, 
        target_database_engine,
        "dim_users",
        "append",
        None,
        None,
        None,
        {
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
))

tables_to_upload.append((
        cleaned_store_data_table,
        target_database_engine,
        "dim_store_details",
        "append",
        None,
        None,
        None,
        {
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
))

tables_to_upload.append((
        cleaned_product_table,
        target_database_engine,
        "dim_product_details",
        "append",
      {"Still_avaliable": True, "Removed": False},
      None,
      None,
       {
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
))

tables_to_upload.append((
        cleaned_card_details_table, 
        target_database_engine,
        "dim_card_details",
        "append",
        None,
        None,
        None,
        {
        "index": BIGINT,
        "card_key": BIGINT,
        "card_number": VARCHAR(30),
        "expiry_date": VARCHAR(20),
        "card_provider": VARCHAR(255),
        "date_payment_confirmed": DATE
        }
))

tables_to_upload.append((
        cleaned_time_event_table,
        target_database_engine,
        "dim_date_times",
        "append",
        None,
        None,
        None,
        {
        "index": BIGINT,
        "date_key": BIGINT,
        "event_time": TIME,
        "day": VARCHAR(30),
        "month": VARCHAR(30),
        "year": VARCHAR(30),
        "time_period": VARCHAR(40),
        "date_uuid": UUID
    } 
))

tables_to_upload.append((
    cleaned_currency_table,
    target_database_engine,
    "dim_currency",
    "append",
    None,
    ["US", "GB", "DE"],
    [
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
    {
    "index": BIGINT,
    "currency_key": BIGINT,
    "currency_conversion_key": BIGINT,
    "country_name": VARCHAR(100),
    "currency_code": VARCHAR(20),
    "country_code": VARCHAR(5),
    "currency_symbol": VARCHAR(5)
} 
))

tables_to_upload.append((
        cleaned_currency_conversion_table,
        target_database_engine,
        "dim_currency_conversion",
        "append",
        None,
        ["USD", "GBP", "EUR"],
        [
            {"currency_conversion_key": -1, "currency_name": "Not Applicable"},
            {"currency_conversion_key": 0, "currency_name": "Unknown"}
        ],
        {
        "index": BIGINT,
        "currency_conversion_key": BIGINT,
        "currency_name": VARCHAR(50),
        "currency_code": VARCHAR(5),
        "conversion_rate": NUMERIC(20,6),
        "conversion_rate_percentage": NUMERIC(20,6),
        "last_updated" : TIMESTAMP(timezone=True)
        }
))

# Uploads all tables to the database 
# after adding them to the list and calling the function below
upload_tables_and_log(
    connector=connector, 
    tables_to_upload=tables_to_upload,
    target_database_engine=target_database_engine
                      )

# Connecting to the database 
sql_transformations.connect_to_database(
    file_pathway_to_datastore, "sales_data_poc"
)

# Adding logic to populate the weight class column in the dim_products_table
sql_transformations.alter_and_update(
    get_absolute_file_path("add_weight_class_column_script.sql", r"sales_data\DML")
)

# Adding primary keys to tables 
sql_transformations.alter_and_update(
    get_absolute_file_path("add_primary_keys.sql", r"sales_data\DDL")
    )

# Adding foreign key constraints to tables 
sql_transformations.alter_and_update(
    get_absolute_file_path("foreign_key_constraints.sql", r"sales_data\DDL")
)

# Mapping foreign keys to empty key columns in fact table and dim_currency table
sql_transformations.alter_and_update(
    get_absolute_file_path(
        "update_foreign_keys.sql", r"sales_data\DML"
    )
)

# Creating views based on database post-load
sql_transformations.alter_and_update(
    get_absolute_file_path(
        "create_views.sql", r"sales_data\DDL"
    )
)   

end_time = time.time()

execution_time = end_time - start_time

print(f"Execution time: {execution_time} seconds")
main_logger.info(f"Time elapsed : {execution_time} seconds")





