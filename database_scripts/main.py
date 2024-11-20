# Import the necessary modules for running the full process 
from database_scripts.database_utils import DatabaseConnector
from database_scripts.data_extraction import DataExtractor
from database_scripts.data_cleaning import DataCleaning 
from database_scripts.currency_rate_extraction import CurrencyExtractor
# Built-in python module imports 
import json 
import logging 
import os 
import yaml 
"""
Opening configuration files 

"""

with open('../config/database_schema.yaml') as schema_file:
    db_schema = yaml.safe_load(schema_file)

with open('../config/main_config.json') as config_file:
    main_config = json.load(config_file)

"""
LOG DEFINITION
"""
log_filename = main_config['filepaths']['logs']['main_log_filepath'] #"../logs/main.log"  
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


source_database_creds_file = main_config['filepaths']['databases']['source_database'] # "../credentials/db_creds.yaml"
target_database_creds_file = main_config['filepaths']['databases']['target_database'] # "../credentials/sales_data_creds.yaml"
currency_url = main_config['urls']['currency_exchange_url'] # "https://www.x-rates.com/table/?from=GBP&amount=1"
source_text_file = main_config['filepaths']['source_files']['source_text_file'] # "../source_data_files/currency_code_mapping"
json_source_file = main_config['filepaths']['source_files']['json_source_file'] # "../source_data_files/country_data.json"
exported_csv_file = main_config['filepaths']['source_files']['currency_csv_file'] # "../source_data_files/currency_conversions.csv"
source_database_name = main_config['databases']['source_database'] #'postgres'
target_database_name = main_config['databases']['target_database'] # 'sales_data'




# Instianting Classes 
connector = DatabaseConnector()
extractor = DataExtractor()
cleaner = DataCleaning() 
currency_extractor = CurrencyExtractor(currency_url) 

# Create the target database 
target_database_conn_string = connector.create_connection_string(target_database_creds_file, True, new_db_name='postgres')
connector.create_database(main_config['databases']['target_database'], target_database_conn_string)

source_database_creds = connector.read_database_credentials(source_database_creds_file)

# Connecting to source database 
source_engine = connector.initialise_database_connection(source_database_creds_file, connect_to_database=True, new_db_name=source_database_name)

# Connecting to target database 
target_engine = connector.initialise_database_connection(target_database_creds_file, connect_to_database=True, new_db_name=target_database_name)


def user_data_pipeline():
    # Extract User Data From RDS 
    user_table = extractor.read_rds_table(main_config['databases']['source_table_names'][0], source_engine)

    print(user_table)
    cleaned_user_table = cleaner.clean_user_data(source_engine, user_table, main_config['databases']['source_table_names'][0])
    print(cleaned_user_table)

    new_user_rows = main_config['databases']['additional_rows']['dim_users']

    connector.upload_to_db(
        user_table
        ,target_engine
        ,main_config['databases']['target_table_names'][0] # raw_user_data
        ,main_config['table_operations']['replace']
        
    )
    connector.upload_to_db(
        cleaned_user_table
        , target_engine
        , main_config['databases']['target_table_names'][1] # land_user_data
        , main_config['table_operations']['replace']
        , schema_config=db_schema
        )

    connector.upload_to_db(
        cleaned_user_table
        , target_engine
        , main_config['databases']['target_table_names'][2] # dim_users
        , main_config['table_operations']['append']
        , additional_rows=new_user_rows
        , schema_config=db_schema
    )


def store_data_pipeline():
    # Extract the source data from the RDS 
    raw_store_details_table = extractor.read_rds_table(main_config['databases']['source_table_names'][1], source_engine)
    print(raw_store_details_table)
    # Clean the raw_data 
    cleaned_store_data_table = cleaner.clean_store_data(source_engine, raw_store_details_table, main_config['databases']['source_table_names'][1])
    print(cleaned_store_data_table)

    new_store_rows = main_config['databases']['additional_rows']['dim_store_details']

    # Uploading tables to database 
    connector.upload_to_db(
        raw_store_details_table
        ,target_engine
        ,main_config['databases']['target_table_names'][3] # raw_store_data
        ,main_config['table_operations']['replace']   
    )
    
    connector.upload_to_db(
        cleaned_store_data_table
        , target_engine
        , main_config['databases']['target_table_names'][4] # 'land_store_data'
        , main_config['table_operations']['replace']
        , schema_config=db_schema
    )

    connector.upload_to_db(
        cleaned_store_data_table
        , target_engine
        , main_config['databases']['target_table_names'][5] # "dim_store_details"
        , main_config['table_operations']['append']
        , additional_rows=new_store_rows
        , schema_config=db_schema
    )

def card_details_pipeline():
    raw_card_details_table = extractor.retrieve_pdf_data(main_config['urls']['pdf_file'])
    print(raw_card_details_table)

    cleaned_card_details_table = cleaner.clean_card_details(raw_card_details_table)
    print(cleaned_card_details_table)

    new_card_details_row_additions = main_config['databases']['additional_rows']['dim_card_details']
    
    # Uploading tables to database 
    connector.upload_to_db(
        raw_card_details_table
        ,target_engine
        , main_config['databases']['target_table_names'][6] # 'raw_card_data'
        ,main_config['table_operations']['replace']   
    )
    
    connector.upload_to_db(
        cleaned_card_details_table
        , target_engine
        , main_config['databases']['target_table_names'][7] # 'land_card_data'
        , main_config['table_operations']['replace']
        , schema_config=db_schema
    )

    connector.upload_to_db(
        cleaned_card_details_table
        , target_engine
        , main_config['databases']['target_table_names'][8] # "dim_card_details"
        , main_config['table_operations']['append']
        , additional_rows=new_card_details_row_additions
        , schema_config=db_schema
    )

def product_details_pipeline(): 
    # Extract the raw products table from the S3 bucket 
    raw_product_details_table = extractor.read_s3_bucket_to_dataframe(main_config['urls']['s3_csv_file'])
    print(raw_product_details_table)
    # Apply the cleaning method to the raw products table 
    cleaned_product_details_table = cleaner.clean_product_table(raw_product_details_table)
    print(cleaned_product_details_table)

    new_product_rows = main_config['databases']['additional_rows']['dim_product'] 
    
    # Uploading tables to database 
    connector.upload_to_db(
        raw_product_details_table
        ,target_engine
        , main_config['databases']['target_table_names'][9] # 'raw_product_data'
        ,main_config['table_operations']['replace']   
    )
    
    connector.upload_to_db(
        cleaned_product_details_table
        , target_engine
        , main_config['databases']['target_table_names'][10]
        ,main_config['table_operations']['replace']
        , schema_config=db_schema
    )

    connector.upload_to_db(
        cleaned_product_details_table
        , target_engine
        , main_config['databases']['target_table_names'][11] #"dim_product"
        , main_config['table_operations']['append']
        , mapping=main_config['databases']['boolean_mapping']
        , additional_rows=new_product_rows
        , schema_config=db_schema
    )

def time_events_pipeline():
    # Reading the raw_time_event_table from s3
    raw_time_event_table = extractor.read_json_from_s3(main_config['urls']['s3_json_file'])
    # Applying cleaning method to raw_time_events_table
    print(raw_time_event_table)
    cleaned_time_event_table = cleaner.clean_time_event_table(raw_time_event_table)
    print(cleaned_time_event_table)
    new_time_event_rows = main_config['databases']['additional_rows']['dim_date_times']
    # Uploading tables to database 
    connector.upload_to_db(
        raw_time_event_table
        , target_engine
        , main_config['databases']['target_table_names'][12] # 'raw_time_event_data'
        , main_config['table_operations']['replace']   
    )
    
    connector.upload_to_db(
        cleaned_time_event_table
        , target_engine
        , main_config['databases']['target_table_names'][13] # "land_date_times"
        , main_config['table_operations']['replace']
        , schema_config=db_schema
    )

    connector.upload_to_db(
        cleaned_time_event_table
        , target_engine
        , main_config['databases']['target_table_names'][14] # "dim_date_times"
        , main_config['table_operations']['append']
        , additional_rows=new_time_event_rows
        , schema_config=db_schema
    )

def orders_table_pipeline():
    raw_orders_table = extractor.read_rds_table(main_config['databases']['target_table_names'][-1], source_engine)
    print(raw_orders_table)
    cleaned_orders_table = cleaner.clean_orders_table(source_engine, raw_orders_table, main_config['databases']['target_table_names'][-1])
    print(cleaned_orders_table)

    # Uploading tables to database 
    connector.upload_to_db(
        cleaned_orders_table
        , target_engine
        , main_config['databases']['target_table_names'][-1] # "orders_table"
        , main_config['table_operations']['append']
        , schema_config=db_schema
    )
    pass     

def currency_data_pipeline():
    raw_currency_data = extractor.read_json_local(json_source_file)
    print(raw_currency_data)
    cleaned_currency_table = cleaner.clean_currency_table(raw_currency_data)
    print(cleaned_currency_table)   

    new_currency_rows = main_config['databases']['additional_rows']['dim_currency'] 
    
    connector.upload_to_db(
        cleaned_currency_table
        , target_engine
        , main_config['databases']['target_table_names'][15] # "land_currency_data"
        ,main_config['table_operations']['replace']
        , schema_config=db_schema
    )

    connector.upload_to_db(
        cleaned_currency_table
        , target_engine
        , main_config['databases']['target_table_names'][16] # "dim_currency"
        , main_config['table_operations']['append']
        , additional_rows=new_currency_rows
        , subset=main_config['currency_subset'] # ["US", "GB", "DE"]
        , schema_config=db_schema
    )

def currency_conversion_pipeline():
        html_data = currency_extractor.read_html_data()
        first_table = currency_extractor.html_to_dataframe(html_data, 0)
        second_table = currency_extractor.html_to_dataframe(html_data, 1)

        combined_table = currency_extractor.merge_dataframes("right", first_table, second_table)
        print(combined_table)

        cleaned_currency_conversion_table = cleaner.clean_currency_exchange_rates(
            combined_table, 
            source_text_file
            )
        print(cleaned_currency_conversion_table)

        new_currency_rows = main_config['databases']['additional_rows']['dim_currency_conversion']
        
        connector.upload_to_db(
        cleaned_currency_conversion_table
        , target_engine
        , main_config['databases']['target_table_names'][17] # "land_currency_conversion_data"
        , main_config['table_operations']['replace']
        , schema_config=db_schema
    )
        
        connector.upload_to_db(
            cleaned_currency_conversion_table
            , target_engine
            , main_config['databases']['target_table_names'][18] # "dim_currency_conversion"
            , main_config['table_operations']['append']
            , additional_rows=new_currency_rows
            , subset=main_config['currency_conversion_subset']
            , schema_config=db_schema
    )

def alter_and_update_database():

    # # Adding logic to populate the weight class column in the dim_products_table
    connector.alter_and_update(
        main_config['filepaths']['sql_scripts']['add_weight_class_column'] #'../sales_data/DML/add_weight_class_column_script.sql'
        ,target_engine
    )

    # Adding primary keys to tables 
    connector.alter_and_update(
        main_config['filepaths']['sql_scripts']['add_primary_keys'] # '../sales_data/DDL/add_primary_keys.sql'
        ,target_engine
        )

    # Adding foreign key constraints to tables 
    connector.alter_and_update(
        # '../sales_data/DML/foreign_key_constraints.sql'
        main_config['filepaths']['sql_scripts']['foreign_key_constraints'] # '../sales_data/DDL/foreign_key_constraints.sql'
        ,target_engine
    )

    # Mapping foreign keys to empty key columns in fact table and dim_currency table
    connector.alter_and_update(
        main_config['filepaths']['sql_scripts']['update_foreign_keys'] # '../sales_data/DML/update_foreign_keys.sql'
        ,target_engine
    )

    # Creating views based on database post-load
    connector.alter_and_update(
        main_config['filepaths']['sql_scripts']['create_views'] # '../sales_data/DDL/create_views.sql'
        ,target_engine
    )        

if __name__ == "__main__":
    user_data_pipeline()
    store_data_pipeline()
    product_details_pipeline()
    card_details_pipeline() 
    time_events_pipeline() 
    orders_table_pipeline()
    currency_data_pipeline() 
    currency_conversion_pipeline() 
    alter_and_update_database()
