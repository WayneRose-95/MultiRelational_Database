# Import the necessary modules for running the full process 
from database_scripts.database_utils import DatabaseConnector
from database_scripts.data_extraction import DataExtractor
from database_scripts.data_cleaning import DataCleaning 
from database_scripts.currency_rate_extraction import CurrencyExtractor
from database_scripts.file_handler import get_absolute_file_path
from sqlalchemy import Column, VARCHAR, DATE, FLOAT, SMALLINT, BOOLEAN, TIME, NUMERIC, TIMESTAMP, INTEGER
from sqlalchemy.dialects.postgresql import BIGINT, UUID
from sqlalchemy.engine import Engine
# Built-in python module imports 
import logging 
import os 
import time 
import yaml 

"""
LOG DEFINITION
"""
log_filename = "../logs/main_reworked.log"   # "logs/main_reworked.log"
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


source_database_creds_file = "../credentials/db_creds.yaml"
target_database_creds_file = "../credentials/sales_data_creds.yaml"
currency_url = "https://www.x-rates.com/table/?from=GBP&amount=1"
source_text_file = "../source_data_files/currency_code_mapping.txt"
json_source_file = "../source_data_files/country_data.json"
exported_csv_file = "../source_data_files/currency_conversions.csv"
source_database_name = 'postgres'
target_database_name = 'sales_data_concept'

with open('../credentials/database_schema.yaml') as schema_file:
    db_schema = yaml.safe_load(schema_file)

# Instianting Classes 
connector = DatabaseConnector()
extractor = DataExtractor()
cleaner = DataCleaning() 
currency_extractor = CurrencyExtractor(currency_url) 

# Create the target database 
target_database_conn_string = connector.create_connection_string(target_database_creds_file, True, new_db_name='postgres')
connector.create_database('sales_data_concept', target_database_conn_string)

source_database_creds = connector.read_database_credentials(source_database_creds_file)

# Connecting to source database 
source_engine = connector.initialise_database_connection(source_database_creds_file, connect_to_database=True, new_db_name=source_database_name)

# Connecting to target database 
target_engine = connector.initialise_database_connection(target_database_creds_file, connect_to_database=True, new_db_name=target_database_name)


def user_data_pipeline():
    # Extract User Data From RDS 
    user_table = extractor.read_rds_table('legacy_users', source_engine)

    print(user_table)
    cleaned_user_table = cleaner.clean_user_data(source_engine, user_table, 'legacy_users')
    print(cleaned_user_table)

    new_user_rows = [
        {
            "user_key": -1,
            "first_name": "Not Applicable",
            "last_name": "Not Applicable",
        },
        {
            "user_key": 0, 
            "first_name": "Unknown", 
            "last_name": "Unknown"
        }
    ]

    connector.upload_to_db(
        user_table
        ,target_engine
        ,'raw_user_data'
        ,'replace'
        

    )
    connector.upload_to_db(
        cleaned_user_table
        , target_engine
        , 'land_user_data'
        ,"replace"
        , schema_config=db_schema
        )

    connector.upload_to_db(
        cleaned_user_table
        , target_engine
        , "dim_users"
        , "append"
        , additional_rows=new_user_rows
        , schema_config=db_schema
    )


def store_data_pipeline():
    # Extract the source data from the RDS 
    raw_store_details_table = extractor.read_rds_table('legacy_store_details', source_engine)
    print(raw_store_details_table)
    # Clean the raw_data 
    cleaned_store_data_table = cleaner.clean_store_data(source_engine, raw_store_details_table, 'legacy_store_details')
    print(cleaned_store_data_table)

    new_store_rows = [
        {"store_key": -1, "store_address": "Not Applicable"},
        {"store_key": 0, "store_address": "Unknown"},
    ]

    # Uploading tables to database 
    connector.upload_to_db(
        raw_store_details_table
        ,target_engine
        ,'raw_store_data'
        ,'replace'   
    )
    
    connector.upload_to_db(
        cleaned_store_data_table
        , target_engine
        , 'land_store_data'
        ,"replace"
        , schema_config=db_schema
    )

    connector.upload_to_db(
        cleaned_store_data_table
        , target_engine
        , "dim_store_details"
        , "append"
        , additional_rows=new_store_rows
        , schema_config=db_schema
    )

def card_details_pipeline():
    raw_card_details_table = extractor.retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")
    print(raw_card_details_table)

    cleaned_card_details_table = cleaner.clean_card_details(raw_card_details_table)
    print(cleaned_card_details_table)

    new_card_details_row_additions = [
            {"card_key": -1, "card_number": "Not Applicable"},
            {"card_key": 0, "card_number": "Unknown"},
        ]
    
    # Uploading tables to database 
    connector.upload_to_db(
        raw_card_details_table
        ,target_engine
        ,'raw_card_data'
        ,'replace'   
    )
    
    connector.upload_to_db(
        cleaned_card_details_table
        , target_engine
        , 'land_card_data'
        ,"replace"
        , schema_config=db_schema
    )

    connector.upload_to_db(
        cleaned_card_details_table
        , target_engine
        , "dim_card_details"
        , "append"
        , additional_rows=new_card_details_row_additions
        , schema_config=db_schema
    )

def product_details_pipeline(): 
    # Extract the raw products table from the S3 bucket 
    raw_product_details_table = extractor.read_s3_bucket_to_dataframe("s3://data-handling-public/products.csv")
    print(raw_product_details_table)
    # Apply the cleaning method to the raw products table 
    cleaned_product_details_table = cleaner.clean_product_table(raw_product_details_table)
    print(cleaned_product_details_table)

    new_product_rows = [
                {"product_key": -1, "ean": "Not Applicable"},
                {"product_key": 0, "ean": "Unknown"},
            ]
    
    # Uploading tables to database 
    connector.upload_to_db(
        raw_product_details_table
        ,target_engine
        ,'raw_product_data'
        ,'replace'   
    )
    
    connector.upload_to_db(
        cleaned_product_details_table
        , target_engine
        , 'land_product_data'
        ,"replace"
        , schema_config=db_schema
    )

    connector.upload_to_db(
        cleaned_product_details_table
        , target_engine
        , "dim_product"
        , "append"
        , mapping={"Still_avaliable": True, "Removed": False}
        , additional_rows=new_product_rows
        , schema_config=db_schema
    )

def time_events_pipeline():
    # Reading the raw_time_event_table from s3
    raw_time_event_table = extractor.read_json_from_s3("https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json")
    # Applying cleaning method to raw_time_events_table
    print(raw_time_event_table)
    cleaned_time_event_table = cleaner.clean_time_event_table(raw_time_event_table)
    print(cleaned_time_event_table)
    new_time_event_rows = [
                {"date_key": -1, "timestamp": "00:00:00"},
                {"date_key": 0, "timestamp": "00:00:00"},
         ]
    # Uploading tables to database 
    connector.upload_to_db(
        raw_time_event_table
        ,target_engine
        ,'raw_time_event_data'
        ,'replace'   
    )
    
    connector.upload_to_db(
        cleaned_time_event_table
        , target_engine
        , "land_date_times"
        ,"replace"
        , schema_config=db_schema
    )

    connector.upload_to_db(
        cleaned_time_event_table
        , target_engine
        , "dim_date_times"
        , "append"
        , additional_rows=new_time_event_rows
        , schema_config=db_schema
    )

def orders_table_pipeline():
    raw_orders_table = extractor.read_rds_table('orders_table', source_engine)
    print(raw_orders_table)
    cleaned_orders_table = cleaner.clean_orders_table(source_engine, raw_orders_table, 'orders_table')
    print(cleaned_orders_table)

    # Uploading tables to database 
    connector.upload_to_db(
        cleaned_orders_table
        , target_engine
        , "orders_table"
        , "append"
        , schema_config=db_schema
    )
    pass     

def currency_data_pipeline():
    raw_currency_data = extractor.read_json_local(json_source_file)
    print(raw_currency_data)
    cleaned_currency_table = cleaner.clean_currency_table(raw_currency_data)
    print(cleaned_currency_table)   

    new_currency_rows = [
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
        ]
    
    connector.upload_to_db(
        cleaned_currency_table
        , target_engine
        , "land_currency_data"
        ,"replace"
        , schema_config=db_schema
    )

    connector.upload_to_db(
        cleaned_currency_table
        , target_engine
        , "dim_currency"
        , "append"
        , additional_rows=new_currency_rows
        , subset=["US", "GB", "DE"]
        , schema_config=db_schema
    )
    

    

# class Configuration:
#     '''
#     A class which is responsible for setting the configuration of the databases and file locations used 
#     '''
#     def __init__(
#             self,
#             credentials_directory_name,
#             source_data_directory_name,
#             source_database_creds_file,
#             target_database_creds_file,
#             currency_url, 
#             source_text_file,
#             json_source_file,
#             exported_csv_file,

#             source_database_name='postgres',
#             # By default, the target_database_name created will be called sales_data
#             target_database_name='sales_data'
#     ):
#         self.connector = DatabaseConnector()
#         self.file_pathway_to_source_database = get_absolute_file_path(source_database_creds_file, credentials_directory_name) # "credentials"
#         self.file_pathway_to_datastore = get_absolute_file_path(target_database_creds_file, credentials_directory_name) # "credentials"
#         self.file_pathway_to_source_text_file = get_absolute_file_path(source_text_file, source_data_directory_name) # "source_data_files"
#         self.file_pathway_to_json_source_file = get_absolute_file_path(json_source_file, source_data_directory_name) # "source_data_files"
#         self.file_pathway_to_exported_csv_file = get_absolute_file_path(exported_csv_file, source_data_directory_name) # "source_data_files"
#         self.currency_url = currency_url
#         self.source_database_name = source_database_name
#         self.target_database_name = target_database_name
#         # Create the database first then connect to it. 
#         self.create_database_connection_string = self.connector.create_connection_string(self.file_pathway_to_datastore, True, new_db_name='postgres')
#         self.sql_transformations = self.connector.create_database(self.target_database_name, self.create_database_connection_string)
#         # self.sql_transformations = SQLAlterations(self.file_pathway_to_datastore)
#         # self.database_creation = self.sql_transformations.create_database(self.target_database_name)

#         self.source_database_engine = self.connector.initialise_database_connection(
#             self.file_pathway_to_source_database, True, self.source_database_name
#         )
#         self.target_database_engine = self.connector.initialise_database_connection(
#             self.file_pathway_to_datastore, True, self.target_database_name
#         )

# class ETLProcess:

#     def __init__(self, configuration : Configuration):
#         self.configuration = configuration
#         self.extractor = DataExtractor()
#         self.cleaner = DataCleaning()
#         self.currency_extractor = CurrencyExtractor(configuration.currency_url)
        
#         self.tables_to_upload_list = []

#     def user_data_pipeline(self):
#         # Extract the raw_data from the source 
#         raw_user_data_table = self.extractor.read_rds_table('legacy_users', self.configuration.source_database_engine)
#         # Run the method to clean the raw data using the clean_user_data method 
#         cleaned_user_data_table = self.cleaner.clean_user_data(self.configuration.source_database_engine, raw_user_data_table, 'legacy_users')

#         # Add the land_user_data table to the list of tables to upload 
#         self.tables_to_upload_list.append((
#             cleaned_user_data_table,
#             self.configuration.target_database_engine,
#             'land_user_data',
#             'replace',
#             None,
#             None, 
#             None, 
#             {
#                     "user_key": BIGINT,
#                     "first_name": VARCHAR(255),
#                     "last_name": VARCHAR(255),
#                     "date_of_birth": DATE,
#                     "company": VARCHAR(255),
#                     "email_address": VARCHAR(255),
#                     "address": VARCHAR(600),
#                     "country": VARCHAR(100),
#                     "country_code": VARCHAR(20),
#                     "phone_number": VARCHAR(50),
#                     "join_date": DATE,
#                     "user_uuid": UUID  
#             }
#         ))
#         # Add the dimension table dim_users to the list of tables to upload 
#         self.tables_to_upload_list.append((
#             cleaned_user_data_table, 
#             self.configuration.target_database_engine,
#             "dim_users",
#             "append",
#             None,
#             None,
#             None,
#             {
#                 "index": BIGINT,
#                 "user_key": BIGINT,
#                 "first_name": VARCHAR(255),
#                 "last_name": VARCHAR(255),
#                 "date_of_birth": DATE,
#                 "company": VARCHAR(255),
#                 "email_address": VARCHAR(255),
#                 "address": VARCHAR(600),
#                 "country": VARCHAR(100),
#                 "country_code": VARCHAR(20),
#                 "phone_number": VARCHAR(50),
#                 "join_date": DATE,
#                 "user_uuid": UUID,
#             }
#     ))

#         pass

#     def store_data_pipeline(self):
#         # Extract the source data from the RDS 
#         raw_store_details_table = self.extractor.read_rds_table('legacy_store_details', self.configuration.source_database_engine)

#         # Clean the raw_data 
#         cleaned_store_data_table = self.cleaner.clean_store_data(self.configuration.source_database_engine, raw_store_details_table, 'legacy_store_details')

#         # Add the land_store_details table to the list of tables to upload 
#         self.tables_to_upload_list.append((
#                 cleaned_store_data_table,
#                 self.configuration.target_database_engine,
#                 'land_store_details',
#                 'replace',
#                 None, 
#                 None, 
#                 None,
#                 {
#                     "index": BIGINT,
#                     "store_key": BIGINT,
#                     "store_address": VARCHAR(1000),
#                     "longitude": FLOAT,
#                     "latitude": FLOAT,
#                     "city": VARCHAR(255),
#                     "store_code": VARCHAR(20),
#                     "number_of_staff": SMALLINT,
#                     "opening_date": DATE,
#                     "store_type": VARCHAR(255),
#                     "country_code": VARCHAR(20),
#                     "region": VARCHAR(255)
#                 }  
#         ))

#         # Add the dim_store_details table to the list of tables to upload 
#         self.tables_to_upload_list.append((
#             cleaned_store_data_table,
#             self.configuration.target_database_engine,
#             "dim_store_details",
#             "append",
#             None,
#             None,
#             None,
#             {
#             "index": BIGINT,
#             "store_key": BIGINT,
#             "store_address": VARCHAR(1000),
#             "longitude": FLOAT,
#             "latitude": FLOAT,
#             "city": VARCHAR(255),
#             "store_code": VARCHAR(20),
#             "number_of_staff": SMALLINT,
#             "opening_date": DATE,
#             "store_type": VARCHAR(255),
#             "country_code": VARCHAR(20),
#             "region": VARCHAR(255)
#         } 

#         ))

#         # raw_user_data_table = extractor.read_rds_table('legacy_users', source_database_engine)
#         # raw_orders_table = extractor.read_rds_table('orders_table', source_database_engine)
#         # raw_store_details_table = extractor.read_rds_table('legacy_store_details', source_database_engine)


#     def card_details_pipeline(self):
#         raw_card_details_table = self.extractor.retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")

#         cleaned_card_details_table = self.cleaner.clean_card_details(raw_card_details_table)

#         self.tables_to_upload_list.append((
#             cleaned_card_details_table,
#             self.configuration.target_database_engine,
#             "land_card_details",
#             "replace",
#             None,
#             None,
#             None,
#             {
#                 "index": BIGINT,
#                 "card_key": BIGINT,
#                 "card_number": VARCHAR(30),
#                 "expiry_date": VARCHAR(20),
#                 "card_provider": VARCHAR(255),
#                 "date_payment_confirmed": DATE
#             } 
#         ))

#         self.tables_to_upload_list.append((
#             cleaned_card_details_table, 
#             self.configuration.target_database_engine,
#             "dim_card_details",
#             "append",
#             None,
#             None,
#             None,
#             {
#             "index": BIGINT,
#             "card_key": BIGINT,
#             "card_number": VARCHAR(30),
#             "expiry_date": VARCHAR(20),
#             "card_provider": VARCHAR(255),
#             "date_payment_confirmed": DATE
#             }
#         ))
#         pass
    
#     def product_table_pipeline(self):
#         raw_product_details_table = self.extractor.read_s3_bucket_to_dataframe("s3://data-handling-public/products.csv")

#         cleaned_product_details_table = self.cleaner.clean_product_table(raw_product_details_table)

#         self.tables_to_upload_list.append((
#         cleaned_product_details_table,
#         self.configuration.target_database_engine,
#         'land_product_details',
#         "replace",
#         None, 
#         None, 
#         None,
#         {
#             "index": BIGINT,
#             "product_key": BIGINT,
#             "ean": VARCHAR(50),
#             "product_name": VARCHAR(500),
#             "product_price": FLOAT,
#             "weight": FLOAT,
#             "weight_class": VARCHAR(50),
#             "category": VARCHAR(50),
#             "date_added": DATE,
#             "uuid": UUID,
#             "availability": VARCHAR(30),
#             "product_code": VARCHAR(50)
#         } 
#     ))
        
#         self.tables_to_upload_list.append((
#         cleaned_product_details_table,
#         self.configuration.target_database_engine,
#         "dim_product_details",
#         "append",
#       {"Still_avaliable": True, "Removed": False},
#       None,
#       None,
#        {
#            "index": BIGINT,
#             "product_key": BIGINT,
#             "ean": VARCHAR(50),
#             "product_name": VARCHAR(500),
#             "product_price": FLOAT,
#             "weight": FLOAT,
#             "weight_class": VARCHAR(50),
#             "category": VARCHAR(50),
#             "date_added": DATE,
#             "uuid": UUID,
#             "availability": VARCHAR(30),
#             "product_code": VARCHAR(50)
#        } 
# ))

#     def orders_table_pipeline(self):
#         raw_orders_table = self.extractor.read_rds_table('orders_table', self.configuration.source_database_engine)

#         cleaned_orders_table = self.cleaner.clean_orders_table(self.configuration.source_database_engine, raw_orders_table, 'orders_table')

#         self.tables_to_upload_list.append((
#             cleaned_orders_table,
#             self.configuration.target_database_engine,
#             "orders_table",
#             "replace",
#             None,
#             None,
#             None,
#             {
#             "index": BIGINT,
#             "order_key": BIGINT,
#             "date_uuid": UUID,
#             "user_uuid": UUID,
#             "card_key": BIGINT,
#             "date_key": BIGINT,
#             "product_key": BIGINT,
#             "store_key": BIGINT,
#             "user_key": BIGINT,
#             "currency_key": BIGINT,
#             "card_number": VARCHAR(30),
#             "store_code": VARCHAR(30),
#             "product_code": VARCHAR(30),
#             "product_quantity": SMALLINT,
#             "country_code": VARCHAR(20)
#         }
#         ))
#         pass 

#     def time_event_pipeline(self):
#         raw_time_event_table = self.extractor.read_json_from_s3("https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json")

#         cleaned_time_event_table = self.cleaner.clean_time_event_table(raw_time_event_table)

#         # Add the land_date_times table to the list of tables to upload 
#         self.tables_to_upload_list.append((
#             cleaned_time_event_table,
#             self.configuration.target_database_engine,
#             'land_date_times',
#             "replace",
#             None,
#             None,
#             None,
#             {
#                 "index": BIGINT,
#                 "date_key": BIGINT,
#                 "date_uuid": UUID,
#                 "day": INTEGER,
#                 "month": INTEGER,
#                 "year": INTEGER,
#                 "time_period": VARCHAR(40),
#                 "event_time": TIME,
#                 "full_date": DATE 
                
#             } 
#         ))

#         # Add the dim_date_times tablet o the list of tables to upload
#         self.tables_to_upload_list.append((
#             cleaned_time_event_table,
#             self.configuration.target_database_engine,
#             "dim_date_times",
#             "append",
#             None,
#             None,
#             None,
#             {
#                 "index": BIGINT,
#                 "date_key": BIGINT,
#                 "date_uuid": UUID,
#                 "day": INTEGER,
#                 "month": INTEGER,
#                 "year": INTEGER,
#                 "event_time": TIME,
#                 "full_date": DATE, 
#                 "time_period": VARCHAR(40),
#                 
#                 
#             } 
#         ))
#         pass

#     def currency_data_pipeline(self):
#         raw_currency_data = self.extractor.read_json_local(self.configuration.file_pathway_to_json_source_file)

#         cleaned_currency_table = self.cleaner.clean_currency_table(raw_currency_data)

#         # Add the table to the list of tables to upload 
#         self.tables_to_upload_list.append((
#         cleaned_currency_table,
#         self.configuration.target_database_engine,
#         "land_currency",
#         "replace",
#         None,
#         None,
#         None,
#         {
#         "index": BIGINT,
#         "currency_key": BIGINT,
#         "currency_conversion_key": BIGINT,
#         "country_name": VARCHAR(100),
#         "currency_code": VARCHAR(20),
#         "country_code": VARCHAR(5),
#         "currency_symbol": VARCHAR(5)
#     } 
# ))
#         # Add the dim_currency table to the list of tables to upload. 
#         self.tables_to_upload_list.append((
#         cleaned_currency_table,
#         self.configuration.target_database_engine,
#         "dim_currency",
#         "append",
#         None,
#         ["US", "GB", "DE"],
#         [
#             {
#                 "currency_key": -1,
#                 "currency_conversion_key": -1,
#                 "currency_code": "Not Applicable",
#             },
#             {
#                 "currency_key": 0,
#                 "currency_conversion_key": 0,
#                 "currency_code": "Unknown",
#             },
#         ],
#         {
#         "index": BIGINT,
#         "currency_key": BIGINT,
#         "currency_conversion_key": BIGINT,
#         "country_name": VARCHAR(100),
#         "currency_code": VARCHAR(20),
#         "country_code": VARCHAR(5),
#         "currency_symbol": VARCHAR(5)
#     } 
#     ))

#         pass
    
#     def currency_conversion_data_pipeline(self):

#         html_data = self.currency_extractor.read_html_data()
#         first_table = self.currency_extractor.html_to_dataframe(html_data, 0)
#         second_table = self.currency_extractor.html_to_dataframe(html_data, 1)

#         combined_table = self.currency_extractor.merge_dataframes("right", first_table, second_table)

#         cleaned_currency_conversion_table = self.cleaner.clean_currency_exchange_rates(
#             combined_table, 
#             self.configuration.file_pathway_to_source_text_file
#             )

#         self.tables_to_upload_list.append((
#         cleaned_currency_conversion_table,
#         self.configuration.target_database_engine,
#         "land_currency_conversion",
#         "replace",
#         None,
#         None, 
#         None,
#         {
#         "index": BIGINT,
#         "currency_conversion_key": BIGINT,
#         "currency_name": VARCHAR(50),
#         "currency_code": VARCHAR(5),
#         "conversion_rate": NUMERIC(20,6),
#         "conversion_rate_percentage": NUMERIC(20,6),
#         "last_updated" : TIMESTAMP(timezone=True)

#     } 
# ))

#         self.tables_to_upload_list.append((
#         cleaned_currency_conversion_table,
#         self.configuration.target_database_engine,
#         "dim_currency_conversion",
#         "append",
#         None,
#         ["USD", "GBP", "EUR"],
#         [
#             {"currency_conversion_key": -1, "currency_name": "Not Applicable"},
#             {"currency_conversion_key": 0, "currency_name": "Unknown"}
#         ],
#         {
#         "index": BIGINT,
#         "currency_conversion_key": BIGINT,
#         "currency_name": VARCHAR(50),
#         "currency_code": VARCHAR(5),
#         "conversion_rate": NUMERIC(20,6),
#         "conversion_rate_percentage": NUMERIC(20,6),
#         "last_updated" : TIMESTAMP(timezone=True)
#         }
# ))
#         pass    
    

#     def upload_tables_and_log(
#     self,
#     connector : DatabaseConnector,
#     target_database_engine : Engine
#     ):
#         """
#         Helper function to upload multiple tables to the database and log the results.

#         Parameters:
#         connector (DatabaseConnector): An instance of the DatabaseConnector class.
#         tables_to_upload (list): A list of tuples where each tuple contains details for table upload.
#             Each tuple should contain (cleaned_table, table_name, action_type, column_datatypes, subset, additional_rows).
#         target_database_engine (sqlalchemy.engine.base.Engine): The engine for the target database.

#         Returns:
#         dict: A dictionary containing the uploaded tables where keys are table names.
#         """
#         uploaded_tables = {}

#         for table_details in self.tables_to_upload_list:
#             cleaned_table, target_database_engine, table_name, table_condition, mapping, subset, additional_rows, column_datatypes = table_details
#             uploaded_table = connector.upload_to_db(
#                 cleaned_table,
#                 target_database_engine,
#                 table_name,
#                 table_condition,
#                 mapping=mapping,
#                 subset=subset,
#                 additional_rows=additional_rows,
#                 column_datatypes=column_datatypes
#             )

#             # Log the result
#             log_message = f"{table_condition.capitalize()} {table_name} table uploaded successfully."
#             main_logger.info(log_message)
#             print(log_message)

#         uploaded_tables[table_name] = uploaded_table

#         return uploaded_tables
    

#     def alter_and_update_database(self):

#         # # Adding logic to populate the weight class column in the dim_products_table
#         self.configuration.connector.alter_and_update(
#             get_absolute_file_path("add_weight_class_column_script.sql", r"sales_data\DML"),
#             self.configuration.target_database_engine
#         )

#         # Adding primary keys to tables 
#         self.configuration.connector.alter_and_update(
#             get_absolute_file_path("add_primary_keys.sql", r"sales_data\DDL"),
#             self.configuration.target_database_engine
#             )

#         # Adding foreign key constraints to tables 
#         self.configuration.connector.alter_and_update(
#             get_absolute_file_path("foreign_key_constraints.sql", r"sales_data\DDL"),
#             self.configuration.target_database_engine
#         )

#         # Mapping foreign keys to empty key columns in fact table and dim_currency table
#         self.configuration.connector.alter_and_update(
#             get_absolute_file_path(
#                 "update_foreign_keys.sql", r"sales_data\DML"
#             ),
#             self.configuration.target_database_engine
#         )

#         # Creating views based on database post-load
#         self.configuration.connector.alter_and_update(
#             get_absolute_file_path(
#                 "create_views.sql", r"sales_data\DDL"
#             ),
#             self.configuration.target_database_engine
#         )


if __name__ == "__main__":
    # user_data_pipeline()
    # store_data_pipeline()
    # product_details_pipeline()
    # card_details_pipeline() 
    # time_events_pipeline() 
    # orders_table_pipeline()
    currency_data_pipeline() 

#     etl_configuration = Configuration(
#         credentials_directory_name='credentials',
#         source_data_directory_name='source_data_files',
#         source_database_creds_file="db_creds.yaml",
#         target_database_creds_file="sales_data_creds.yaml",
#         currency_url="https://www.x-rates.com/table/?from=GBP&amount=1",
#         source_text_file="currency_code_mapping",
#         json_source_file="country_data.json",
#         exported_csv_file="currency_conversions.csv",
#         source_database_name='postgres',
#         target_database_name='sales_data'

#     )

#     etl_process = ETLProcess(configuration=etl_configuration)

#     etl_process.user_data_pipeline()
#     etl_process.store_data_pipeline()
#     etl_process.product_table_pipeline()
#     etl_process.card_details_pipeline()
#     etl_process.orders_table_pipeline()
#     etl_process.time_event_pipeline()
#     etl_process.currency_data_pipeline()
#     etl_process.currency_conversion_data_pipeline()

#     list_of_uploaded_tables  = etl_process.upload_tables_and_log(etl_configuration.connector, etl_configuration.target_database_engine)

#     etl_process.alter_and_update_database()