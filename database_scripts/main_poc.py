# Import the necessary modules for running the full process 
from database_scripts.database_utils import DatabaseConnector
from database_scripts.data_extraction import DatabaseExtractor
from database_scripts.data_cleaning import DataCleaning 
from database_scripts.sql_transformations import SQLAlterations
from database_scripts.currency_rate_extraction import CurrencyRateExtractor
from database_scripts.file_handler import get_absolute_file_path
# Built-in python module imports 
import logging 
import os 

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
    "sales_data_creds.yaml", "credentials"
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


# Step 2: Instantiate instances of the classes 

connector = DatabaseConnector() 
extractor = DatabaseExtractor() 
# cleaner = DataCleaning()
# sql_transformations = SQLAlterations(file_pathway_to_datastore)
currency_extractor = CurrencyRateExtractor(undetected_chrome=True)

# Step 3: Initialise the source and target engine objects from database_utils.py 

source_database_engine = connector.initialise_database_connection(file_pathway_to_source_database, True, 'postgres')
target_database_engine = connector.initialise_database_connection(file_pathway_to_datastore, True, 'sales_data_dev')

# Step 4 : List the database tables using the source_database_engine object 

# Reliant on the successful connection to the source and target database engine in order to work successfully. 
# As shown in step 3
source_database_table_names = connector.list_db_tables(source_database_engine)
target_database_table_names = connector.list_db_tables(target_database_engine)


# Step 5: Read in an RDS Table from the source database 

raw_user_data_table = extractor.read_rds_table('legacy_users', source_database_engine)
raw_orders_table = extractor.read_rds_table('orders_table', source_database_engine)
raw_store_details_table = extractor.read_rds_table('legacy_store_details', source_database_engine)

# Step 6: Retrieve details from other sources such as: 

# 6a. The pdf file which has the card details 
raw_card_details_table = extractor.retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")

# 6b. The s3 bucket which contains the .csv file on products 

raw_product_details_table = extractor.read_s3_bucket_to_dataframe("s3://data-handling-public/products.csv")

# 6c. The time even table which is hosted inside the s3 bucket 
raw_time_event_table = extractor.read_json_from_s3("https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json")

# 6d. The currency tables

# 6d (i) The currency table which contains currencies and their associated symbols 
raw_currency_data = extractor.read_json_local(file_pathway_to_json_source_file)

# 6d (ii) The currency conversion table which contains currencies and their exchange rates relative to GBP

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


# Step 7: Address the Cleaning Methods (BIG TASK! Will need to plan this out)

# == LAND TABLES == 

# Step 7a. Adjust the clean_user_table method to provide the cleaned land_user_data table 

# Step 7b. Adjust the clean_store_data method to produce the land_store_data table 

# Step 7c. Adjust the clean_card_details method to produce the land_card_details table 

# Step 7d. Adjust the clean_orders_table method to produce the orders_table 

# Step 7e. Adjust the clean_time_event_table to produce the land_date_times table 

# Step 7f. Adjust the clean_product_table to producce the land_product_details table 

# Step 7g. Adjust the clean_currency_table to produce the land_currency table 

# Step 7h. Adjust the clean_currency_exchange_rates method to produce the land_currency_exchange rates table
# NOTE: For step 7h. You must fix the issues with extracting the currency_rates first before doing this 

# == FOR LOADING DIMENSION TABLES == 

# Step 7i Adjust the upload_to_db method in database_utils to encapsulate the logic presented
#         within the upload_to_database and load_dimension_table methods 
# Reason? Slowly Changing Dimension Logic can be done within a seperate script, which can be imported into main.py

# Final Step? Once all methods are edited accordingly, create a loop which will upload the tables to the database. 




