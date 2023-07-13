from data_cleaning import data_cleaning_logger as data_cleaning_logger
from data_extraction import data_extraction_logger as data_extraction_logger
from database_utils import database_utils_logger as database_utils_logger
from sql_transformations import sql_transformations_logger as sql_transformations_logger
from file_handler import get_absolute_file_path
from data_cleaning import DataCleaning
from sql_transformations import SQLAlterations
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

sql_transformations_file_path = get_absolute_file_path(
    "sales_data_creds_test.yaml", "credentials"
)

start_time = time.time()

main_logger.info("Process started")

# Create instance of DataCleaning and SQLAlterations classes
cleaner = DataCleaning(file_pathway_to_datastore)
sql = SQLAlterations(get_absolute_file_path("sales_data_creds.yaml", "credentials"))

# Create the database. If it already exists
sql.create_database("sales_data")  # 'Sales_Data_Test', "Sales_Data_Admin"

# Main ETL Process to Extract, Transform and Load Data into Postgres
land_user_table = cleaner.clean_user_data(
    "legacy_users", file_pathway_to_source_database, "postgres", "land_user_data"
)

dim_users_table = cleaner.load_dimension_table(land_user_table, "dim_users")

land_store_table = cleaner.clean_store_data(
    "legacy_store_details",
    file_pathway_to_source_database,
    "postgres",
    "land_store_details",
)

dim_store_table = cleaner.load_dimension_table(land_store_table, "dim_store_details")

land_product_table = cleaner.clean_product_table(
    "s3://data-handling-public/products.csv", "land_product_details"
)

dim_product_details_table = cleaner.load_dimension_table(
    land_product_table,
    "dim_product_details",
    mapping={"Still_avaliable": True, "Removed": False},
)

land_time_events_table = cleaner.clean_time_event_table(
    "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json",
    "land_date_times",
)

dim_date_times_table = cleaner.load_dimension_table(
    land_time_events_table, "dim_date_times"
)

land_card_details_table = cleaner.clean_card_details(
    "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf",
    "land_card_details",
)

dim_card_details_table = cleaner.load_dimension_table(
    land_card_details_table, "dim_card_details"
)

land_currency_table = cleaner.clean_currency_table(
    file_pathway_to_json_source_file, "land_currency"
)  # ["US", "GB", "DE"]


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
        {"currency_key": 0, "currency_conversion_key": 0, "currency_code": "Unknown"},
    ],
)

land_currency_conversions_table = cleaner.clean_currency_exchange_rates(
    "https://www.x-rates.com/table/?from=GBP&amount=1",
    '//table[@class="tablesorter ratesTable"]/tbody',
    '//*[@id="content"]/div[1]/div/div[1]/div[1]/span[2]',
    ["currency_name", "conversion_rate", "percentage_change"],
    file_pathway_to_exported_csv_file,
    file_pathway_to_source_text_file,
    "land_currency_conversion",
)
dim_currency_conversion_table = cleaner.load_dimension_table(
    land_currency_conversions_table,
    "dim_currency_conversion",
    subset=["USD", "GBP", "EUR"],
    additional_rows=[
        {"currency_conversion_key": -1, "currency_name": "Not Applicable"},
        {"currency_conversion_key": 0, "currency_name": "Unknown"},
    ],
)
cleaner.clean_orders_table(
    "orders_table", file_pathway_to_source_database, "postgres", "orders_table"
)


# Altering the schema and forming the STAR Schema model using the uploaded database
sql.connect_to_database(
    get_absolute_file_path("sales_data_creds.yaml", "credentials"), "sales_data"
)
sql.alter_and_update(
    get_absolute_file_path("alter_table_schema.sql", f"sales_data\DDL")
)
sql.alter_and_update(
    get_absolute_file_path("add_weight_class_column_script.sql", r"sales_data\DML")
)
sql.alter_and_update(
    get_absolute_file_path("add_primary_keys.sql", r"sales_data\DDL")
)  # r'sales_data\DDL\add_primary_keys.sql')
sql.alter_and_update(
    get_absolute_file_path("orders_table_FK_constraints.sql", r"sales_data\DDL")
)
sql.alter_and_update(
    get_absolute_file_path("update_orders_table_foreign_keys.sql", r"sales_data\DML")
)  # r'sales_data\DML\update_orders_table_foreign_keys.sql')
sql.alter_and_update(
    get_absolute_file_path("dim_currency_FK_constraint.sql", r"sales_data\DDL")
)
sql.alter_and_update(
    get_absolute_file_path(
        "update_dim_currency_table_foreign_keys.sql", r"sales_data\DML"
    )
)  # r'sales_data\DML\update_dim_currency_table_foreign_keys.sql')


end_time = time.time()

execution_time = end_time - start_time

print(f"Execution time: {execution_time} seconds")
main_logger.info(f"Time elapsed : {execution_time} seconds")
