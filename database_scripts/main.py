
from data_cleaning import data_cleaning_logger as data_cleaning_logger
from data_extraction import data_extraction_logger as data_extraction_logger
from database_utils import database_utils_logger as database_utils_logger
from sql_transformations import sql_transformations_logger as sql_transformations_logger
from data_cleaning import perform_data_cleaning
from sql_transformations import perform_database_operations
import time 
import os 
import logging

def get_absolute_file_path(file_name, file_directory):
    # Retrieve the absolute path of the current script
    current_dir = os.path.dirname(os.path.abspath(__file__))

    # Construct the absolute file path for the file in the credentials directory
    file_path = os.path.join(current_dir, "..", file_directory, file_name)

    return file_path

log_filename = "logs/main.log"
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

file_pathway_to_source_database = get_absolute_file_path("db_creds.yaml", "credentials")
file_pathway_to_datastore = get_absolute_file_path("sales_data_creds_test.yaml", "credentials")
file_pathway_to_source_text_file = get_absolute_file_path("currency_code_mapping", "source_data_files")
file_pathway_to_json_source_file = get_absolute_file_path("country_data.json", "source_data_files")
file_pathway_to_exported_csv_file = get_absolute_file_path("currency_conversions_test", "source_data_files")

sql_transformations_file_path = get_absolute_file_path("sales_data_creds_test.yaml", "credentials")



# main_logger.info("Process started")

start_time = time.time()

data_cleaning_logger.info("Calling data_cleaning_script")
perform_data_cleaning(
      file_pathway_to_datastore,
      file_pathway_to_source_database,
      file_pathway_to_source_text_file,
      file_pathway_to_json_source_file,
      file_pathway_to_exported_csv_file 
)

sql_transformations_logger.info("Calling SQL transformations script")
perform_database_operations(sql_transformations_file_path)

end_time = time.time() 

execution_time = end_time - start_time 

print(f"Execution time: {execution_time} seconds")
main_logger.info(f"Time elapsed : {execution_time} seconds")