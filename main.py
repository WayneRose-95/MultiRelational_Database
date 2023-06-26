
from data_cleaning import data_cleaning_logger as data_cleaning_logger
from data_extraction import data_extraction_logger as data_extraction_logger
from database_utils import database_utils_logger as database_utils_logger
from sql_transformations import sql_transformations_logger as sql_transformations_logger
from data_cleaning import perform_data_cleaning
from sql_transformations import perform_database_operations
import time 
import os 
import logging

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


main_logger.info("Process started")

start_time = time.time()

data_cleaning_logger.info("Calling data_cleaning_script")
perform_data_cleaning('sales_data_creds.yaml')

sql_transformations_logger.info("Calling SQL transformations script")
perform_database_operations('sales_data_creds.yaml')

end_time = time.time() 

execution_time = end_time - start_time 

print(f"Execution time: {execution_time} seconds")
main_logger.info(f"Time elapsed : {execution_time} seconds")