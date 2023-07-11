import yaml
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from database_scripts.file_handler import get_absolute_file_path
import pandas as pd 
import os 
import logging



'''
LOG CREATION 
'''
log_filename = get_absolute_file_path("database_utils.log", "logs") # "logs/database_utils.log"
if not os.path.exists(log_filename):
    os.makedirs(os.path.dirname(log_filename), exist_ok=True)

database_utils_logger = logging.getLogger(__name__)

# Set the default level as DEBUG
database_utils_logger.setLevel(logging.DEBUG)

# Format the logs by time, filename, function_name, level_name and the message
format = logging.Formatter(
    "%(asctime)s:%(filename)s:%(funcName)s:%(levelname)s:%(message)s"
)
file_handler = logging.FileHandler(log_filename)

# Set the formatter to the variable format

file_handler.setFormatter(format)

database_utils_logger.addHandler(file_handler)



class DatabaseConnector:
    def read_database_credentials(self, config_file: yaml):
        '''
        Method to read database_credentials from a yaml file 

        Parameters: 
        config_file  
        The file pathway to the yaml file 

        Returns: 
        database_credentials : dict 
        A dictionary of the database credentials from yaml file 

        '''

        # Check if the file extension is YAML
        database_utils_logger.info(f"Reading {config_file}")
        valid_extensions = ['.yaml', '.yml']
        name_of_file, file_extension = os.path.splitext(config_file)
        database_utils_logger.info("Validating file extension")
        database_utils_logger.info(name_of_file)
        database_utils_logger.info(file_extension)
        if file_extension.lower() not in valid_extensions:
            database_utils_logger.exception("Invalid file extension. Only YAML files are allowed")
            raise ValueError("Invalid file extension. Only YAML files are allowed.")
        # Read the yaml file
        database_utils_logger.info(f"{config_file} successfully validated")
        try:
            database_utils_logger.info(f"Opening {config_file}")
            with open(config_file) as file:
                database_credentials = yaml.safe_load(file)
                database_utils_logger.info(f"Config file {config_file} loaded")
                database_utils_logger.info(database_credentials)
             # Return the yaml file as a dictionary    
            return database_credentials
        # If the file is not found, raise an exception 
        except FileNotFoundError:
            database_utils_logger.exception(f"Logging file {config_file} not found" )
            raise FileNotFoundError("Config file not found")
        # If the config file is not in a YAML format, raise an exception
        except yaml.YAMLError:
            database_utils_logger.exception(f"Invalid file format" )
            database_utils_logger.critical("Invalid .yaml file. Please check the formatting of your .yaml / .yml file")
            raise yaml.YAMLError("Invalid YAML format.")

    def create_connection_string(self, config_file_name, connect_to_database=False, new_db_name=None):
        '''
        Method to create the connection_string needed to connect to a postgresql database 

        Parameters: 
        config_file_name: 
        The pathway to the config_file used to create the string 

        connect_to_database: bool
        Flag indicating whether to connect to a specific database within the server (default: False)

        new_db_name: str
        The name of the new database to connect to if connect_to_database is True (default: None)

        Returns: 
        Connection string : str 
        A string used to connect to the database 

        '''
        # Call the database details method to use the dictionary as an output
        database_credentials = self.read_database_credentials(config_file_name)
        # If the database_credentials method fails 
        # i.e. recieves the wrong weapon type 
        if not database_credentials:
            raise Exception("Invalid database credentials.")
        
        connection_string = f"postgresql+psycopg2://{database_credentials['RDS_USER']}:{database_credentials['RDS_PASSWORD']}@{database_credentials['RDS_HOST']}:{database_credentials['RDS_PORT']}"

        if connect_to_database:
            if not new_db_name:
                raise ValueError("New database name not provided")
            connection_string += f"/{new_db_name}"

        database_utils_logger.debug(f"Connection string created: {connection_string}")
        return connection_string

    def initialise_database_connection(self, config_file_name, connect_to_database=False, new_db_name=None, isolation_level="AUTOCOMMIT"):
        '''
        Method to establish a connection to the database 

        Parameters: 
        config_file_name: str 
        The file pathway to the yaml file

        connect_to_database: bool
        Flag indicating whether to connect to a specific database within the server (default: False)

        new_db_name: str
        The name of the new database to connect to if connect_to_database is True (default: None)

        Returns: 
        database_engine: engine 
        A database engine object

        isolation_level : str = AUTOCOMMIT 

        The level of isolation for the database transaction. 
        By default, this is set to AUTOCOMMIT


        '''
        
        connection_string = self.create_connection_string(config_file_name, connect_to_database, new_db_name)

        # Lastly try to connect to the database using your connection string variable
        try:
            database_utils_logger.info("Creating database engine using sqlaclhemy's create_engine")
            database_engine = create_engine(connection_string, isolation_level=isolation_level)
            database_utils_logger.info(f"Database Engine : {database_engine} created attempting to connect")
            database_engine.connect()
            database_utils_logger.info("connection successful")
            print("connection successful")
            return database_engine
        except OperationalError:
            database_utils_logger.exception("OperationalError detected. Error connecting to the database")
            print("Error Connecting to the Database")
            raise OperationalError 
    
        
         
    def upload_to_db(self, dataframe : pd.DataFrame , connection,  table_name : str, table_condition="append"): 
        '''
        Method to upload the table to the database 

        Parameters: 
        dataframe : pd.DataFrame
        A pandas dataframe 

        connection 
        The engine to connect to the database 

        table_name : str 
        The name of the table to be uploaded to the database 
        '''
        try:
            database_utils_logger.info(f"Attempting to upload table {table_name} to the database")
            
            dataframe.to_sql(table_name, con=connection, if_exists=table_condition)
            database_utils_logger.info("Table Uploaded")
        except:
            database_utils_logger.exception(f"Error uploading table to the database. Connection used : {connection}")
            print("Error uploading table to the database")
            raise Exception 
        pass


if __name__ == "__main__":
    yaml_file_path = get_absolute_file_path("db_creds.yaml", "credentials")
    new_database = DatabaseConnector()
    new_database.read_database_credentials(yaml_file_path)
    new_database.create_connection_string(yaml_file_path)
    new_database.initialise_database_connection(yaml_file_path)
    
