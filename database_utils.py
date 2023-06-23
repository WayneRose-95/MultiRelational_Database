import yaml
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError 
from logger import DatabaseLogger
import pandas as pd 
import os 


logger = DatabaseLogger("logs/database_utils.log")

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
        logger.info(f"Reading {config_file}")
        valid_extensions = ['.yaml', '.yml']
        name_of_file, file_extension = os.path.splitext(config_file)
        logger.info("Validating file extension")
        logger.info(name_of_file)
        logger.info(file_extension)
        if file_extension.lower() not in valid_extensions:
            logger.exception("Invalid file extension. Only YAML files are allowed")
            raise ValueError("Invalid file extension. Only YAML files are allowed.")
        # Read the yaml file
        logger.info(f"{config_file} successfully validated")
        try:
            logger.info(f"Opening {config_file}")
            with open(config_file) as file:
                database_credentials = yaml.safe_load(file)
                logger.info(f"Config file {config_file} loaded")
                logger.info(database_credentials)
             # Return the yaml file as a dictionary    
            return database_credentials
        # If the file is not found, raise an exception 
        except FileNotFoundError:
            logger.exception(f"Logging file {config_file} not found" )
            raise FileNotFoundError("Config file not found")
        # If the config file is not in a YAML format, raise an exception
        except yaml.YAMLError:
            logger.exception(f"Invalid file format" )
            logger.critical("Invalid .yaml file. Please check the formatting of your .yaml / .yml file")
            raise yaml.YAMLError("Invalid YAML format.")

    def create_connection_string(self, config_file_name):
        '''
        Method to create the connection_string needed to connect to a postgresql database 

        Parameters: 
        config_file_name: 
        The pathway to the config_file used to create the string 

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
        
        connection_string = f"postgresql+psycopg2://{database_credentials['RDS_USER']}:{database_credentials['RDS_PASSWORD']}@{database_credentials['RDS_HOST']}:{database_credentials['RDS_PORT']}/{database_credentials['RDS_DATABASE']}"
        logger.debug(f"Connection string created {connection_string}")
        return connection_string

    def initialise_database_connection(self, config_file_name):
        '''
        Method to establish a connection to the database 

        Parameters: 
        config_file_name: str 
        The file pathway to the yaml file

        Returns: 
        database_engine: engine 
        A database engine object 

        '''
        
        connection_string = self.create_connection_string(config_file_name)

        # Lastly try to connect to the database using your connection string variable
        try:
            logger.info("Creating database engine using sqlaclhemy's create_engine")
            database_engine = create_engine(connection_string)
            logger.info(f"Database Engine : {database_engine} created attempting to connect")
            database_engine.connect()
            logger.info("connection successful")
            print("connection successful")
            return database_engine
        except OperationalError:
            logger.exception("OperationalError detected. Error connecting to the database")
            print("Error Connecting to the Database")
            raise OperationalError 
    
        
         
    def upload_to_db(self, dataframe : pd.DataFrame , connection,  table_name : str): 
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
            logger.info(f"Attempting to upload table {table_name} to the database")
            
            dataframe.to_sql(table_name, con=connection, if_exists='replace')
            logger.info("Table Uploaded")
        except:
            logger.exception(f"Error uploading table to the database. Connection used : {connection}")
            print("Error uploading table to the database")
            raise Exception 
        pass


if __name__ == "__main__":
    new_database = DatabaseConnector()
    new_database.read_database_credentials('test_config_yaml.yaml') 
    # new_database.create_connection_string('sales_data_creds_dev.yaml')
    # new_database.initialise_database_connection('db_creds.yaml')
