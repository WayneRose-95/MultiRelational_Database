import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError
from database_scripts.file_handler import get_absolute_file_path
import pandas as pd
import os
import logging


"""
LOG CREATION 
"""
log_filename = get_absolute_file_path(
    "database_utils.log", "logs"
)  # "logs/database_utils.log"
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
        """
        Method to read database_credentials from a yaml file

        Parameters:
        config_file
        The file pathway to the yaml file

        Returns:
        database_credentials : dict
        A dictionary of the database credentials from yaml file

        """

        # Check if the file extension is YAML
        database_utils_logger.info(f"Reading {config_file}")
        valid_extensions = [".yaml", ".yml"]
        name_of_file, file_extension = os.path.splitext(config_file)
        database_utils_logger.info("Validating file extension")
        database_utils_logger.info(name_of_file)
        database_utils_logger.info(file_extension)
        if file_extension.lower() not in valid_extensions:
            database_utils_logger.exception(
                "Invalid file extension. Only YAML files are allowed"
            )
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
            database_utils_logger.exception(f"Logging file {config_file} not found")
            raise FileNotFoundError("Config file not found")
        # If the config file is not in a YAML format, raise an exception
        except yaml.YAMLError:
            database_utils_logger.exception(f"Invalid file format")
            database_utils_logger.critical(
                "Invalid .yaml file. Please check the formatting of your .yaml / .yml file"
            )
            raise yaml.YAMLError("Invalid YAML format.")

    def create_connection_string(
        self, config_file_name, connect_to_database=False, new_db_name=None
    ):
        """
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

        """
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

    def initialise_database_connection(
        self,
        config_file_name,
        connect_to_database=False,
        new_db_name=None,
        isolation_level="AUTOCOMMIT",
    ):
        """
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


        """

        connection_string = self.create_connection_string(
            config_file_name, connect_to_database, new_db_name
        )

        # Lastly try to connect to the database using your connection string variable
        try:
            database_utils_logger.info(
                "Creating database engine using sqlaclhemy's create_engine"
            )
            database_engine = create_engine(
                connection_string, isolation_level=isolation_level
            )
            database_utils_logger.info(
                f"Database Engine : {database_engine} created attempting to connect"
            )
            database_engine.connect()
            database_utils_logger.info("connection successful")
            print("connection successful")
            return database_engine
        except OperationalError:
            database_utils_logger.exception(
                "OperationalError detected. Error connecting to the database"
            )
            print("Error Connecting to the Database")
            raise OperationalError

    def list_db_tables(self, engine : Engine): # , config_file_name: str, database_name: str
        """
        Method to list the tables within a database

        Parameters:
        engine : Engine 
        The database Engine object which is used to interact with the source or target database

        """
        try:
            # Initilise the database connection using the Engine object 
            database_utils_logger.info("Initialising database connection")
            database_utils_logger.info(
                f"Initialising database connection using {engine}"
            )
            # database_utils_logger.debug(f"Using {engine}")

            # Use the inspect method of sqlalchemy to get an inspector element
            inspector = inspect(engine)
            database_utils_logger.info("Inspecting database engine")

            # Get the table names using the get_table_names method
            table_names = inspector.get_table_names()
            database_utils_logger.info("Collecting table_names")
            database_utils_logger.info(f"List of table_names : {table_names}")
            # print the table names to the console
            print(table_names)

            # Output: ['legacy_store_details', 'legacy_users', 'orders_table']
            # Return the list of table names as an output 
            return table_names
        
        # Raise an Exception if there are any issues with listing the tables.   
        except Exception as e:
            database_utils_logger.exception(
                "An error occured while listing tables. Please verify your credentials"
            )
            print("Error occurred while listing tables: %s", str(e))
            raise Exception
        

    def upload_to_db(
        self,
        dataframe: pd.DataFrame,
        connection : Engine,
        table_name: str,
        table_condition : str = "append" or "replace" or "fail",
        mapping: dict = None,
        subset: list = None,
        additional_rows: list = None,
        column_datatypes=None
    ):
        """
        Method to upload the table to the database

        Parameters:
        dataframe : pd.DataFrame
        A pandas dataframe

        connection
        The engine to connect to the database

        table_name : str
        The name of the table to be uploaded to the database

        table_condition : str = "append" or "replace" or "fail" 
        The table condition specified either append, replace or fail

        mapping : dict = None 
        An optional parameter to apply mapping to the dataframe. 
        By default, it is None 

        subset : list = None 
        An optional parameter to filter the dataframe by a list of values. 
        By defult, it is None 

        additional_rows : list = None 
        An optional parameter to add additional rows to the start of the dataframe. 
        By default, it is None 
        """
        if mapping:
            # Apply the mapping to the specified column
            dataframe = dataframe.assign(
                availability=dataframe["availability"].map(mapping)
            )

        if subset:
            try:
                # Filter rows based on the specified subset
                dataframe = dataframe[
                    dataframe["country_code"].isin(subset)
                ]
            except KeyError:
                dataframe = dataframe[
                    dataframe["currency_code"].isin(subset)
                ]

        if additional_rows:
            # Add additional rows to the start of the table
            additional_rows_df = pd.DataFrame(additional_rows)
            dataframe = pd.concat(
                [additional_rows_df, dataframe]
            ).reset_index(drop=True)
        try:
            database_utils_logger.info(
                f"Attempting to upload table {table_name} to the database"
            )

            dataframe.to_sql(table_name, con=connection, if_exists=table_condition, dtype=column_datatypes)
            database_utils_logger.info("Table Uploaded")
        except:
            database_utils_logger.exception(
                f"Error uploading table to the database. Connection used : {connection}"
            )
            print("Error uploading table to the database")
            raise Exception
        
    def create_database(self, database_name: str, connection_string : str):
        # Create the database with the provided database_name and database_username
        
        connection = create_engine(connection_string)
        database_engine = connection.connect()
        if database_engine:
            # Disable transactional behavior
            database_engine.execution_options(isolation_level="AUTOCOMMIT")

            # Check if the database exists
            check_db_stmt = text(
                f"SELECT 1 FROM pg_database WHERE datname = '{database_name}'"
            )
            result = database_engine.execute(check_db_stmt)

            if result.scalar():
                # Database already exists, so skip database creation
                print(
                    f"Database '{database_name}' already exists. Skipping database creation."
                )
                database_utils_logger.warning(
                    f"Database '{database_name}' already exists. Skipping database creation."
                )
            else:
                # Create a new database
                create_db_stmt = text(f"CREATE DATABASE {database_name}")
                database_engine.execute(create_db_stmt)
                print(f"Database '{database_name}' created successfully.")
                database_utils_logger.info(
                    f"Database '{database_name}' created successfully."
                )

            # Close the connection
            database_engine.close()
        else:
            print("Connection to the database failed.")
            database_utils_logger.error("Connection to the database failed.")
   
    def alter_and_update(self, sql_file_path: str, database_engine : Engine):
        # Create a session object using sessionmaker
        sql_session = sessionmaker(bind=database_engine)
        session = sql_session()

        try:
            with open(sql_file_path, "r") as file:
                sql_statement = file.read()
                print(sql_statement)
            database_utils_logger.info(sql_statement)
            database_utils_logger.info("Executing and committing sql_statement")
            session.execute(text(sql_statement))
            session.commit()
            database_utils_logger.info(
                "SQL statement submitted to database. Please verify."
            )
            print("SQL statement submitted to database. Please verify.")
        except:
            database_utils_logger.exception(
                f"Error when running sql_statement. The sql statement submitted was {sql_statement}"
            )
            print(
                f"Error when running sql_statement. The sql statement submitted was {sql_statement}"
            )
            raise Exception

        finally:
            database_utils_logger.info("Closing Session")
            session.close()

if __name__ == "__main__":
    yaml_file_path = get_absolute_file_path("db_creds.yaml", "credentials")
    new_database = DatabaseConnector()
    new_database.read_database_credentials(yaml_file_path)
    new_database.create_connection_string(yaml_file_path)
    new_database.initialise_database_connection(yaml_file_path)
