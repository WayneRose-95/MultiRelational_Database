from database_utils import DatabaseConnector 
from logger import DatabaseLogger
from sqlalchemy import inspect
from sqlalchemy import select
from sqlalchemy import Table
from sqlalchemy import MetaData
from sqlalchemy.exc import OperationalError 
from urllib.parse import urlparse
from io import StringIO
import pandas as pd 
import boto3
import tabula 
import re 

logger = DatabaseLogger("logs/data_extraction.log")

class DatabaseExtractor:

    def __init__(self):
        pass

    def list_db_tables(self, config_file_name : str):
        '''
        Method to list the tables within a database 

        Parameters:
        config_file_name
        The file pathway to the config file 

        '''
        try:
            # Create an instance of the DatabaseConnector Class 
            database_connection = DatabaseConnector()

            logger.info("Initialising database connection")
            # Initialise the connection 
            engine = database_connection.initialise_database_connection(config_file_name)
            logger.info(f"Initialising database connection using {config_file_name}")
            logger.debug(f"Using {engine}")

            # Use the inspect method of sqlalchemy to get an inspector element 
            inspector = inspect(engine)
            logger.info("Inspecting database engine")

            # Get the table names using the get_table_names method 
            table_names = inspector.get_table_names()
            logger.info("Collecting table_names")
            logger.info("List of table_names : {table_names}")
            # print the table names to the console 
            print(table_names)

            # Output: ['legacy_store_details', 'legacy_users', 'orders_table']

            return table_names
        except Exception as e:
            logger.exception("An error occured while listing tables. Please verify your credentials")
            print("Error occurred while listing tables: %s", str(e))
            raise Exception 
            



    def read_rds_table(self, table_name :str, config_file_name : str):
        '''
        Method to read a table from an RDS and return a Pandas Dataframe 

        Parameters:
        table_name : str 
        The name of the table from the source database 

        config_file_name 
        The file pathway for the name of the .yaml file 
        '''
        try:
            # Instantiate an instance of the DatabaseConnector class 
            database_connector = DatabaseConnector()

            # Initialise the connection 
            connection = database_connector.initialise_database_connection(config_file_name)
            logger.info("Initialising connection to the database")
            logger.info(f"Using {connection}")

            # Connect to the database 
            connection = connection.connect() 
            logger.info(f"Successfully connected to database via {connection}")

            # Initialise a MetaData object 
            metadata = MetaData() 

            # Set a user table object 
            user_table = Table(table_name, metadata, autoload_with=connection)

            # Show the table
            print(metadata.tables.keys())
            logger.debug(f"List of tables within the database {metadata.tables.keys()}")

            # Do a select statement to select all rows of the table 
            print(select(user_table))
            logger.debug(f"SQL Statement Submitted to database : {print(select(user_table))}")
            # Declare a select statement on the table to select all rows of the table
            select_statement = str(select(user_table))

            logger.info("Creating DataFrame from {select_statement}")
            # Pass this select statement into a pandas function, which reads the sql query 
            dataframe_table = pd.read_sql(select_statement, con=connection)
            logger.info("Successfully created DataFrame")
            # Return the dataframe_table as an output of the method
            return dataframe_table
        
        except OperationalError as e:
            # Handles connection query errors
            logger.exception("Failed to read table, please ensure that the table_name is correct")
            raise ValueError(f"Failed to read table '{table_name}': {e}")

        except Exception as e:
            # Handles other exceptions 
            logger.exception("An error occured while reading the table. Please view the traceback message")
            raise ValueError(f"Error occured while reading table '{table_name}' : {e}")

            
    def retrieve_pdf_data(self, link_to_pdf : str):
        '''
        Method to retrieve pdf data using tabula-py

        Parameters: 
        link_to_pdf : str 
        The link to the pdf file 

        Returns: 
        combined_table : DataFrame 
        The combined Pandas DataFrame 
        '''
        logger.info("Validating PDF link")
        # Firstly, validate the url 
        if not self._is_valid_url(link_to_pdf):
            logger.exception("Invalid PDF Link. Please verify the validity of the link")
            logger.critical("Critical error. Please ensure that the PDF link is correct")
            raise ValueError("Invalid PDF Link")
        
        try:
            logger.info("Reading in PDF table")
            # Read in the pdf_table using tabula-py ensuring all pages are captured 
            pdf_table = tabula.read_pdf(link_to_pdf, multiple_tables=True, pages='all', lattice=True)
            logger.info("Successfully read PDF table.")

            if pdf_table:
                logger.info(f"Multiple tables found. Found {len(pdf_table)} tables")
                logger.debug(f"Number of tables : {len(pdf_table)}")
                logger.info(f"Concatentating {len(pdf_table)} tables")

                # Combine the list of tables using pd.concat 
                combined_table = pd.concat(pdf_table)
                logger.info(f"{len(pdf_table)} tables successfully combined")

                # Reset the index upon combining the tables 
                combined_table.reset_index(drop=True, inplace=True)
                logger.info("Index of table reset")

                logger.info("Combined Pandas Dataframe returned successfully")
                return combined_table
            else:
                logger.warning("No tables found inside the PDF. Empty DataFrame recieved")
                print("No tables found in PDF")
                return pd.DataFrame()

        except Exception as e:
            logger.exception("An error occured while retrieving PDF data")
            print(f"Error occured while retrieving PDF data: {str(e)}")
            raise Exception 
           

    def read_s3_bucket_to_dataframe(self, s3_url : str):
        '''
        Method to read data from an s3_bucket into a Pandas Dataframe

        Parameters: 
        s3_url 
        The url to the s3_bucket 

        Returns: 
        dataframe: DataFrame 
        A pandas dataframe for the raw data from the S3 bucket 

        '''
        logger.info(f"Validating {s3_url}...")
        # Validation of the s3_url format 
        if not self._validate_s3_url(s3_url):
            logger.exception("Failed to validate S3 URL due to its format. Please check the format.")
            raise ValueError("Invalid S3 URL format")
        logger.info(f"s3_url {s3_url} successfully validated")
        
        logger.info("Creating instance of s3_client using boto3")
        # Make an instance of the boto3 object to use s3
        s3_client = boto3.client('s3')
        logger.debug("s3_client created succesfully")
        logger.debug(s3_client)

        try:
            # Assign the bucket_name and the key to a tuple which is the output of the previous function. 
            bucket_name, key = self._parse_s3_url(s3_url)
            logger.info(f"Successfully parsed s3_url {s3_url}")

            logger.info(f"Collecting objects from the s3_bucket")
            logger.info(f"Using {bucket_name} and {key}")
            # Use the get_object method to collect items in the bucket. Index it on the key. 
            response = s3_client.get_object(Bucket=bucket_name, Key=key)

            logger.info("Reading data from s3_bucket")
            # Extract the data getting the values assigned to each key, then decode it using utf-8 encoding 
            data = response['Body'].read().decode('utf-8')

            logger.info("Extracted data from s3_bucket. Reading data into a dataframe")
            # Lastly, read the csv into pandas as a dataframe, then return the dataframe as an output. 
            dataframe = pd.read_csv(StringIO(data), delimiter=',')  
            logger.info("Successfully read data into a DataFrame")
            return dataframe
        except Exception as e:
            logger.exception(f"Error occurred while reading S3 bucket '{bucket_name}/{key}': {e}")
            print(f"Error occurred while reading S3 bucket '{bucket_name}/{key}': {e}")
            raise Exception
    
    def _validate_s3_url(self, s3_url: str) -> bool:
        '''
        Validate the format of the S3 URL

        Parameters:
        s3_url
        The URL to the S3 bucket

        Returns:
        bool: True if the format is valid, False otherwise
        '''
        pattern = r'^s3://[\w\-]+/.+$'
        logger.info(f"Validating s3_url using {pattern}")
        return bool(re.match(pattern, s3_url))
    
    def _parse_s3_url(self, s3_url : str):
        '''
        Utility method to parse an s3_url 

        Parameters: 
        s3_url 

        A url to the s3_bucket 

        Returns: 
        bucket, key : Tuple 

        A Tuple of strings representing the name of the bucket, and the bucket_key

        '''
        logger.info(f"Parsing s3_url {s3_url}")
        logger.info(f"Splitting s3_url")
        # Example s3_url: s3://my-bucket-name/my-data.csv
        # Extracting bucket name and key from the URL
        # Done by replacing the first part of the s3_url with an empty string, then splitting it by the '/' character
        s3_url_splitted = s3_url.replace('s3://', '').split('/')
        logger.info("Successfully splitted s3_url")
        logger.info(s3_url_splitted)
        # Print the s3_url_splitted variable for debugging purposes 
        print(s3_url_splitted)

        # Set the bucket_name variable to the first index of the list of strings
        bucket_name = s3_url_splitted[0]
        logger.debug(f"Name of the bucket : {bucket_name}")
        # Print the bucket name for debugging purposes 
        print(bucket_name)

        # Set the key variable to second index of the splitted string onwards, and join it to the '/' character. 
        key = '/'.join(s3_url_splitted[1:])
        logger.debug(f"The key for the s3_bucket : {key}")
        # Print the key variable for debugging purposes 
        print(key)
        logger.debug(f"bucket_name : {bucket_name} \n key : {key}")
        return bucket_name, key
    
    def read_json_from_s3(self, bucket_url : str):
        '''
        Method to read in a .json file from an s3_bucket on AWS 

        Parameters: 
        bucket_url : str 
        The link to the bucket 

        Returns 
        df: DataFrame 

        A pandas dataframe of the .json file 

        None if reading the .json from s3 throws an exception. 

        '''
        logger.info("Creating instance of s3_client")
        # Create an instance of the boto3 client for s3 
        s3_client = boto3.client('s3')
        logger.debug(f"Starting session using : {s3_client}")

        logger.info(f"Validating s3_url : {bucket_url}")
        # Set the bucket_name and key of the bucket to the output of the method
        bucket_name, key = self.parse_s3_url_json(bucket_url)

        # Try to get the object from the s3_bucket
        try:
            logger.info(f"Getting objects from .json file : {key}")
            response = s3_client.get_object(Bucket=bucket_name, Key=key)
            # Get the body of the .json file and decode it with utf-8 encoding 
            json_data = response['Body'].read().decode('utf-8')

            logger.info("Reading data into DataFrame")
            # Lastly, read the json_data into a pandas dataframe and return it 
            df = pd.read_json(json_data)
            logger.info("Succesfully read data into DataFrame")
            logger.debug(f"Number of rows : {len(df)}")
            return df
        # If the code runs into an exception, raise the exception. 
        except Exception as e:
            print(f"Error reading JSON from S3: {e}")
            raise Exception

    @staticmethod
    def parse_s3_url_json(url : str):
        '''
        Method to interpret an s3_url for a .json file 

        Parameters: 
        url : str 
        The url of the s3 bucket 

        Returns: 
        bucket,key : Tuple 
        A tuple containing the bucket name and the key of the s3_bucket. 
        '''
        # Extract the bucket name and key using a regex pattern 
        match = re.match(r'^https?://([^.]+)\..+/(.*)$', url)
        # if it matches the regex pattern, 
        # group the first and second matches and set it to the bucket_name and key variables respectively
        logger.info(f"Parsing .json s3_url using {match}")

        if match:
            bucket_name = match.group(1)
            key = match.group(2)
            logger.info("Bucket information retrieved successfully")
            logger.debug(f"bucket_name : {bucket_name}")
            logger.debug(f"key : {key}")
        # Else set the bucket_name and key to None 
        else:
            bucket_name = None
            key = None
            logger.warning(f"bucket_name and key are set to None. Please verify if {match} is correct.")
        
        return bucket_name,key

    def _is_valid_url(self, url):
        logger.info("Parsing PDF url")
        parsed_url = urlparse(url)
        # Returns True if the schema and netloc outputs are non-empty strings
        # False if they either string is empty due to an invalid URL. 
        logger.debug(parsed_url.scheme)
        logger.debug(parsed_url.netloc)
        return parsed_url.scheme and parsed_url.netloc
    
if __name__ == "__main__":
    extract = DatabaseExtractor() 
    extract.list_db_tables('db_creds.yaml')
    # extract.read_rds_table('legacy_users', 'db_creds.yaml')
    # extract.retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")
    # extract._parse_s3_url("s3://data-handling-public/products.csv")
    # extract.read_s3_bucket_to_dataframe("s3://data-handling-public/products.csv")
            

     
     
    

