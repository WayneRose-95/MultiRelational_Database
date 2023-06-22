from database_utils import DatabaseConnector 
from sqlalchemy import inspect
from sqlalchemy import select
from sqlalchemy import Table
from sqlalchemy import MetaData
from sqlalchemy.exc import OperationalError 
from io import StringIO
import pandas as pd 
import boto3
import tabula 
import re 

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

            # Initialise the connection 
            engine = database_connection.initialise_database_connection(config_file_name)

            # Use the inspect method of sqlalchemy to get an inspector element 
            inspector = inspect(engine)

            # Get the table names using the get_table_names method 
            table_names = inspector.get_table_names()
            
            # print the table names to the console 
            print(table_names)

            # Output: ['legacy_store_details', 'legacy_users', 'orders_table']

            return table_names
        except Exception as e:
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

            # Connect to the database 
            connection = connection.connect() 

            # Initialise a MetaData object 
            metadata = MetaData() 

            # Set a user table object 
            user_table = Table(table_name, metadata, autoload_with=connection)

            # Show the table
            print(metadata.tables.keys())

            # Do a select statement to select all rows of the table 
            print(select(user_table))

            # Declare a select statement on the table to select all rows of the table
            select_statement = str(select(user_table))

            # Pass this select statement into a pandas function, which reads the sql query 
            dataframe_table = pd.read_sql(select_statement, con=connection)

            # Return the dataframe_table as an output of the method
            return dataframe_table
        
        except OperationalError as e:
            # Handles connection query errors
            raise ValueError(f"Failed to read table '{table_name}': {e}")

        except Exception as e:
            # Handles other exceptions 
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
        # Read in the pdf_table using tabula-py ensuring all pages are captured 
        pdf_table = tabula.read_pdf(link_to_pdf, multiple_tables=True, pages='all', lattice=True)
        
        # Combine the list of tables using pd.concat 
        combined_table = pd.concat(pdf_table)

        # Reset the index upon combining the tables 
        combined_table.reset_index(drop=True, inplace=True)

        return combined_table 
           

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
        # Make an instance of the boto3 object to use s3
        s3_client = boto3.client('s3')

        # Assign the bucket_name and the key to a tuple which is the output of the previous function. 
        bucket_name, key = self._parse_s3_url(s3_url)

        # Use the get_object method to collect items in the bucket. Index it on the key. 
        response = s3_client.get_object(Bucket=bucket_name, Key=key)

        # Extract the data getting the values assigned to each key, then decode it using utf-8 encoding 
        data = response['Body'].read().decode('utf-8')

        # Lastly, read the csv into pandas as a dataframe, then return the dataframe as an output. 
        dataframe = pd.read_csv(StringIO(data), delimiter=',')  
        
        return dataframe
    
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
        # Example s3_url: s3://my-bucket-name/my-data.csv
        # Extracting bucket name and key from the URL
        # Done by replacing the first part of the s3_url with an empty string, then splitting it by the '/' character
        s3_url_splitted = s3_url.replace('s3://', '').split('/')

        # Print the s3_url_splitted variable for debugging purposes 
        print(s3_url_splitted)

        # Set the bucket_name variable to the first index of the list of strings
        bucket_name = s3_url_splitted[0]

        # Print the bucket name for debugging purposes 
        print(bucket_name)

        # Set the key variable to second index of the splitted string onwards, and join it to the '/' character. 
        key = '/'.join(s3_url_splitted[1:])

        # Print the key variable for debugging purposes 
        print(key)
        
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
        # Create an instance of the boto3 client for s3 
        s3_client = boto3.client('s3')

        # Set the bucket_name and key of the bucket to the output of the method
        bucket_name, key = self.parse_s3_url_json(bucket_url)

        # Try to get the object from the s3_bucket
        try:
            response = s3_client.get_object(Bucket=bucket_name, Key=key)
            # Get the body of the .json file and decode it with utf-8 encoding 
            json_data = response['Body'].read().decode('utf-8')
            # Lastly, read the json_data into a pandas dataframe and return it 
            df = pd.read_json(json_data)
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
        if match:
            bucket_name = match.group(1)
            key = match.group(2)
        # Else set the bucket_name and key to None 
        else:
            bucket_name = None
            key = None

        return bucket_name,key

  
    
if __name__ == "__main__":
    extract = DatabaseExtractor() 
    extract.list_db_tables('db_creds.yaml')
    # extract.read_rds_table('legacy_users', 'db_creds.yaml')
    # extract.retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")
    # extract._parse_s3_url("s3://data-handling-public/products.csv")
    # extract.read_s3_bucket_to_dataframe("s3://data-handling-public/products.csv")
            

     
     
    

