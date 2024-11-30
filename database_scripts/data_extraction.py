from database_scripts.file_handler import get_absolute_file_path
from sqlalchemy import inspect
from sqlalchemy import select
from sqlalchemy.engine import Engine
from sqlalchemy import Table
from sqlalchemy import MetaData
from sqlalchemy.exc import OperationalError
from urllib.parse import urlparse
from io import StringIO
import pandas as pd
import boto3
import tabula
import re
import os
import logging


"""
LOGS DEFINITION
"""
log_filename = "../logs/data_extraction.log"
if not os.path.exists(log_filename):
    os.makedirs(os.path.dirname(log_filename), exist_ok=True)

data_extraction_logger = logging.getLogger(__name__)

# Set the default level as DEBUG
data_extraction_logger.setLevel(logging.DEBUG)

# Format the logs by time, filename, function_name, level_name and the message
format = logging.Formatter(
    "%(asctime)s:%(filename)s:%(funcName)s:%(levelname)s:%(message)s"
)
file_handler = logging.FileHandler(log_filename)

# Set the formatter to the variable format

file_handler.setFormatter(format)

data_extraction_logger.addHandler(file_handler)


class DataExtractor:
    def __init__(self):
        pass 


    def read_rds_table(
        self, table_name: str, engine : Engine 
    ):
        """
        Method to read a table from an RDS and return a Pandas Dataframe

        Parameters:
        table_name : str
        The name of the table from the source database

        engine : Engine
        The Engine object which represents either the source or target database 

        """
        try:


            data_extraction_logger.info("Initialising connection to the database")
            data_extraction_logger.info(f"Using {engine}")

            # Connect to the database
            connection = engine.connect()

            data_extraction_logger.info(
                f"Successfully connected to database via {connection}"
            )

            dataframe_table = pd.read_sql_table(table_name, engine)
            return dataframe_table

        except OperationalError as e:
            # Handles connection query errors
            data_extraction_logger.exception(
                "Failed to read table, please ensure that the table_name is correct"
            )
            raise ValueError(f"Failed to read table '{table_name}': {e}")

        except Exception as e:
            # Handles other exceptions
            data_extraction_logger.exception(
                "An error occured while reading the table. Please view the traceback message"
            )
            raise Exception(f"Error occured while reading table '{table_name}' : {e}")

    def retrieve_pdf_data(self, link_to_pdf: str):
        """
        Method to retrieve pdf data using tabula-py

        Parameters:
        link_to_pdf : str
        The link to the pdf file

        Returns:
        combined_table : DataFrame
        The combined Pandas DataFrame
        """
        data_extraction_logger.info("Validating PDF link")
        # Firstly, validate the url
        if not self._is_valid_url(link_to_pdf):
            data_extraction_logger.exception(
                "Invalid PDF Link. Please verify the validity of the link"
            )
            data_extraction_logger.critical(
                "Critical error. Please ensure that the PDF link is correct"
            )
            raise ValueError("Invalid PDF Link")

        try:
            data_extraction_logger.info("Reading in PDF table")
            # Read in the pdf_table using tabula-py ensuring all pages are captured
            pdf_table = tabula.read_pdf(
                link_to_pdf, multiple_tables=True, pages="all", lattice=True
            )
            data_extraction_logger.info("Successfully read PDF table.")

            if pdf_table:
                data_extraction_logger.info(
                    f"Multiple tables found. Found {len(pdf_table)} tables"
                )
                data_extraction_logger.debug(f"Number of tables : {len(pdf_table)}")
                data_extraction_logger.info(f"Concatentating {len(pdf_table)} tables")

                # Combine the list of tables using pd.concat
                combined_table = pd.concat(pdf_table)
                data_extraction_logger.info(
                    f"{len(pdf_table)} tables successfully combined"
                )

                # Reset the index upon combining the tables
                combined_table.reset_index(drop=True, inplace=True)
                data_extraction_logger.info("Index of table reset")

                data_extraction_logger.info(
                    "Combined Pandas Dataframe returned successfully"
                )
                return combined_table
            else:
                data_extraction_logger.warning(
                    "No tables found inside the PDF. Empty DataFrame recieved"
                )
                print("No tables found in PDF")
                return pd.DataFrame()

        except Exception as e:
            data_extraction_logger.exception(
                "An error occured while retrieving PDF data"
            )
            print(f"Error occured while retrieving PDF data: {str(e)}")
            raise Exception

    def read_s3_bucket_to_dataframe(self, s3_url: str):
        """
        Method to read data from an s3_bucket into a Pandas Dataframe

        Parameters:
        s3_url
        The url to the s3_bucket

        Returns:
        dataframe: DataFrame
        A pandas dataframe for the raw data from the S3 bucket

        """
        data_extraction_logger.info(f"Validating {s3_url}...")
        # Validation of the s3_url format
        if not self._validate_s3_url(s3_url):
            data_extraction_logger.exception(
                "Failed to validate S3 URL due to its format. Please check the format."
            )
            raise ValueError("Invalid S3 URL format")
        data_extraction_logger.info(f"s3_url {s3_url} successfully validated")

        data_extraction_logger.info("Creating instance of s3_client using boto3")
        # Make an instance of the boto3 object to use s3
        s3_client = boto3.client("s3")
        data_extraction_logger.debug("s3_client created succesfully")
        data_extraction_logger.debug(s3_client)

        try:
            # Assign the bucket_name and the key to a tuple which is the output of the previous function.
            bucket_name, key = self._parse_s3_url(s3_url)
            data_extraction_logger.info(f"Successfully parsed s3_url {s3_url}")

            data_extraction_logger.info(f"Collecting objects from the s3_bucket")
            data_extraction_logger.info(f"Using {bucket_name} and {key}")
            # Use the get_object method to collect items in the bucket. Index it on the key.
            response = s3_client.get_object(Bucket=bucket_name, Key=key)

            data_extraction_logger.info("Reading data from s3_bucket")
            # Extract the data getting the values assigned to each key, then decode it using utf-8 encoding
            data = response["Body"].read().decode("utf-8")

            data_extraction_logger.info(
                "Extracted data from s3_bucket. Reading data into a dataframe"
            )
            # Lastly, read the csv into pandas as a dataframe, then return the dataframe as an output.
            dataframe = pd.read_csv(StringIO(data), delimiter=",")
            data_extraction_logger.info("Successfully read data into a DataFrame")
            return dataframe
        except Exception as e:
            data_extraction_logger.exception(
                f"Error occurred while reading S3 bucket '{bucket_name}/{key}': {e}"
            )
            print(f"Error occurred while reading S3 bucket '{bucket_name}/{key}': {e}")
            raise Exception

    def _validate_s3_url(self, s3_url: str) -> bool:
        """
        Validate the format of the S3 URL

        Parameters:
        s3_url
        The URL to the S3 bucket

        Returns:
        bool: True if the format is valid, False otherwise
        """
        pattern = r"^s3://[\w\-]+/.+$"
        data_extraction_logger.info(f"Validating s3_url using {pattern}")
        return bool(re.match(pattern, s3_url))

    def _parse_s3_url(self, s3_url: str):
        """
        Utility method to parse an s3_url

        Parameters:
        s3_url

        A url to the s3_bucket

        Returns:
        bucket, key : Tuple

        A Tuple of strings representing the name of the bucket, and the bucket_key

        """
        data_extraction_logger.info(f"Parsing s3_url {s3_url}")
        data_extraction_logger.info(f"Splitting s3_url")
        # Example s3_url: s3://my-bucket-name/my-data.csv
        # Extracting bucket name and key from the URL
        # Done by replacing the first part of the s3_url with an empty string, then splitting it by the '/' character
        s3_url_splitted = s3_url.replace("s3://", "").split("/")
        data_extraction_logger.info("Successfully splitted s3_url")
        data_extraction_logger.info(s3_url_splitted)
        # Print the s3_url_splitted variable for debugging purposes
        print(s3_url_splitted)

        # Set the bucket_name variable to the first index of the list of strings
        bucket_name = s3_url_splitted[0]
        data_extraction_logger.debug(f"Name of the bucket : {bucket_name}")
        # Print the bucket name for debugging purposes
        print(bucket_name)

        # Set the key variable to second index of the splitted string onwards, and join it to the '/' character.
        key = "/".join(s3_url_splitted[1:])
        data_extraction_logger.debug(f"The key for the s3_bucket : {key}")
        # Print the key variable for debugging purposes
        print(key)
        data_extraction_logger.debug(f"bucket_name : {bucket_name} \n key : {key}")
        return bucket_name, key

    def read_json_local(self, file_pathway):
        """
        Method to read in a .json file into a pandas dataframe

        Parameters:
        file_pathway:
        The path to the .json file

        returns:
        dataframe : pd.DataFrame
        A pandas dataframe of the .json file read into the system

        """
        try:
            data_extraction_logger.info(f"Reading in .json file from {file_pathway}")
            dataframe = pd.read_json(file_pathway, orient="index")
            data_extraction_logger.info(
                f"Successfully read .json file from {file_pathway}"
            )
            return dataframe

        except Exception:
            data_extraction_logger.exception(
                "Failed to read .json file into dataframe. Please check the file pathway"
            )
            raise Exception


    def read_json_from_s3(self, bucket_url: str):
        """
        Method to read in a .json file from an s3_bucket on AWS

        Parameters:
        bucket_url : str
        The link to the bucket

        Returns
        df: DataFrame

        A pandas dataframe of the .json file

        None if reading the .json from s3 throws an exception.

        """
        data_extraction_logger.info("Creating instance of s3_client")
        # Create an instance of the boto3 client for s3
        s3_client = boto3.client("s3")
        data_extraction_logger.debug(f"Starting session using : {s3_client}")

        data_extraction_logger.info(f"Validating s3_url : {bucket_url}")
        # Set the bucket_name and key of the bucket to the output of the method
        bucket_name, key = self.parse_s3_url_json(bucket_url)

        # Try to get the object from the s3_bucket
        try:
            data_extraction_logger.info(f"Getting objects from .json file : {key}")
            response = s3_client.get_object(Bucket=bucket_name, Key=key)
            # Get the body of the .json file and decode it with utf-8 encoding
            json_data = response["Body"].read().decode("utf-8")

            data_extraction_logger.info("Reading data into DataFrame")
            # Lastly, read the json_data into a pandas dataframe and return it
            df = pd.read_json(json_data)
            data_extraction_logger.info("Succesfully read data into DataFrame")
            data_extraction_logger.debug(f"Number of rows : {len(df)}")
            return df
        # If the code runs into an exception, raise the exception.
        except Exception as e:
            print(f"Error reading JSON from S3: {e}")
            raise Exception

    @staticmethod
    def parse_s3_url_json(url: str):
        """
        Method to interpret an s3_url for a .json file

        Parameters:
        url : str
        The url of the s3 bucket

        Returns:
        bucket,key : Tuple
        A tuple containing the bucket name and the key of the s3_bucket.
        """
        # Extract the bucket name and key using a regex pattern
        match = re.match(r"^https?://([^.]+)\..+/(.*)$", url)
        # if it matches the regex pattern,
        # group the first and second matches and set it to the bucket_name and key variables respectively
        data_extraction_logger.info(f"Parsing .json s3_url using {match}")

        if match:
            bucket_name = match.group(1)
            key = match.group(2)
            data_extraction_logger.info("Bucket information retrieved successfully")
            data_extraction_logger.debug(f"bucket_name : {bucket_name}")
            data_extraction_logger.debug(f"key : {key}")
        # Else set the bucket_name and key to None
        else:
            bucket_name = None
            key = None
            data_extraction_logger.warning(
                f"bucket_name and key are set to None. Please verify if {match} is correct."
            )

        return bucket_name, key

    def _is_valid_url(self, url):
        data_extraction_logger.info("Parsing PDF url")
        parsed_url = urlparse(url)
        # Returns True if the schema and netloc outputs are non-empty strings
        # False if they either string is empty due to an invalid URL.
        data_extraction_logger.debug(parsed_url.scheme)
        data_extraction_logger.debug(parsed_url.netloc)
        return parsed_url.scheme and parsed_url.netloc


if __name__ == "__main__":
    credentials_file_path = get_absolute_file_path("db_creds.yaml", "credentials")
    source_data_file_path = get_absolute_file_path(
        "currency_conversions_test", "source_data_files"
    )
    source_json_file_path = get_absolute_file_path(
        "country_data.json", "source_data_files"
    )
    extract = DataExtractor()
    extract.read_json_local(source_json_file_path)
    extract.read_rds_table("legacy_users", credentials_file_path, "postgres")
    extract.retrieve_pdf_data(
        "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
    )
    extract._parse_s3_url("s3://data-handling-public/products.csv")
    extract.read_s3_bucket_to_dataframe("s3://data-handling-public/products.csv")
