from database_scripts.file_handler import get_absolute_file_path
from sqlalchemy.exc import OperationalError
from sqlalchemy.engine import Engine
from sqlalchemy.dialects.postgresql import BIGINT, UUID
from datetime import datetime
from typing import Optional
import pandas as pd
import numpy as np
import re
import yaml
import os
import logging
import pytz



log_filename = get_absolute_file_path(
    "data_cleaning.log", "logs"
)  # "logs/data_cleaning.log"
if not os.path.exists(log_filename):
    os.makedirs(os.path.dirname(log_filename), exist_ok=True)

data_cleaning_logger = logging.getLogger(__name__)

# Set the default level as DEBUG
data_cleaning_logger.setLevel(logging.DEBUG)

# Format the logs by time, filename, function_name, level_name and the message
format = logging.Formatter(
    "%(asctime)s:%(filename)s:%(funcName)s:%(levelname)s:%(message)s"
)
file_handler = logging.FileHandler(log_filename)

# Set the formatter to the variable format

file_handler.setFormatter(format)

data_cleaning_logger.addHandler(file_handler)


class DataCleaning:
    def __init__(self): 
        pass 

    def clean_user_data(
        self,
        source_database_engine : Engine,
        legacy_users_dataframe : pd.DataFrame,
        source_table_name: str,
        # source_database_config_file_name: str,
        # name_of_source_database: str
        
    ):
        """
        Method to clean the user data table

        Parameters:
        source_database_engine : Engine
        The Engine object for the source database

        legacy_users_dataframe : pd.DataFrame 
        A dataframe which contains information from the legacy_users table 

        source_table_name : str 
        The name of the source table from the source database 

        Returns:
        legacy_users_dataframe: Dataframe
        Pandas dataframe used to upload to the database.

        """
        data_cleaning_logger.info("Starting job clean_user_data")
        data_cleaning_logger.info(f"Attempting to clean {source_table_name}")
        data_cleaning_logger.debug(f"Connecting to {source_database_engine}")
        data_cleaning_logger.info("Reading in table from source database")


        data_cleaning_logger.debug(f"Number of rows : {len(legacy_users_dataframe)}")

        data_cleaning_logger.info("Cleaning date columns : join_date and birth_date")
        data_cleaning_logger.info("Attempting to clean dates into correct format")
        # Apply the clean_dates function to the dataframe
        legacy_users_dataframe["join_date"] = legacy_users_dataframe["join_date"].apply(
            self.clean_dates
        )
        legacy_users_dataframe["date_of_birth"] = legacy_users_dataframe[
            "date_of_birth"
        ].apply(self.clean_dates)

        data_cleaning_logger.info(
            "Dropping rows within birth_date and join_date columns which have nulls"
        )
        # Drop all columns with nulls in their dates from the birth and join date columns
        legacy_users_dataframe = legacy_users_dataframe.dropna(
            subset=["date_of_birth", "join_date"]
        )
        data_cleaning_logger.debug(f"Number of rows : {len(legacy_users_dataframe)}")

        data_cleaning_logger.info("resetting the index of the Dataframe")
        # Reset the index if desired
        legacy_users_dataframe = legacy_users_dataframe.reset_index(drop=True)

        data_cleaning_logger.debug("Setting the index column of the table to 1")
        legacy_users_dataframe.index = legacy_users_dataframe.index + 1

        data_cleaning_logger.info(
            "Setting the value of the user_key to the index column"
        )
        legacy_users_dataframe["user_key"] = legacy_users_dataframe.index

        data_cleaning_logger.info("Creating a new dataframe to handle unknowns")
        data_cleaning_logger.info("Reordering the columns")
        column_order = [
            "user_key",
            "first_name",
            "last_name",
            "date_of_birth",
            "company",
            "email_address",
            "address",
            "country",
            "country_code",
            "phone_number",
            "join_date",
            "user_uuid",
        ]
        legacy_users_dataframe = legacy_users_dataframe[column_order]

        data_cleaning_logger.info("Appended new rows to the beginning of the dataframe")
        data_cleaning_logger.debug(f"Number of rows : {len(legacy_users_dataframe)}")
        data_cleaning_logger.debug(f"Job clean_user_data has completely successfully")
        print('Job clean_user_data has been completed')
        return legacy_users_dataframe

    def clean_store_data(
        self,
        source_database_engine : Engine,
        legacy_store_dataframe : pd.DataFrame,
        source_table_name: str,
        # source_database_config_file_name: str,
        # name_of_source_database: str

    ):
        """
        Method to reads in store_data from an RDS,
        cleans it then uploads it to the datastore

        Parameters:
        source_table_name : str
        The table name in the source database

        source_database_config_file_name : str
        The name of the config file used to connect to the source database

        datastore_table_name : str
        The name of the table which will be uploaded to the datastore

        Returns:

        legacy_store_database_table : DataFrame
        A cleaned version of the source data as a Pandas DataFrame

        """
        data_cleaning_logger.info("Starting Job clean_store_data")
        data_cleaning_logger.info(f"Attempting to clean {source_table_name}")
        data_cleaning_logger.debug(f"Connecting to {source_database_engine}")
        data_cleaning_logger.info("Reading in table from the source database")

        data_cleaning_logger.info("Database table successfully read.")
        data_cleaning_logger.info(f"Number of rows : {len(legacy_store_dataframe)}")

        data_cleaning_logger.info("Stating column names for the table")
        # State the column names for the table
        legacy_store_dataframe.columns = [
            "store_key",
            "store_address",
            "longitude",
            "null_column",
            "city",
            "store_code",
            "number_of_staff",
            "opening_date",
            "store_type",
            "latitude",
            "country_code",
            "region",
        ]

        data_cleaning_logger.debug("Column names")
        data_cleaning_logger.debug(legacy_store_dataframe.columns)

        data_cleaning_logger.info("Dropping null_column within the table")
        # Drop the null_column column within the table
        legacy_store_dataframe = legacy_store_dataframe.drop("null_column", axis=1)
        data_cleaning_logger.info("null_column dropped")
        data_cleaning_logger.info(f"Number of rows : {len(legacy_store_dataframe)}")

        data_cleaning_logger.info("Reordering the columns")
        # Reorder the columns
        column_order = [
            "store_key",
            "store_address",
            "longitude",
            "latitude",
            "city",
            "store_code",
            "number_of_staff",
            "opening_date",
            "store_type",
            "country_code",
            "region",
        ]
        data_cleaning_logger.info(column_order)
        # remake the dataframe with the column order in place
        legacy_store_dataframe = legacy_store_dataframe[column_order]
        data_cleaning_logger.info("Dataframe remade in column order")
        data_cleaning_logger.debug(legacy_store_dataframe.columns)

        data_cleaning_logger.info(
            "Applying the clean_date method to the opening_date column"
        )
        # Change the opening_date column to a datetime
        legacy_store_dataframe["opening_date"] = legacy_store_dataframe[
            "opening_date"
        ].apply(self.clean_dates)

        data_cleaning_logger.info(
            "Dropping dates from the opening_date column which are null"
        )
        # Drop dates in the opening_date which are null
        legacy_store_dataframe = legacy_store_dataframe.dropna(subset=["opening_date"])
        data_cleaning_logger.info("Dropped columns")
        data_cleaning_logger.info(f"Number of rows : {len(legacy_store_dataframe)}")

        data_cleaning_logger.info("Resetting the index of the table")
        # Reset the index if desired
        legacy_store_dataframe = legacy_store_dataframe.reset_index(drop=True)

        data_cleaning_logger.info("Setting the index column to start from 1")
        # Set the store_key column as the index column to reallign it with the index column
        # legacy_store_dataframe['store_key'] = legacy_store_dataframe.index
        # Set the index to start from 1 instead of 0
        legacy_store_dataframe.index = legacy_store_dataframe.index + 1

        data_cleaning_logger.debug(
            "Setting the store_key to the same as the index column"
        )
        legacy_store_dataframe["store_key"] = legacy_store_dataframe.index

        data_cleaning_logger.info(
            "Replacing the incorrectly spelleed regions with the correct spelling"
        )

        data_cleaning_logger.info("Unique values of column")
        data_cleaning_logger.info(legacy_store_dataframe["region"].unique())
        # Replace the Region with the correct spelling
        legacy_store_dataframe = legacy_store_dataframe.replace("eeEurope", "Europe")
        legacy_store_dataframe = legacy_store_dataframe.replace("eeAmerica", "America")

        data_cleaning_logger.info("Unique values of column")
        data_cleaning_logger.info(legacy_store_dataframe["region"].unique())

        data_cleaning_logger.info("Converting longitude column to a numeric value")
        # Clean the longitude column by converting it to a numeric value
        legacy_store_dataframe["longitude"] = pd.to_numeric(
            legacy_store_dataframe["longitude"], errors="coerce"
        )

        data_cleaning_logger.info(
            "Replacing values in the longitude column to the correct values"
        )
        #TODO: This needs a refactor. Do it either SQL or Python? 
        legacy_store_dataframe["number_of_staff"] = legacy_store_dataframe[
            "number_of_staff"
        ].replace({"3n9": "39", "A97": "97", "80R": "80", "J78": "78", "30e": "30"})

        data_cleaning_logger.info("Affected values")
        data_cleaning_logger.info(
            {"3n9": "39", "A97": "97", "80R": "80", "J78": "78", "30e": "30"}
        )

        data_cleaning_logger.debug(f"Number of Rows : {len(legacy_store_dataframe)}")

        data_cleaning_logger.info("Job clean_store_data has completed succesfully")
        print("Job clean_store_data has completed succesfully")
        return legacy_store_dataframe

    def clean_card_details(self, card_details_table : pd.DataFrame):
        """
        Method to clean a pdf file with card details and upload it to a datastore

        Parameters:

        link_to_pdf : str
        The link to the pdf file

        Returns

        card_details_database_table
        A dataframe consisting of card_details read from the pdf

        """
        data_cleaning_logger.info("Starting Job clean_card_details")
        data_cleaning_logger.info(f"Number of rows : {len(card_details_table)}")
        data_cleaning_logger.info(
            "Removing non-numeric characters from the card_number column"
        )

        # removing non-numeric characters from the card_number column
        card_details_table["card_number"] = card_details_table["card_number"].apply(
            lambda x: re.sub(r"\D", "", str(x))
        )

        data_cleaning_logger.info("List of unique card_providers")
        data_cleaning_logger.info(card_details_table["card_provider"].unique())
        # Define the items to remove
        items_to_remove = [
            "NULL",
            "NB71VBAHJE",
            "WJVMUO4QX6",
            "JRPRLPIBZ2",
            "TS8A81WFXV",
            "JCQMU8FN85",
            "5CJH7ABGDR",
            "DE488ORDXY",
            "OGJTXI6X1H",
            "1M38DYQTZV",
            "DLWF2HANZF",
            "XGZBYBYGUW",
            "UA07L7EILH",
            "BU9U947ZGV",
            "5MFWFBZRM9",
        ]
        data_cleaning_logger.info(
            "Filtering out the last 15 entries of the card_provider column"
        )
        data_cleaning_logger.info(items_to_remove)

        # Filter out the last 15 entries from the card provider column
        card_details_table = card_details_table[
            ~card_details_table["card_provider"].isin(items_to_remove)
        ]
        data_cleaning_logger.info("Card_details filtered")
        data_cleaning_logger.info(f"Number of rows : {len(card_details_table)}")

        data_cleaning_logger.info(
            "Applying the clean_dates method to to the dataframe to clean the date values"
        )
        # Apply the clean_dates method to the dataframe to clean the date values
        card_details_table["date_payment_confirmed"] = card_details_table[
            "date_payment_confirmed"
        ].apply(self.clean_dates)

        data_cleaning_logger.info(
            "Converting date_payment_confirmed column to a datetime"
        )
        # Convert the date_payment_confirmed column into a datetime
        card_details_table["date_payment_confirmed"] = pd.to_datetime(
            card_details_table["date_payment_confirmed"], errors="coerce"
        )

        data_cleaning_logger.info("For any null values, drop them from the table")
        # For any null values, drop them.
        card_details_table = card_details_table.dropna(
            subset=["date_payment_confirmed"]
        )
        data_cleaning_logger.info("Rows dropped")
        data_cleaning_logger.info(f"Number of rows : {len(card_details_table)}")

        data_cleaning_logger.debug(
            "Setting the dataframe index to start at 1 instead of 0"
        )
        # Set the index of the dataframe to start at 1 instead of 0
        card_details_table.index = card_details_table.index + 1
        # Add a new column called card_key, which is the length of the index column of the card_details table
        data_cleaning_logger.debug(
            "Setting the card_key to the same as the index column"
        )
        card_details_table["card_key"] = card_details_table.index

        data_cleaning_logger.info("Rearranging the order of the columns")
        # Rearrange the order of the columns
        column_order = [
            "card_key",
            "card_number",
            "expiry_date",
            "card_provider",
            "date_payment_confirmed",
        ]

        data_cleaning_logger.info(column_order)
        # Set the order of the columns in the table
        card_details_table = card_details_table[column_order]

        data_cleaning_logger.info("New column order")
        data_cleaning_logger.info(card_details_table.columns)

        data_cleaning_logger.info("Resetting the index of the table")
        # Reset the index of the table to match the indexes to the card_keys
        card_details_table = card_details_table.reset_index(drop=True)

        # data_cleaning_logger.info("Adding new rows to the table to cover for unknowns")
        # # Add new rows to the table
        # new_rows_additions = self.add_new_rows(
        #     [
        #         {"card_key": -1, "card_number": "Not Applicable"},
        #         {"card_key": 0, "card_number": "Unknown"},
        #     ]
        # )
        # data_cleaning_logger.info("New rows added")
        # data_cleaning_logger.info(new_rows_additions)

        # data_cleaning_logger.info(
        #     "Concatentating the two dataframes together to add the new rows to the top"
        # )
        # # Concatentate the two dataframes together
        # card_details_table = pd.concat(
        #     [new_rows_additions, card_details_table]
        # ).reset_index(drop=True)
        data_cleaning_logger.info(f"Number of rows : {len(card_details_table)}")
        print("Job clean_card_details has been completed successfully")
        return card_details_table

    def clean_orders_table(
        self,
        source_database_engine: Engine,
        orders_dataframe: pd.DataFrame,
        orders_table_name : str
    ):
        '''
        Method to clean the orders fact table from the AWS RDS 

        Parameters: 
        source_table_name : str 
        The name of the table present in the source database 

        source_database_config_file_name : str 
        The name of the config file which connects to the source database

        name_of_source_database : str 
        The name of the source database

        '''

        data_cleaning_logger.info("Starting job clean_orders_table")
        data_cleaning_logger.info("Reading in the table from the source database")
        data_cleaning_logger.info(f"Attempting to clean {orders_table_name}")
        data_cleaning_logger.debug(f"Connecting to {source_database_engine}")

        data_cleaning_logger.info(f"Number of rows : {len(orders_dataframe)}")

        data_cleaning_logger.info("Stating the name of the columns")
        # State the names of the columns
        orders_dataframe.columns = [
            "order_key",
            "null_key",
            "date_uuid",
            "first_name",
            "last_name",
            "user_uuid",
            "card_number",
            "store_code",
            "product_code",
            "null_column",
            "product_quantity",
        ]
        data_cleaning_logger.info("Names of columns")
        data_cleaning_logger.info(orders_dataframe.columns)

        data_cleaning_logger.info("Dropping columns from the dataframe")
        data_cleaning_logger.debug("Columns to drop")
        data_cleaning_logger.debug(
            ["null_key", "first_name", "last_name", "null_column"]
        )
        # Drop the following columns within the dataframe if they exist
        orders_dataframe.drop(
            ["null_key", "first_name", "last_name", "null_column"], axis=1, inplace=True
        )

        data_cleaning_logger.debug("Columns dropped")
        data_cleaning_logger.debug(orders_dataframe.columns)
        data_cleaning_logger.info(f"Number of rows : {len(orders_dataframe)}")

        # Addition of FK key columns and country_code column
        foreign_key_columns = [
            "card_key",
            "date_key",
            "product_key",
            "store_key",
            "user_key",
            "currency_key",
            "country_code",
        ]

        # Set the columns to null
        for column in foreign_key_columns:
            orders_dataframe[column] = np.nan

        column_order = [
            "order_key",
            "date_uuid",
            "user_uuid",
            "card_key",
            "date_key",
            "product_key",
            "store_key",
            "user_key",
            "currency_key",
            "card_number",
            "store_code",
            "product_code",
            "product_quantity",
            "country_code",
        ]

        orders_dataframe = orders_dataframe[column_order]

        data_cleaning_logger.info("Job clean_orders_table has completed successfully.")
        print("Job clean_orders_table has completed successfully.")
        return orders_dataframe 

    def clean_time_event_table(self, time_df : pd.DataFrame):
        """
        Method to read in a time_dimension table from an AWS S3 Bucket,
        clean it, and upload it to the datastore.

        Parameters:
        bucket_url : str
        A link to the s3 bucket hosted on AWS

        datastore_table_name : str
        The name of the table to be uploaded to the datastore

        dimension_table_name : str
        The name of the table to be uploaded to the datastore
        """
        data_cleaning_logger.info("Starting job clean_time_event_table")
        data_cleaning_logger.info("Reading file from s3_bucket into dataframe")
        # Read in the json data from the s3 bucket
        data_cleaning_logger.info(f"Number of rows : {len(time_df)}")

        data_cleaning_logger.info(
            "Filtering out non-numeric values from the 'month' column"
        )
        # Filter out non-numeric values and convert the column to numeric using boolean mask
        numeric_mask = time_df["month"].apply(pd.to_numeric, errors="coerce").notna()
        # copy the dataframe then convert the month column to a numeric datatype
        time_df = time_df[numeric_mask].copy()
        time_df["month"] = pd.to_numeric(time_df["month"])
        # Afterwards, drop any values which are not null or not numeric
        time_df = time_df.dropna(subset=["month"])

        data_cleaning_logger.info(f"Number of rows : {len(time_df)}")

        # TODO: Write a piece of code which verifies that the number of rows in the orders_table
        # is equal to the number of rows in the dim_date_times table
        # Currently, the operation drops 38 rows
        # 120161 - 120123 = 38
        # Correct number because the orders table has 120123 rows, so we have a time event per order.
        data_cleaning_logger.debug(f"Setting the index to start at 1")
        time_df.index = time_df.index + 1

        data_cleaning_logger.debug(f"Setting the time_key column to start at 1")
        time_df["date_key"] = time_df.index

        data_cleaning_logger.info("Resetting the index of the table")
        # Reset the index
        time_df = time_df.reset_index(drop=True)
        # Adding a new column full_date which is the full date YYYY-MM-DD format 
        time_df['full_date'] = pd.to_datetime(time_df[['year', 'month', 'day']]).dt.strftime('%Y-%m-%d')

        data_cleaning_logger.info("Changing the column order of the table")
        # Lastly, change the column order
        column_order = [
            "date_key",
            "date_uuid",
            "day",
            "month",
            "year",
            "timestamp",
            "full_date",
            "time_period"
             
        ]
        data_cleaning_logger.info(column_order)

        time_df = time_df[column_order]

        data_cleaning_logger.info("New column order")
        data_cleaning_logger.info(time_df.columns)

        data_cleaning_logger.info(
            "Job clean_time_event_table has completed successfully"
        )
        print("Job clean time_event_table has been completed sucessfully")
        return time_df

    def clean_product_table(self, products_table : pd.DataFrame):
        """
        Method to read in a .csv file from an S3 Bucket on AWS,
        CLean the data, and then upload it to the datastore

        Parameters:

        s3_bucket_url : str
        The link to the s3_bucket

        datastore_table_name : str
        The name of the table uploaded to the datastore

        dimension_table_name : str
        The name of the table uploaded to the datastore

        Returns
        products_table:
        A dataframe containing the cleaned products_table

        """
        data_cleaning_logger.info("Starting job clean_product_table")

        data_cleaning_logger.info(f"Number of rows : {len(products_table)}")

        data_cleaning_logger.info(
            "Creating a list of unique values from within the removed column"
        )
        # Create a list of unique values within the 'removed' column  and print them out for debugging purposes
        values = list(products_table["removed"].unique())
        print(values)

        data_cleaning_logger.info("Filtering out the last three values inside the list")
        # Filter out the last 3 values of the values inside the list using a boolean mask
        products_table = products_table[~products_table["removed"].isin(values[-3:])]
        data_cleaning_logger.info(f"Number of rows : {len(products_table)}")

        data_cleaning_logger.info(
            "Converting dates in the date_added column to a datetime"
        )
        # Convert the values in the date_added column to a datetime turning any invalid dates to NaNs
        products_table["date_added"] = pd.to_datetime(
            products_table["date_added"], errors="coerce"
        )

        data_cleaning_logger.info("Removing the £ sign in the product_price column")
        # Strip the £ sign from each of the prices within the product_price column
        products_table["product_price"] = products_table["product_price"].str.replace(
            "£", ""
        )

        data_cleaning_logger.info(
            "Applying the convert_to_kg method to the weight column"
        )
        # Apply the convert_to_kg method to the 'weight' column to standardise the weights to kg
        products_table["weight"] = products_table["weight"].apply(self.convert_to_kg)

        data_cleaning_logger.info("Dropping any weights which are Nulls")
        # Drop any weights which are NaNs
        products_table = products_table.dropna(subset=["weight"])
        data_cleaning_logger.info("Dropped rows")
        data_cleaning_logger.info(f"Number of rows : len({products_table})")

        data_cleaning_logger.debug("Setting the index column to start from 1")
        # Set the index column to start from 1
        products_table.index = products_table.index + 1

        data_cleaning_logger.debug("Adding a new column product_key")
        # Add a new column product_key, which is a list of numbers ranging for the length of the dataframe
        products_table["product_key"] = products_table.index
        # Add a new column weight_class which will be populated by a sql script
        data_cleaning_logger.info("Adding a new column weight_class")
        products_table["weight_class"] = np.nan
        data_cleaning_logger.info("Dropping Unamaed : 0 column")
        # Drop the "Unamed:  0" column within the dataframe
        products_table = products_table.drop("Unnamed: 0", axis=1)
        data_cleaning_logger.info(f"Number of columns : {len(products_table)}")

        data_cleaning_logger.info("Displaying the names of the columns")
        # stating the names of the columns
        products_table.columns = [
            "product_name",
            "product_price",
            "weight",
            "category",
            "ean",
            "date_added",
            "uuid",
            "availability",
            "product_code",
            "product_key",
            "weight_class",
        ]
        data_cleaning_logger.info(products_table.columns)

        data_cleaning_logger.info("Rearranging the order of the columns")
        # Rearrange the order of the columns
        column_order = [
            "product_key",
            "ean",
            "product_name",
            "product_price",
            "weight",
            "weight_class",
            "category",
            "date_added",
            "uuid",
            "availability",
            "product_code",
        ]
        
        # Set the new products_table to the name of the column order
        products_table = products_table[column_order]
        data_cleaning_logger.info("New column order")
        data_cleaning_logger.info(products_table.columns)

        data_cleaning_logger.info("Job clean_product_table has completed successfully")
        print("Job clean_product_table has completed successfully")
        return products_table

    def clean_currency_table(self, currency_table : pd.DataFrame):
        '''
        Method to read in currency data from a .json file, clean the data using Pandas 
        and upload the table to the datastore 

        Parameters: 
        source_file_name : str 
        The file pathway to the file containing the source data. 

        datastore_table_name : str 
        The name of the table uploaded to the datastore. 

        Returns: 
        currency_datastore_table : pd.DataFrame 
        A dataframe representing the cleaned data produced from this method
        '''
        data_cleaning_logger.info("Starting Job clean_currency_table")


        data_cleaning_logger.debug(f"Number of rows : {len(currency_table)}")

        # Reset the index of the dataframe
        data_cleaning_logger.info("Resetting the index of the dataframe")
        currency_table.reset_index(inplace=True)

        # Setting the names of the columns
        data_cleaning_logger.info("Setting the names of the columns")

        currency_table.columns = [
            "country_code",
            "country_name",
            "currency_code",
            "currency_symbol",
        ]
        data_cleaning_logger.debug(currency_table.columns)

        # Adding a new column currency_key to start from 1 onwards
        currency_table["currency_key"] = currency_table.index + 1

        # Adding a new column of nulls for the currency_conversion_key
        currency_table["currency_conversion_key"] = np.nan
        # Rearranging the column order
        data_cleaning_logger.info("Rearranging column order")
        column_order = [
            "currency_key",
            "currency_conversion_key",
            "currency_code",
            "country_code",
            "country_name",
            "currency_symbol",
        ]
        data_cleaning_logger.debug(column_order)
        currency_table = currency_table[column_order]
        data_cleaning_logger.info(currency_table.columns)

        data_cleaning_logger.info(
            "Job clean_currency_table has completed successfully."
        )
        print("job clean_currency_table has been completed successfully")
        return currency_table

    #TODO: There is something wrong with this method as the percentage_change column goes to null when it's uploaded. 
    def clean_currency_exchange_rates(
        self,
        raw_currency_data : pd.DataFrame,
        currency_mapping_document_name: str
    ):
        '''
        Method to extract currency conversion data from a website, 
        clean it, and upload it to the target datastore. 

        Parameters
        raw_currency_data : pd.DataFrame 
        The data from the raw_currency_data table 

        currency_mapping_document_name : str 
        The name of the currency_mapping document 

        Returns: 
        cleaned_currency_conversion_df : pd.DataFrame 
        A DataFrame containing the cleaned currency conversion data
        '''

        data_cleaning_logger.info("Starting job clean_currency_exchange_rates")

        data_cleaning_logger.debug(f"Number of rows : {len(raw_currency_data)}")

        # Dropping duplicates
        data_cleaning_logger.info("Dropping duplicates from table")
        no_duplicates_dataframe = raw_currency_data.drop_duplicates()

        data_cleaning_logger.debug(f"Number of rows : {len(no_duplicates_dataframe)}")

        # Getting the datetime using utcnow() 
        current_utc_time = datetime.now() 

        # Specify the UTC timezone
        utc_timezone = pytz.utc

        # Add timezone information to the datetime object
        current_utc_time_with_timezone = utc_timezone.localize(current_utc_time)

        # Format the datetime object as a string with timezone
        formatted_time_with_timezone = current_utc_time_with_timezone.strftime("%Y-%m-%d %H:%M:%S")

        print(formatted_time_with_timezone)
        data_cleaning_logger.debug(formatted_time_with_timezone)

        data_cleaning_logger.info("Creating new column with datetime object")
        # Create a new column called last_updated using the datetime object, then fill all nulls with the datetime
        df_with_datetime = no_duplicates_dataframe.assign(
            last_updated=pd.Series([formatted_time_with_timezone] * len(no_duplicates_dataframe))
        ).fillna(formatted_time_with_timezone)

        data_cleaning_logger.debug(df_with_datetime.columns)
        data_cleaning_logger.debug(
            f"Number of columns : {len(df_with_datetime.columns)}"
        )

        # Applying currency mapping to dataframe
        data_cleaning_logger.info("Appying currency codes to table")
        currency_mapping = self.convert_text_file_to_dict(
            currency_mapping_document_name
        )
        data_cleaning_logger.debug(currency_mapping)

        print(df_with_datetime.columns)
        try:
            df_with_datetime.columns = ["currency_name", "conversion_rate", "conversion_rate_percentage", "last_updated"]
        except ValueError:
            df_with_datetime.columns = ['Unnamed: 0', "currency_name", "conversion_rate", "conversion_rate_percentage", "last_updated"]


        print(df_with_datetime.columns)
        # Mapping currency code dictionary to the currency names within the dataframe
        df_with_datetime["currency_code"] = df_with_datetime["currency_name"].map(
            currency_mapping
        )
        data_cleaning_logger.debug(df_with_datetime.columns)
        data_cleaning_logger.debug(
            f"Number of columns : {len(df_with_datetime.columns)}"
        )

        # Adding new row to dataframe
        new_row = {
            "currency_name": "British Pound",
            "currency_code": "GBP",
            "conversion_rate": "1.000000",
            "conversion_rate_percentage": "1.000000",
            "percentage_change": np.nan,
            "last_updated": f"{formatted_time_with_timezone}"
            
        }

        data_cleaning_logger.info(f"Adding new row {new_row}")

        # Reading in the dataframe from a dictionary and tranposing it
        uk_row_df = pd.DataFrame.from_dict(new_row, orient="index").T

        data_cleaning_logger.info("New dataframe created")

        # Dropping the Unamed: 0 column if it is present 
        try:
            df_with_datetime.drop('Unnamed: 0', axis=1, inplace=True)
        except:
            print("WARNING: No Unamed : 0 found in axis")

        data_cleaning_logger.debug(uk_row_df)
        updated_df = pd.concat([uk_row_df, df_with_datetime]).reset_index(drop=True)

        # Adding 1 to index column to start from 1 instead of 0
        data_cleaning_logger.debug("Adding 1 to the index column to start from 1")
        updated_df.index = updated_df.index + 1
        data_cleaning_logger.info("Index updated")

        # Adding currency_conversion_key column to match the new index
        data_cleaning_logger.info(
            "Adding currency_conversion_key column to match the new index"
        )
        updated_df["currency_conversion_key"] = updated_df.index

        column_order = [
            "currency_conversion_key",
            "currency_name",
            "currency_code",
            "conversion_rate",
            "conversion_rate_percentage",
            "last_updated",
        ]
        data_cleaning_logger.info("Updating column_order of dataframe")
        data_cleaning_logger.info(column_order)

        # Updating column order of the dataframe
        updated_df = updated_df[column_order]
        data_cleaning_logger.info("New column order implemented")
        data_cleaning_logger.debug(updated_df.columns)

        # # Adding new rows to cover for unknowns
        # data_cleaning_logger.info("Adding new rows to cover for unknowns")
        # new_rows_addition = self.add_new_rows(
        #     [
        #         {"currency_conversion_key": -1, "currency_name": "Not Applicable"},
        #         {"currency_conversion_key": 0, "currency_name": "Unknown"},
        #     ]
        # )

        # cleaned_currency_conversion_df = pd.concat([new_rows_addition, updated_df])
        # data_cleaning_logger.info("Successfully concatentated rows from together")


        data_cleaning_logger.info(
            "Job clean_currency_exchange_rates completed successfully"
        )
        print("Job clean_currency_exchange rates has been completed successfully")
        return updated_df

    @staticmethod
    def add_new_rows(rows_to_add: list):
        new_rows = rows_to_add
        new_rows_df = pd.DataFrame(new_rows)
        return new_rows_df

    def convert_to_kg(self, weight):
        """
        Utilty method to standardise weights into kg

        Parameters:
        weight
        The numerical weight of the object.

        Returns:
        clean_weight: float
        The weight of the object in kg.

        None if the value is not a weight.

        """
        # Check if the weight is a float
        if isinstance(weight, float):
            # Return the original float value if weight is already a float
            return weight

        # Cast the weight value to a string
        clean_weight = str(weight)

        # Check if the weight has 'kg' in it.
        if "kg" in weight:
            data_cleaning_logger.debug("'kg' in weight stripping the kg suffix")
            # If it does, strip the 'kg' from it, and return the value as a float
            return float(clean_weight.strip("kg"))

        # Else if, the weight value is a multipack e.g. 12 x 100g
        elif " x " in weight:
            data_cleaning_logger.debug("Multipack weight detected.")
            # Split the value and unit of the weight via the use of a tuple
            value, unit = clean_weight.split(" x ")

            # Multiply the first number, and the first number of the unit together
            combined_value = float(value) * float(unit[:-1])
            data_cleaning_logger.debug("Value converted to kg value")
            return combined_value

        # Else if there is a 'g' or an 'ml' in the weight value, treat it like it is kgs.
        elif "g" in clean_weight or "ml" in clean_weight:
            try:
                data_cleaning_logger.debug("'g' or 'ml' in weight value")
                # Try to replace the . with an empty string
                clean_weight = clean_weight.replace(".", " ")

                # Next, remove the 'g' and the 'ml' from the string
                clean_weight = clean_weight.strip("g").strip("ml")

                data_cleaning_logger.debug(
                    "Weight cleaned. Dividing result by 1000 for kg value"
                )
                # Lastly, convert the value to a float and divide it by 1000
                return float(clean_weight) / 1000

            # If the value does not fit the desired format then
            except:
                data_cleaning_logger.warning(
                    "Irregular formatting found. Attempting to convert to kg"
                )
                # strip the '.' , get rid of all empty spaces, then strip the 'g' or 'ml' from the string.
                modified_weight = (
                    clean_weight.strip(".").replace(" ", "").strip("g").strip("ml")
                )
                # Lastly divide the weight by 1000 and return it as a float
                data_cleaning_logger.debug("Weight converted to kg value")
                return float(modified_weight) / 1000
        # Else if the weight is listed as 'oz'
        elif "oz" in clean_weight:
            data_cleaning_logger.debug("'oz' detected in weight value")
            # strip the 'oz' from the string, then
            clean_weight = clean_weight.strip("oz")
            # Divide the weight by the conversion factor of oz to kg.
            # Round the answer to 2 decimal places.
            data_cleaning_logger.debug("Weight value converted to kg")
            return round(float(clean_weight) * 0.028349523, 2)
        # Else if the value does not fit any condition, return None
        else:
            data_cleaning_logger.warning("Unknown value, returning None")
            return None

    def convert_text_file_to_dict(self, text_file_name):
        mapping_dictionary = {}
        with open(f"{text_file_name}.txt", "r") as file:
            for line in file:
                key, value = line.strip().split(",")
                mapping_dictionary[key] = value
        data_cleaning_logger.debug(f"Using mapping dictionary {mapping_dictionary}")

        return mapping_dictionary

    def clean_dates(self, date):
        """
        Utility method to clean dates extracted from the data sources

        Parameters:
        date : a date object

        Returns:
        pd.NaT : Not a datetime if the value is null

        pd.to-dateime(date) : The date object in a datetime format.
        """
        if date == "NULL":
            # Convert 'NULL' to NaT (Not a Time) for missing values
            data_cleaning_logger.warning(
                "Date field is NULL. Converting to NaT (Not a Datetime)"
            )
            return pd.NaT
        elif re.match(r"\d{4}-\d{2}-\d{2}", date):
            # Already in the correct format, convert to datetime
            return pd.to_datetime(date)
        elif re.match(r"\d{4}/\d{1,2}/\d{1,2}", date):
            # Convert from 'YYYY/MM/DD' format to datetime
            data_cleaning_logger.debug(
                f"{date} with Date Format YYYY/MM/DD converted to datetime object"
            )
            return pd.to_datetime(date, format="%Y/%m/%d")
        elif re.match(r"\d{4} [a-zA-Z]{3,} \d{2}", date):
            # Convert from 'YYYY Month DD' format to datetime
            data_cleaning_logger.debug(
                f"{date} with Date Format YYYY Month DD converted to datetime object"
            )
            return pd.to_datetime(date, format="%Y %B %d")
        else:
            # Try to convert with generic parsing, ignoring errors
            data_cleaning_logger.warning(
                f"Date format unknown. Attempting to convert to datetime"
            )
            return pd.to_datetime(date, errors="coerce")


