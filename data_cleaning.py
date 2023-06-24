from data_extraction import DatabaseExtractor
from database_utils import DatabaseConnector
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
import pandas as pd 
import re 
import yaml
import os 
import logging

log_filename = "logs/data_cleaning.log"
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

    def __init__(self, datastore_config_file_name):
        # Instantitating an instance of the DatabaseExtractor Class
        self.extractor = DatabaseExtractor()
        # Instantitating an instance of the DatabaseConnector Class
        self.uploader = DatabaseConnector()
        data_cleaning_logger.info(f"Parsing datastore_config_file : {datastore_config_file_name}")
        with open(datastore_config_file_name) as file:
            creds = yaml.safe_load(file)
            DATABASE_TYPE = creds['DATABASE_TYPE']
            DBAPI = creds['DBAPI']
            RDS_USER = creds['RDS_USER']
            RDS_PASSWORD = creds['RDS_PASSWORD']
            RDS_HOST = creds['RDS_HOST']
            RDS_PORT = creds['RDS_PORT']
            DATABASE = creds['RDS_DATABASE']

        try:
            data_cleaning_logger.info("Attempting to connect to datastore")  
            self.engine = create_engine(
                 f"{DATABASE_TYPE}+{DBAPI}://{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}:{RDS_PORT}/{DATABASE}"
            )
            data_cleaning_logger.info("Connection successful") 
            print("connection successful")
        except OperationalError:
            data_cleaning_logger.exception("Error connecting to the database")
            print("Error connecting to database")
            raise Exception    

    def clean_user_data(self, source_table_name : str, source_database_config_file_name : str, datastore_table_name : str):
        '''
        Method to clean the user data table 

        Parameters: 
        source_table_name : str 
        The table name in the source database 

        source_database_config_file_name : str 
        The name of the config file used to connect to the source database 

        datastore_table_name : str 
        The name of the table which will be uploaded to the datastore

        Returns: 
        legacy_users_dataframe: Dataframe 
        Pandas dataframe used to upload to the database. 

        '''
        data_cleaning_logger.info("Starting job clean_user_data")
        data_cleaning_logger.info(f"Attempting to clean {source_table_name}")
        data_cleaning_logger.info("Reading in table from source database")
        # Reading in the table from the AWS database 
        legacy_users_dataframe = self.extractor.read_rds_table(source_table_name, source_database_config_file_name)
        
        data_cleaning_logger.debug(f"Number of rows : {len(legacy_users_dataframe)}")

        # Naming the columns in the dataframe 
        legacy_users_dataframe.columns = [
                'user_key', 
                'first_name', 
                'last_name', 
                'birth_date', 
                'company', 
                'e-mail_address', 
                'address', 
                'country', 
                'country_index',
                'phone_number', 
                'join_date', 
                'user_uuid'
            ]
        data_cleaning_logger.info(f"Naming the table columns {legacy_users_dataframe.columns}")

        data_cleaning_logger.info("Cleaning date columns : join_date and birth_date")
        data_cleaning_logger.info("Attempting to clean dates into correct format")
        # Apply the clean_dates function to the dataframe  
        legacy_users_dataframe["join_date"] = legacy_users_dataframe["join_date"].apply(self.clean_dates)
        legacy_users_dataframe["birth_date"] = legacy_users_dataframe["birth_date"].apply(self.clean_dates)
        
        # legacy_users_dataframe["join_date"] = pd.to_datetime(legacy_users_dataframe['join_date'], errors='coerce')
        # legacy_users_dataframe["birth_date"] = pd.to_datetime(legacy_users_dataframe['birth_date'], errors='coerce')
        
        data_cleaning_logger.info("Setting columns as the appropriate datatypes")
        # Renaming the columns as appropriate data types 
        legacy_users_dataframe = legacy_users_dataframe.astype(
            {
                "first_name": "string",
                "last_name": "string",
                "company": "string",
                "e-mail_address": "string",
                "address": "string", 
                "country": "string",
                "country_index": "string", 
                "phone_number": "string"
            }
        )
        data_cleaning_logger.debug("Dictionary of datatypes changed")
        data_cleaning_logger.debug(
            
             {
                "first_name": "string",
                "last_name": "string",
                "company": "string",
                "e-mail_address": "string",
                "address": "string", 
                "country": "string",
                "country_index": "string", 
                "phone_number": "string"
            }
        )
        data_cleaning_logger.info("Dropping rows within birth_date and join_date columns which have nulls")
        # Drop all columns with nulls in their dates from the birth and join date columns 
        legacy_users_dataframe = legacy_users_dataframe.dropna(subset=['birth_date', 'join_date'])
        data_cleaning_logger.debug(f"Number of rows : {len(legacy_users_dataframe)}")

        data_cleaning_logger.info("resetting the index of the Dataframe")
        # Reset the index if desired
        legacy_users_dataframe = legacy_users_dataframe.reset_index(drop=True)

        data_cleaning_logger.debug("Setting the index column of the table to 1")
        legacy_users_dataframe.index = legacy_users_dataframe.index + 1

        data_cleaning_logger.info("Setting the value of the user_key to the index column")
        legacy_users_dataframe['user_key'] = legacy_users_dataframe.index 

        data_cleaning_logger.info("Creating a new dataframe to handle unknowns")

        new_rows_addition = self.add_new_rows(
            [
                {
                    "user_key": -1,
                    "first_name": "Not Applicable",
                    "last_name": "Not Applicable"
                },
                {
                    "user_key": 0, 
                    "first_name": "Unknown",
                    "last_name": "Unknown"
                }
            ]
        )
        data_cleaning_logger.debug(new_rows_addition)

        data_cleaning_logger.info("Appending the new_rows to the beginning of the dataframe")

        legacy_users_dataframe = pd.concat([new_rows_addition, legacy_users_dataframe]).reset_index(drop=True)
        data_cleaning_logger.info("Appended new rows to the beginning of the dataframe")
        data_cleaning_logger.debug(f"Number of rows : {len(legacy_users_dataframe)}")

        
        # Upload the dataframe to the datastore  
        legacy_users_database_table = self._upload_to_database(
                                    legacy_users_dataframe, 
                                    self.engine, 
                                    datastore_table_name
                                )
        data_cleaning_logger.info("Job clean_user_data completed successfully")

        return legacy_users_database_table
        
    
    def clean_store_data(self, source_table_name : str , source_database_config_file_name : str, datastore_table_name : str):
        '''
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

        legacy_store_dataframe : DataFrame 
        A cleaned version of the source data as a Pandas DataFrame 

        '''
        data_cleaning_logger.info("Starting Job clean_store_data")
        data_cleaning_logger.info("Reading in table from the source database")
        # Reading in the table from the AWS database 
        legacy_store_dataframe = self.extractor.read_rds_table(source_table_name, source_database_config_file_name)
        data_cleaning_logger.info("Database table successfully read.")
        data_cleaning_logger.info(f"Number of rows : {len(legacy_store_dataframe)}")

        data_cleaning_logger.info("Stating column names for the table")
        # State the column names for the table 
        legacy_store_dataframe.columns = [
            'store_key',
            'store_address', 
            'longitude', 
            'null_column', 
            'city', 
            'store_code', 
            'number_of_staff', 
            'opening_date', 
            'store_type', 
            'latitude',
            'country_code', 
            'region'
        
        ]
        
        data_cleaning_logger.debug("Column names")
        data_cleaning_logger.debug(legacy_store_dataframe.columns)

        data_cleaning_logger.info("Dropping null_column within the table")
        # Drop the null_column column within the table 
        legacy_store_dataframe = legacy_store_dataframe.drop('null_column', axis=1)
        data_cleaning_logger.info("null_column dropped")
        data_cleaning_logger.info(f"Number of rows : {len(legacy_store_dataframe)}")
        
        data_cleaning_logger.info("Reordering the columns")
        # Reorder the columns 
        column_order = [
            'store_key',
            'store_address',
            'longitude',
            'latitude',
            'city',
            'store_code',
            'number_of_staff',
            'opening_date',
            'store_type',
            'country_code',
            'region'
        ]
        data_cleaning_logger.info(column_order)
        # remake the dataframe with the column order in place 
        legacy_store_dataframe = legacy_store_dataframe[column_order]
        data_cleaning_logger.info("Dataframe remade in column order")
        data_cleaning_logger.debug(legacy_store_dataframe.columns)

        data_cleaning_logger.info("Applying the clean_date method to the opening_date column")
        # Change the opening_date column to a datetime 
        legacy_store_dataframe["opening_date"] = legacy_store_dataframe['opening_date'].apply(self.clean_dates)

        data_cleaning_logger.info("Dropping dates from the opening_date column which are null")
        # Drop dates in the opening_date which are null 
        legacy_store_dataframe = legacy_store_dataframe.dropna(subset=['opening_date'])
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

        data_cleaning_logger.debug("Setting the store_key to the same as the index column")
        legacy_store_dataframe['store_key'] = legacy_store_dataframe.index

        data_cleaning_logger.info("Replacing the incorrectly spelleed regions with the correct spelling")

        data_cleaning_logger.info("Unique values of column")
        data_cleaning_logger.info(legacy_store_dataframe["region"].unique())
        # Replace the Region with the correct spelling 
        legacy_store_dataframe = legacy_store_dataframe.replace('eeEurope', 'Europe')
        legacy_store_dataframe = legacy_store_dataframe.replace('eeAmerica', 'America')

        data_cleaning_logger.info("Unique values of column")
        data_cleaning_logger.info(legacy_store_dataframe["region"].unique())

        data_cleaning_logger.info("Converting longitude column to a numeric value")
        # Clean the longitude column by converting it to a numeric value 
        legacy_store_dataframe["longitude"] = pd.to_numeric(legacy_store_dataframe["longitude"], errors='coerce')

        data_cleaning_logger.info("Replacing values in the longitude column to the correct values")
        legacy_store_dataframe["number_of_staff"] = legacy_store_dataframe["number_of_staff"].replace({'3n9': '39', 'A97': '97', '80R': '80', 'J78': '78', '30e': '30'})
        
        data_cleaning_logger.info("Affected values")
        data_cleaning_logger.info({'3n9': '39', 'A97': '97', '80R': '80', 'J78': '78', '30e': '30'})

        data_cleaning_logger.info("Adding new rows to cover for unknown values")

        new_rows_addition = self.add_new_rows([
            {
            "store_key": -1,
            "store_address": "Not Applicable"
            }, 
            {
            "store_key": 0,
            "store_address": "Unknown"
            }

        ])
        data_cleaning_logger.info("New rows addded")
        data_cleaning_logger.info("Concatenating new rows with store_dataframe")

        legacy_store_dataframe = pd.concat([new_rows_addition, legacy_store_dataframe]).reset_index(drop=True)

        data_cleaning_logger.debug(f"Number of Rows : {len(legacy_store_dataframe)}")

        data_cleaning_logger.info("Uploading table to the datastore")
        legacy_store_database_table = self._upload_to_database(
                                            legacy_store_dataframe, 
                                            self.engine, 
                                            datastore_table_name
                                            )
        data_cleaning_logger.info("Table uploaded to datastore")
        data_cleaning_logger.info("Job clean_store_data has completed succesfully")
        return legacy_store_database_table
    
        
        
    def clean_card_details(self, link_to_pdf : str, datastore_table_name : str):
        '''
        Method to clean a pdf file with card details and upload it to a datastore 

        Parameters: 

        link_to_pdf : str 
        The link to the pdf file 

        datastore_table_name : str 
        The name of the table to be uploaded to the datastore 

        Returns 

        card_details_table 
        A dataframe consisting of card_details read from the pdf 

        '''
        data_cleaning_logger.info("Starting Job clean_card_details")
        data_cleaning_logger.info(f"Attempting to read PDF table from {link_to_pdf}")
        # Read in the pdf data for the card details 
        card_details_table = self.extractor.retrieve_pdf_data(link_to_pdf)

        data_cleaning_logger.info(f"Succesfully read table from {link_to_pdf}")
        data_cleaning_logger.info(f"Number of rows : {len(card_details_table)}")
        data_cleaning_logger.info("Removing non-numeric characters from the card_number column")

        # removing non-numeric characters from the card_number column 
        card_details_table['card_number'] = card_details_table['card_number'].apply(lambda x: re.sub(r'\D', '', str(x)))

        data_cleaning_logger.info("List of unique card_providers")
        data_cleaning_logger.info(card_details_table["card_provider"].unique())
        # Define the items to remove
        items_to_remove = [
                        'NULL', 'NB71VBAHJE', 'WJVMUO4QX6', 'JRPRLPIBZ2', 'TS8A81WFXV',
                        'JCQMU8FN85', '5CJH7ABGDR', 'DE488ORDXY', 'OGJTXI6X1H',
                        '1M38DYQTZV', 'DLWF2HANZF', 'XGZBYBYGUW', 'UA07L7EILH',
                        'BU9U947ZGV', '5MFWFBZRM9'
                        ]
        data_cleaning_logger.info("Filtering out the last 15 entries of the card_provider column")
        data_cleaning_logger.info(items_to_remove)

        # Filter out the last 15 entries from the card provider column 
        card_details_table = card_details_table[~card_details_table['card_provider'].isin(items_to_remove)]
        data_cleaning_logger.info("Card_details filtered")
        data_cleaning_logger.info(f"Number of rows : {len(card_details_table)}")

        data_cleaning_logger.info("Applying the clean_dates method to to the dataframe to clean the date values")
        # Apply the clean_dates method to the dataframe to clean the date values
        card_details_table["date_payment_confirmed"] = card_details_table['date_payment_confirmed'].apply(self.clean_dates)

        data_cleaning_logger.info("Converting date_payment_confirmed column to a datetime")
        # Convert the date_payment_confirmed column into a datetime 
        card_details_table["date_payment_confirmed"] = pd.to_datetime(card_details_table['date_payment_confirmed'], errors='coerce')

        data_cleaning_logger.info("For any null values, drop them from the table")
        # For any null values, drop them. 
        card_details_table = card_details_table.dropna(subset=['date_payment_confirmed'])
        data_cleaning_logger.info("Rows dropped")
        data_cleaning_logger.info(f"Number of rows : {len(card_details_table)}")
        
        data_cleaning_logger.debug("Setting the dataframe index to start at 1 instead of 0")
        # Set the index of the dataframe to start at 1 instead of 0 
        card_details_table.index = card_details_table.index + 1
        # Add a new column called card_key, which is the length of the index column of the card_details table
        data_cleaning_logger.debug("Setting the card_key to the same as the index column")
        card_details_table['card_key'] = card_details_table.index

        data_cleaning_logger.info("Rearranging the order of the columns")
        # Rearrange the order of the columns 
        column_order = [
            'card_key',
            'card_number',
            'expiry_date',
            'card_provider',
            'date_payment_confirmed'
        ]

        data_cleaning_logger.info(column_order)
        # Set the order of the columns in the table 
        card_details_table = card_details_table[column_order]

        data_cleaning_logger.info("New column order")
        data_cleaning_logger.info(card_details_table.columns)

        data_cleaning_logger.info("Resetting the index of the table")
        # Reset the index of the table to match the indexes to the card_keys 
        card_details_table = card_details_table.reset_index(drop=True)

        data_cleaning_logger.info("Adding new rows to the table to cover for unknowns")
        # Add new rows to the table 
        new_rows_additions = self.add_new_rows(
            [
                {
                    "card_key": -1,
                    "card_number": "Not Applicable"
                },
                {
                    "card_key": 0,
                    "card_number": "Unknown"
                }
            ]
        )
        data_cleaning_logger.info("New rows added")
        data_cleaning_logger.info(new_rows_additions)

        data_cleaning_logger.info("Concatentating the two dataframes together to add the new rows to the top")
        # Concatentate the two dataframes together
        card_details_table = pd.concat([new_rows_additions, card_details_table]).reset_index(drop=True)
        data_cleaning_logger.info(f"Number of rows : {len(card_details_table)}")

        
        # Lastly, try to upload the table to the database. 
        card_details_database_table = self._upload_to_database(
                                            card_details_table,
                                            self.engine,
                                            datastore_table_name
                                        )
        data_cleaning_logger.info("Table uploaded")
        data_cleaning_logger.info("Job clean_card_details has completed successfully.")
        return card_details_database_table


    def clean_orders_table(self, source_table_name : str, source_database_config_file_name : str, datastore_table_name : str):
        
        data_cleaning_logger.info("Starting job clean_orders_table")
        data_cleaning_logger.info("Reading in the table from the source database")

        # Read in the table from the RDS database 
        orders_dataframe = self.extractor.read_rds_table(source_table_name, source_database_config_file_name)
        data_cleaning_logger.info(f"Successfully read the table {source_table_name} from the source database")
        data_cleaning_logger.info(f"Number of rows : {len(orders_dataframe)}")

        data_cleaning_logger.info("Stating the name of the columns")
        # State the names of the columns 
        orders_dataframe.columns = [
            'order_key',
            'null_key',
            'date_uuid',
            'first_name',
            'last_name',
            'user_uuid',
            'card_number',
            'store_code',
            'product_code',
            'null_column',
            'product_quantity'
            
        ]
        data_cleaning_logger.info("Names of columns")
        data_cleaning_logger.info(orders_dataframe.columns)

        data_cleaning_logger.info("Dropping columns from the dataframe")
        data_cleaning_logger.debug("Columns to drop")
        data_cleaning_logger.debug(["null_key", "first_name", "last_name", "null_column"])
        # Drop the following columns within the dataframe if they exist 
        orders_dataframe.drop(["null_key", "first_name", "last_name", "null_column"], axis=1, inplace=True)

        data_cleaning_logger.debug("Columns dropped")
        data_cleaning_logger.debug(orders_dataframe.columns)
        data_cleaning_logger.info(f"Number of rows : {len(orders_dataframe)}")

       
        # Lastly, try to upload the cleaned table to the database 
        orders_datatable = self._upload_to_database(
                                    orders_dataframe,
                                    self.engine,
                                    datastore_table_name
                                )
        data_cleaning_logger.info("Table uploaded")
        data_cleaning_logger.info("Job clean_orders_table has completed successfully.")
        return orders_datatable 

    def clean_time_event_table(self, s3_bucket_url : str , datastore_table_name : str):
        '''
        Method to read in a time_dimension table from an AWS S3 Bucket, 
        clean it, and upload it to the datastore.

        Parameters: 
        bucket_url : str 
        A link to the s3 bucket hosted on AWS 

        datastore_table_name : str 
        The name of the table to be uploaded to the datastore 
        '''
        data_cleaning_logger.info("Starting job clean_time_event_table")
        data_cleaning_logger.info("Reading file from s3_bucket into dataframe")
        # Read in the json data from the s3 bucket 
        time_df = self.extractor.read_json_from_s3(s3_bucket_url)

        data_cleaning_logger.info(f"Successfully read file from {s3_bucket_url}")
        data_cleaning_logger.info(f"Number of rows : {len(time_df)}")

        data_cleaning_logger.info("Filtering out non-numeric values from the 'month' column")
        # Filter out non-numeric values and convert the column to numeric using boolean mask 
        numeric_mask = time_df['month'].apply(pd.to_numeric, errors='coerce').notna()
        # copy the dataframe then convert the month column to a numeric datatype 
        time_df = time_df[numeric_mask].copy()
        time_df['month'] = pd.to_numeric(time_df['month'])
        # Afterwards, drop any values which are not null or not numeric 
        time_df = time_df.dropna(subset=['month'])

        data_cleaning_logger.info(f"Number of rows : {len(time_df)}")

        #TODO: Write a piece of code which verifies that the number of rows in the orders_table 
        # is equal to the number of rows in the dim_date_times table 
        # Currently, the operation drops 38 rows 
        # 120161 - 120123 = 38 
        # Correct number because the orders table has 120123 rows, so we have a time event per order.
        data_cleaning_logger.debug(f"Setting the index to start at 1")
        time_df.index = time_df.index + 1 

        data_cleaning_logger.debug(f"Setting the time_key column to start at 1")
        time_df["time_key"] = time_df.index

        data_cleaning_logger.info("Resetting the index of the table")
        # Reset the index 
        time_df = time_df.reset_index(drop=True)

        data_cleaning_logger.info("Changing the column order of the table")
        # Lastly, change the column order
        column_order = [
            'time_key',
            'timestamp',
            'day',
            'month',
            'year',
            'time_period',
            'date_uuid'
        ]
        data_cleaning_logger.info(column_order)

        time_df = time_df[column_order]

        data_cleaning_logger.info("New column order")
        data_cleaning_logger.info(time_df.columns)

        data_cleaning_logger.info("Adding new rows to the table in case of unknowns")
        new_rows_addition = self.add_new_rows(
            [
                {
                    "time_key": -1,
                    "timestamp": "Not Applicable"
                }, 
                {
                    "time_key": 0,
                    "timestamp": "Unknown"
                }
            ]
        )
        data_cleaning_logger.info("New rows added")
        data_cleaning_logger.info(new_rows_addition)

        data_cleaning_logger.info("Concatenating new rows to the start of the dataframe")
        time_df = pd.concat([new_rows_addition, time_df]).reset_index(drop=True)

        
        # Try to upload the table to the database
        time_datastore_table = self._upload_to_database(
                                    time_df,
                                    self.engine,
                                    datastore_table_name
                                )
        data_cleaning_logger.info("Table uploaded")
        data_cleaning_logger.info("Job clean_time_event_table has completed successfully")
        return time_datastore_table

    def clean_product_table(self, s3_bucket_url : str, datastore_table_name : str):
        '''
        Method to read in a .csv file from an S3 Bucket on AWS, 
        CLean the data, and then upload it to the datastore 

        Parameters: 

        s3_bucket_url : str 
        The link to the s3_bucket 

        datastore_table_name : str
        The name of the table uploaded to the datastore

        Returns 
        products_table: 
        A dataframe containing the cleaned products_table 

        '''
        data_cleaning_logger.info("Starting job clean_product_table")
        data_cleaning_logger.info(f"Reading data from {s3_bucket_url}")
        # Set the dataframe to the output of the method 
        products_table = self.extractor.read_s3_bucket_to_dataframe(s3_bucket_url)

        data_cleaning_logger.info(f"Successfully read data from {s3_bucket_url}")
        data_cleaning_logger.info(f"Number of rows : {len(products_table)}")

        data_cleaning_logger.info("Creating a list of unique values from within the removed column")
        # Create a list of unique values within the 'removed' column  and print them out for debugging purposes 
        values = list(products_table["removed"].unique())
        print(values)

        data_cleaning_logger.info("Filtering out the last three values inside the list")
        # Filter out the last 3 values of the values inside the list using a boolean mask 
        products_table = products_table[~products_table['removed'].isin(values[-3:])]
        data_cleaning_logger.info(f"Number of rows : {len(products_table)}")

        data_cleaning_logger.info("Converting dates in the date_added column to a datetime")
        # Convert the values in the date_added column to a datetime turning any invalid dates to NaNs
        products_table["date_added"] = pd.to_datetime(products_table['date_added'], errors='coerce')

        data_cleaning_logger.info("Removing the £ sign in the product_price column")
        # Strip the £ sign from each of the prices within the product_price column 
        products_table['product_price'] = products_table['product_price'].str.replace('£', '')

        data_cleaning_logger.info("Applying the convert_to_kg method to the weight column")
        # Apply the convert_to_kg method to the 'weight' column to standardise the weights to kg
        products_table['weight'] = products_table['weight'].apply(self.convert_to_kg)

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
            "EAN",
            "date_added",
            "uuid",
            "availability",
            "product_code",
            "product_key"
        ]
        data_cleaning_logger.info(products_table.columns)

        data_cleaning_logger.info("Rearranging the order of the columns")
        # Rearrange the order of the columns 
        column_order = [
    
            "product_key",
            "EAN",
            "product_name", 
            "product_price",
            "weight",
            "category",
            "date_added",
            "uuid",
            "availability",
            "product_code"
            
        ]

        # Set the new products_table to the name of the column order 
        products_table = products_table[column_order]
        data_cleaning_logger.info("New column order")
        data_cleaning_logger.info(products_table.columns)


        data_cleaning_logger.info("Adding new rows to the table in case of unknowns")
        new_rows_addition = self.add_new_rows(
            [
                {
                    "product_key": -1,
                    "EAN": "Not Applicable"
                }, 
                {
                    "product_key": 0,
                    "EAN": "Unknown"
                }
            ]
        )

        data_cleaning_logger.info("Concatenating new rows to the start of the dataframe")
        products_table = pd.concat([new_rows_addition, products_table]).reset_index(drop=True)

        
        # Try to upload the table to the database. 
        products_datastore_table = self._upload_to_database(
                                            products_table,
                                            self.engine,
                                            datastore_table_name
                                        )
        data_cleaning_logger.info("Table uploaded")
        data_cleaning_logger.info("Job clean_time_event_table has completed successfully")
        return products_datastore_table

    def _upload_to_database(self, dataframe : pd.DataFrame, database_engine, datastore_table_name : str):
        '''
        Method to upload the completed dataframe to the datastore 
        Method uses the upload_to_db method in the DatabaseConnector class to upload the table to the database 

        Parameters: 
        dataframe : pd.DataFrame 
        A pandas dataframe 

        database_engine 
        The database_engine that the user wants to use. Defined inside the __init__ method of the DataCleaning class 

        datastore_table_name : str 
        The name of the table uploaded to the datastore 

        Returns: 
        dataframe 
        A pandas dataframe 
        '''
        try:
            self.uploader.upload_to_db(dataframe, database_engine, datastore_table_name)
            print(f"Table uploaded")
            return dataframe
        except: 
            print("Error uploading table to database")
            raise Exception
    
         
    @staticmethod 
    def add_new_rows(rows_to_add : list):
        new_rows = rows_to_add 
        new_rows_df = pd.DataFrame(new_rows)
        return new_rows_df 
    
    def convert_to_kg(self, weight):
        '''
        Utilty method to standardise weights into kg

        Parameters: 
        weight 
        The numerical weight of the object. 

        Returns: 
        clean_weight: float 
        The weight of the object in kg. 

        None if the value is not a weight. 

        '''
        # Check if the weight is a float
        if isinstance(weight, float):
            # Return the original float value if weight is already a float
            return weight  
        
        # Cast the weight value to a string
        clean_weight = str(weight)
        
        # Check if the weight has 'kg' in it. 
        if 'kg' in weight:
            data_cleaning_logger.debug("'kg' in weight stripping the kg suffix")
            # If it does, strip the 'kg' from it, and return the value as a float 
            return float(clean_weight.strip('kg'))
        
        # Else if, the weight value is a multipack e.g. 12 x 100g 
        elif ' x ' in weight:
            data_cleaning_logger.debug("Multipack weight detected.")
            # Split the value and unit of the weight via the use of a tuple 
            value, unit = clean_weight.split(' x ')

            # Multiply the first number, and the first number of the unit together 
            combined_value = float(value) * float(unit[:-1])
            data_cleaning_logger.debug("Value converted to kg value")
            return combined_value
        
        # Else if there is a 'g' or an 'ml' in the weight value, treat it like it is kgs. 
        elif 'g' in clean_weight or 'ml' in clean_weight:
            try:
                data_cleaning_logger.debug("'g' or 'ml' in weight value")
                # Try to replace the . with an empty string
                clean_weight = clean_weight.replace('.', " ")

                # Next, remove the 'g' and the 'ml' from the string 
                clean_weight = clean_weight.strip('g').strip('ml')
                
                data_cleaning_logger.debug("Weight cleaned. Dividing result by 1000 for kg value")
                # Lastly, convert the value to a float and divide it by 1000
                return float(clean_weight) / 1000
            
            # If the value does not fit the desired format then
            except:
                data_cleaning_logger.warning("Irregular formatting found. Attempting to convert to kg")
                # strip the '.' , get rid of all empty spaces, then strip the 'g' or 'ml' from the string. 
                modified_weight = clean_weight.strip('.').replace(" ", "").strip('g').strip('ml')
                # Lastly divide the weight by 1000 and return it as a float
                data_cleaning_logger.debug("Weight converted to kg value")
                return float(modified_weight) / 1000
        # Else if the weight is listed as 'oz' 
        elif 'oz' in clean_weight:
            data_cleaning_logger.debug("'oz' detected in weight value")
            # strip the 'oz' from the string, then 
            clean_weight = clean_weight.strip('oz')
            # Divide the weight by the conversion factor of oz to kg.
            # Round the answer to 2 decimal places.
            data_cleaning_logger.debug("Weight value converted to kg") 
            return round(float(clean_weight) * 0.028349523, 2)
        # Else if the value does not fit any condition, return None 
        else:
            data_cleaning_logger.warning("Unknown value, returning None")
            return None
        
    def clean_dates(self, date):
            '''
            Utility method to clean dates extracted from the data sources 

            Parameters: 
            date : a date object 

            Returns: 
            pd.NaT : Not a datetime if the value is null 
            
            pd.to-dateime(date) : The date object in a datetime format. 
            '''
            if date == 'NULL':
                # Convert 'NULL' to NaT (Not a Time) for missing values
                data_cleaning_logger.warning("Date field is NULL. Converting to NaT (Not a Datetime)")
                return pd.NaT  
            elif re.match(r'\d{4}-\d{2}-\d{2}', date):
                # Already in the correct format, convert to datetime
                return pd.to_datetime(date)  
            elif re.match(r'\d{4}/\d{1,2}/\d{1,2}', date):
                # Convert from 'YYYY/MM/DD' format to datetime
                data_cleaning_logger.debug(f"{date} with Date Format YYYY/MM/DD converted to datetime object")
                return pd.to_datetime(date, format='%Y/%m/%d')  
            elif re.match(r'\d{4} [a-zA-Z]{3,} \d{2}', date):
                # Convert from 'YYYY Month DD' format to datetime
                data_cleaning_logger.debug(f"{date} with Date Format YYYY Month DD converted to datetime object")
                return pd.to_datetime(date, format='%Y %B %d')  
            else:
                # Try to convert with generic parsing, ignoring errors
                data_cleaning_logger.warning(f"Date format unknown. Attempting to convert to datetime")
                return pd.to_datetime(date, errors='coerce')  

def perform_data_cleaning(target_datastore_config_file_name):
    cleaner = DataCleaning(target_datastore_config_file_name)
    cleaner.clean_user_data("legacy_users", 'db_creds.yaml', "dim_users")
    cleaner.clean_store_data("legacy_store_details", "db_creds.yaml", "dim_store_details")
    cleaner.clean_card_details(
          "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf",
          "dim_card_details"
      ) 
    cleaner.clean_orders_table("orders_table", "db_creds.yaml", "orders_table") 
    cleaner.clean_time_event_table(
        "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json",
        "dim_date_times"
    )
    cleaner.clean_product_table(
        "s3://data-handling-public/products.csv",
        "dim_product_details"
    ) 

if __name__=="__main__":
    perform_data_cleaning()
 
