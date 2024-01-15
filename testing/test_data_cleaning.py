import unittest 
import pandas as pd 
import json
from unittest.mock import patch 
from database_scripts.data_cleaning import DataCleaning 
from database_scripts.data_extraction import DatabaseExtractor
from database_scripts.database_utils import DatabaseConnector
from database_scripts.file_handler import get_absolute_file_path
from sqlalchemy import create_engine
from pandas import Timestamp 
import os 

class TestDataCleaning(unittest.TestCase):

    @classmethod
    def setUpClass(cls):

        cls.target_database_config_file_name = get_absolute_file_path('sales_data_creds_test.yaml', 'credentials') # 'sales_data_creds_test.yaml'
        cls.source_database_config_file_name = get_absolute_file_path('db_creds.yaml', 'credentials') # "db_creds.yaml"

        # Setting up database names 
        cls.source_database_name = 'postgres'
        cls.target_database_name = 'sales_data_test'


        # Setting up source table names 

        cls.source_user_data_table_name= "legacy_users"
        cls.source_store_detail_table_name = "legacy_store_details"
        cls.source_orders_table_name = "orders_table"


        # Setting up target_database_names 

        cls.target_land_users_table_name = "land_user_data"
        cls.target_land_store_data_table_name = "land_store_details"
        cls.target_land_card_details_table_name = "land_card_details"
        cls.target_land_time_details_table_name = "land_date_times"
        cls.target_land_product_details_table_name = "land_product_details"
        cls.target_land_currency_table_name = "land_currency"
        cls.target_land_currency_conversion_table_name = "land_currency_conversion"
        cls.target_orders_table_name = "orders_table"

        # Setting up links and file pathways to data sources 
        cls.pdf_link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
        cls.json_s3_link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"
        cls.csv_s3_link = "s3://data-handling-public/products.csv"
        cls.json_file_path = get_absolute_file_path('country_data.json', 'source_data_files') # r"source_data_files\country_data.json" 
        cls.currency_conversions_file_path = get_absolute_file_path('currency_conversions_test.csv', 'source_data_files')
        cls.currency_code_mapping_file = get_absolute_file_path('currency_code_mapping', 'source_data_files')

        # Variables for convert_to_kg tests 
        cls.list_of_weights = [    
       '0.08kg', '420g', '1.68kg', '0.718kg', '600ml', '0.504kg', '480g', '8 x 150g', '6oz', 
        ]
        cls.list_of_invalid_weights = ['110lbs', '10 Gallons', '10ms']

        # Variables for convert_to_date tests 
        cls.list_of_dates = ['2013-10-14','2010-07-17','2000-06-15','2017-01-20','2006 September 03']
    

        # Instantiating instances of classes 
        cls.test_data_connector = DatabaseConnector() 
        cls.test_data_extractor = DatabaseExtractor()
        cls.test_data_cleaner = DataCleaning() 

        cls.source_database_engine = cls.test_data_connector.initialise_database_connection(cls.source_database_config_file_name, True, cls.source_database_name)
        cls.target_database_engine = cls.test_data_connector.initialise_database_connection(cls.target_database_config_file_name , True, cls.target_database_name)

    def test_clean_user_data(self):

        # Extract the user_data then apply the cleaning method on it. 

        test_user_data_table = self.test_data_extractor.read_rds_table(self.source_user_data_table_name, self.source_database_engine)

        cleaned_user_table = self.test_data_cleaner.clean_user_data(self.source_database_engine, test_user_data_table, self.source_orders_table_name)

        # Compare the output with the land_table in the sales_data_test database. 

        print(self.target_land_users_table_name)
        print(self.target_database_engine)

        target_user_table = self.test_data_extractor.read_rds_table(self.target_land_users_table_name, self.target_database_engine)

        # No need to compare with the dimension tables yet as these tables are the same, but with extra rows at the top. 

        # Next run the following tests 

        # 1. If the cleaned_user_table is a pd.DataFrame object 
        print(f" The type of the variable {type(cleaned_user_table)}")
        # Next, test if the method returns a dataframe 
        self.assertIsInstance(cleaned_user_table, pd.DataFrame)

        # 2. Testing if the number of rows are the same 

        print(f"Number of rows in cleaned_table : {len(cleaned_user_table)}")
        print(f"Number of rows in land_users from Test Database : {len(target_user_table)}")

        # 3. Testing if the column names match up. 

        expected_columns = [
        'user_key', 'first_name', 'last_name', 'date_of_birth', 'company', 'email_address',
        'address', 'country', 'country_code', 'phone_number', 'join_date', 'user_uuid'
        ]

        # Print the test list of columns and the expected list of columns 
        print(f"The test list: {cleaned_user_table.columns.tolist()}")
        print(f"The expected list : {expected_columns}")

        self.assertEqual(cleaned_user_table.columns.tolist(), expected_columns)

    
    def test_clean_store_table(self):
        
        # Read in the raw_store_data 
        raw_store_data = self.test_data_extractor.read_rds_table(self.source_store_detail_table_name, self.source_database_engine)

        # Apply the cleaning method to the raw store data variable 

        cleaned_store_data = self.test_data_cleaner.clean_store_data(self.source_database_engine, raw_store_data, self.source_store_detail_table_name)


        # 1. If the cleaned_store_table is a pd.DataFrame object 
        print(f" The type of the variable {type(cleaned_store_data)}")
        # Next, test if the method returns a dataframe 
        self.assertIsInstance(cleaned_store_data, pd.DataFrame)

        # 2. Testing if the number of rows are the same 

        print(f"Number of rows in cleaned_table : {len(cleaned_store_data)}")
        print(f"Number of rows in land_users from Test Database : {len(cleaned_store_data)}")

        # 3. Testing if the column names match up. 
        expected_columns  = [
            'store_key', 'store_address','longitude','latitude','city',
            'store_code','number_of_staff','opening_date','store_type','country_code','region'
        ]
        # print a comparison between the test_list of columns and the expected list of columns
        print(f"The test list: {cleaned_store_data.columns.tolist()}")
        print(f"The expected list : {expected_columns}")

        # Assert if the two lists of columns are equal 
        self.assertEqual(cleaned_store_data.columns.tolist(), expected_columns)
 
        pass

    def test_clean_orders_table(self):
        # Reading in the orders data 
        raw_orders_data = self.test_data_extractor.read_rds_table(self.source_orders_table_name, self.source_database_engine)

        cleaned_orders_data = self.test_data_cleaner.clean_orders_table(self.source_database_engine, raw_orders_data, self.target_orders_table_name)

        # 1. If the cleaned_orders_table is a pd.DataFrame object 
        print(f" The type of the variable {type(cleaned_orders_data)}")
        # Next, test if the method returns a dataframe 
        self.assertIsInstance(cleaned_orders_data, pd.DataFrame)

        # 2. Testing if the number of rows are the same 

        print(f"Number of rows in cleaned_table : {len(cleaned_orders_data)}")
        print(f"Number of rows in land_users from Test Database : {len(cleaned_orders_data)}")

        # 3. Testing if the column names match up. 
        expected_columns  = ['order_key','date_uuid','user_uuid','card_key','date_key','product_key',
                            'store_key','user_key','currency_key','card_number','store_code','product_code',
                            'product_quantity', 'country_code']
        # print a comparison between the test_list of columns and the expected list of columns
        print(f"The test list: {cleaned_orders_data.columns.tolist()}")
        print(f"The expected list : {expected_columns}")

        # Assert if the two lists of columns are equal 
        self.assertEqual(cleaned_orders_data.columns.tolist(), expected_columns)
 
        pass

    def test_clean_card_details(self):
        # Extract the card details data
        raw_card_details_data = self.test_data_extractor.retrieve_pdf_data(self.pdf_link)

        # Apply the cleaning method to the raw_card_details data variable 
        cleaned_card_data = self.test_data_cleaner.clean_card_details(raw_card_details_data)

        # 1. If the cleaned_card_table is a pd.DataFrame object 
        print(f" The type of the variable {type(cleaned_card_data)}")
        # Next, test if the method returns a dataframe 
        self.assertIsInstance(cleaned_card_data, pd.DataFrame)

        # 2. Testing if the number of rows are the same 

        print(f"Number of rows in cleaned_table : {len(cleaned_card_data)}")
        print(f"Number of rows in land_users from Test Database : {len(cleaned_card_data)}")


    def test_clean_time_event_data(self):
        # Read in the raw time event data 
        raw_time_event_details = self.test_data_extractor.read_json_from_s3(self.json_s3_link)

        # Applying cleaning method to the raw time event details table 
        cleaned_time_event_data = self.test_data_cleaner.clean_time_event_table(raw_time_event_details)

        # 1. If the cleaned_time_event_table is a pd.DataFrame object 
        print(f" The type of the variable {type(cleaned_time_event_data)}")
        # Next, test if the method returns a dataframe 
        self.assertIsInstance(cleaned_time_event_data, pd.DataFrame)

        # 2. Testing if the number of rows are the same 

        print(f"Number of rows in cleaned_table : {len(cleaned_time_event_data)}")
        print(f"Number of rows in land_users from Test Database : {len(cleaned_time_event_data)}")

        # 3. Testing if the number of columns are the same. 

        expected_columns  = [
            'date_key','event_time','day', 'month', 'year', 'time_period','date_uuid'
        ]
        # print a comparison between the test_list of columns and the expected list of columns
        print(f"The test list: {cleaned_time_event_data.columns.tolist()}")
        print(f"The expected list : {expected_columns}")

        # Assert if the two lists of columns are equal 
        self.assertEqual(cleaned_time_event_data.columns.tolist(), expected_columns)


    def test_clean_product_data(self):
        # Reading in the raw product data
        raw_product_data = self.test_data_extractor.read_s3_bucket_to_dataframe(self.csv_s3_link)
        # Applying the cleaning method to the product data 
        cleaned_product_data = self.test_data_cleaner.clean_product_table(raw_product_data)

        # 1. If the cleaned_product_data table is a pd.DataFrame object 
        print(f" The type of the variable {type(cleaned_product_data)}")
        # Next, test if the method returns a dataframe 
        self.assertIsInstance(cleaned_product_data, pd.DataFrame)

        # 2. Testing if the number of rows are the same 

        print(f"Number of rows in cleaned_table : {len(cleaned_product_data)}")
        print(f"Number of rows in land_users from Test Database : {len(cleaned_product_data)}")

        # 3. Testing if the number of columns are the same. 

        expected_columns  = [
            "product_key", "ean", "product_name", "product_price", "weight",
            "weight_class", "category", "date_added", "uuid","availability","product_code"
        ]
        # print a comparison between the test_list of columns and the expected list of columns
        print(f"The test list: {cleaned_product_data.columns.tolist()}")
        print(f"The expected list : {expected_columns}")


    def test_clean_currency_data(self):
        # Reading in the raw product data
        raw_currency_data = self.test_data_extractor.read_s3_bucket_to_dataframe(self.csv_s3_link)
        # Applying the cleaning method to the product datat
        cleaned_currency_data = self.test_data_cleaner.clean_product_table(raw_currency_data)

        # 1. If the cleaned_product_data table is a pd.DataFrame object 
        print(f" The type of the variable {type(cleaned_currency_data)}")
        # Next, test if the method returns a dataframe 
        self.assertIsInstance(cleaned_currency_data, pd.DataFrame)

        # 2. Testing if the number of rows are the same 

        print(f"Number of rows in cleaned_table : {len(cleaned_currency_data)}")
        print(f"Number of rows in land_users from Test Database : {len(cleaned_currency_data)}")

        # 3. Testing if the number of columns are the same. 

        expected_columns  = ["currency_key", 'currency_conversion_key',"currency_code", "country_code", "country_name","currency_symbol"]
        # print a comparison between the test_list of columns and the expected list of columns
        print(f"The test list: {cleaned_currency_data.columns.tolist()}")
        print(f"The expected list : {expected_columns}")
        pass

    def test_clean_currency_conversion_data(self):
        #TODO: Could the data for the tests just be read in from source .csv files instead? 
        # Applying the cleaning method to the currency conversions .csv file 

        raw_currency_data = pd.read_csv(self.currency_conversions_file_path)
        # Applying the cleaning method to the dataframe 
        cleaned_currency_conversion_data = self.test_data_cleaner.clean_currency_exchange_rates(raw_currency_data, self.currency_code_mapping_file)

        # 1. If the cleaned_product_data table is a pd.DataFrame object 
        print(f" The type of the variable {type(cleaned_currency_conversion_data)}")
        # Next, test if the method returns a dataframe 
        self.assertIsInstance(cleaned_currency_conversion_data, pd.DataFrame)

        # 2. Testing if the number of rows are the same 

        print(f"Number of rows in cleaned_table : {len(cleaned_currency_conversion_data)}")
        print(f"Number of rows in land_users from Test Database : {len(cleaned_currency_conversion_data)}")



    def test_convert_to_kg_valid_weights(self):
        list_of_weights = self.list_of_weights

        test_list = []
        for item in list_of_weights:
            weight = self.test_data_cleaner.convert_to_kg(item)
            # Assert if each item passed in the list is a float 
            self.assertIsInstance(weight, float)
            test_list.append(weight)

        print(test_list)
        expected_output = [0.08, 0.42, 1.68, 0.718, 0.6, 0.504, 0.48, 1200.0, 0.17]
        # Asssert if the test_list is a list 
        self.assertIsInstance(test_list, list)
        # Assert if the hard-coded expected output matches the output of the test list
        self.assertEqual(test_list, expected_output)

    
    def test_convert_to_kg_invalid_weights(self):

        # Asserting if the list of invalid weights are None 
        # If this test fails then it is positive the function is covering the edge cases
        for weight in self.list_of_invalid_weights:
            invalid_weight = self.test_data_cleaner.convert_to_kg(weight)
            self.assertIsNone(invalid_weight)
      
    
    def test_clean_dates(self):
        
        formatted_date_list = []

        for date in self.list_of_dates:
            formatted_date = self.test_data_cleaner.clean_dates(date)
            formatted_date_list.append(formatted_date)

        expected_output = [Timestamp('2013-10-14 00:00:00'),
                            Timestamp('2010-07-17 00:00:00'),
                            Timestamp('2000-06-15 00:00:00'),
                            Timestamp('2017-01-20 00:00:00'),
                            Timestamp('2006-09-03 00:00:00')]

        # Asserting if the created list of formatted dates is the same as the expected output
        self.assertEqual(formatted_date_list, expected_output)
    
    @classmethod
    def tearDown(cls):
        pass


if __name__ == "__main__":
    unittest.main(argv=[''], verbosity=2, exit=False)