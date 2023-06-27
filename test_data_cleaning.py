import unittest 
from unittest.mock import patch 
from data_cleaning import DataCleaning 
from data_extraction import DatabaseExtractor
import pandas as pd 
from pandas import Timestamp 

class TestDataCleaning(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Setting up credentials files 
        cls.database_credentials_dev = 'sales_data_creds_dev.yaml'
        cls.database_credentials_test = 'sales_data_creds_test.yaml'
        cls.source_database_config_file_name = "db_creds.yaml"

        # Setting up table names 
        cls.source_data_table_test = "legacy_users"
        cls.source_data_table_store_details = "legacy_store_details"
        cls.datastore_date_time_details_table_name = "dim_date_times"
        cls.datastore_product_details_table_name = "dim_product_details"
        cls.datastore_store_details_table_name = "dim_store_details"
        cls.datastore_user_details_table_name = "dim_users"
        cls.datastore_card_details_table_name = "dim_card_details"
        cls.json_local_datastore_table_name = 'dim_currency'
        cls.datastore_orders_table_name = "orders_table"

        # Setting up land_table_names 
        cls.datastore_land_users_table_name = "land_user_data"
        
        # Setting up links and file pathways to data sources 
        cls.pdf_link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
        cls.json_s3_link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"
        cls.csv_s3_link = "s3://data-handling-public/products.csv"
        cls.json_file_path = "country_data.json" 

        # Setting up test lists
        cls.json_file_path_subset = ["US", "GB", "DE"]
        cls.list_of_weights = [    
       '0.08kg', '420g', '1.68kg', '0.718kg', '600ml', '0.504kg', '480g', '8 x 150g', '6oz', 
        ]
        cls.list_of_invalid_weights = ['110lbs', '10 Gallons', '10ms']
        cls.list_of_dates = ['2013-10-14','2010-07-17','2000-06-15','2017-01-20','2006 September 03']

        # Setting up the classes within the setUpClass
        cls.test_data_extractor_dev = DatabaseExtractor()
        cls.test_data_extractor_test = DatabaseExtractor()
        cls.test_data_cleaner_dev = DataCleaning(cls.database_credentials_dev)
        cls.test_data_cleaner_test = DataCleaning(cls.database_credentials_test)
    
    
    def test_clean_user_data(self):
        # Run the method to upload the land table to the test database 
        test_table_to_upload_to_database = self.test_data_cleaner_test.clean_user_data(
            self.source_data_table_test,
            self.source_database_config_file_name,
            self.datastore_land_users_table_name

        )
        # Read the dimension table from the dev database, return it as a dataframe to compare to the uploaded land_table from the test database
        reading_rds_dim_table_dev = self.test_data_extractor_dev.read_rds_table(self.datastore_user_details_table_name, self.database_credentials_dev)
        
        '''
        LAND_TABLE testing
        '''
        # Read the land table from the dev database, return it as a dataframe and compare it to the land_table uploaded from the test database 
        reading_rds_land_table_dev = self.test_data_extractor_dev.read_rds_table(self.datastore_land_users_table_name, self.database_credentials_dev)
        print(f"Number of rows in land_users from Dev Database : {len(reading_rds_land_table_dev)}")
        print(f"Number of rows in land_users from Test Database : {len(test_table_to_upload_to_database)}")

        # Asserting that the number of rows in the uploaded land_table is the same as the land_table read in from dev database
        self.assertEqual(len(reading_rds_land_table_dev), len(test_table_to_upload_to_database))

        print(f" The type of the variable {type(test_table_to_upload_to_database)}")
        # Next, test if the method returns a dataframe 
        self.assertIsInstance(test_table_to_upload_to_database, pd.DataFrame)

        # Testing if the length of the index column of the land_table test vs land_table in dev are the same
        expected_index = pd.RangeIndex(start=0, stop=len(reading_rds_land_table_dev), step=1)

        # Print the length of the expected_index and the length of the table_index
        print(f"Length of the expected_index : {len(expected_index)}")
        print(f"Length of the table_index : {len(test_table_to_upload_to_database.index)}")

        # Assert if the length of the index from the land table in dev database is the same as the uploaded table from test database
        self.assertEqual(test_table_to_upload_to_database.index.equals(expected_index), True)

        # Assert the column names of the land_table uploaded from test database 
        expected_columns = [
            'user_key', 'first_name', 'last_name', 'birth_date', 'company', 'e-mail_address',
            'address', 'country', 'country_index', 'phone_number', 'join_date', 'user_uuid'
        ]
        # Print the test list of columns and the expected list of columns 
        print(f"The test list: {test_table_to_upload_to_database.columns.tolist()}")
        print(f"The expected list : {expected_columns}")

        self.assertEqual(test_table_to_upload_to_database.columns.tolist(), expected_columns)

        '''
        DIM_TABLE Comparison testing 

        '''
        # Assert that the number of rows in the land_table is equal to the dimension table read in from dev database 
        print(f"Number of rows in dim_users from Dev Database : {len(reading_rds_dim_table_dev)}")
        print(f"Number of rows in land_users from Test Database : {len(test_table_to_upload_to_database)}")

        self.assertEqual(len(test_table_to_upload_to_database), len(reading_rds_dim_table_dev))

         # Testing if the length of the index column of the land_table test vs land_table in dev are the same
        dim_expected_index = pd.RangeIndex(start=0, stop=len(reading_rds_dim_table_dev), step=1)

        # Print the length of the expected_index and the length of the table_index
        print(f"Length of the expected_index : {len(dim_expected_index)}")
        print(f"Length of the table_index : {len(test_table_to_upload_to_database.index)}")

        # Assert if the two lists of columns are equal 
        self.assertEqual(test_table_to_upload_to_database.index.equals(dim_expected_index), True)

        print(f"Columns in dim_table {reading_rds_dim_table_dev.columns.to_list()}")
        print(f"Columns in dim_table {test_table_to_upload_to_database.columns.to_list()}")
        dim_users_column_list = reading_rds_dim_table_dev.columns.to_list()
        # Asserting if the columns in the dim_table from dev database are the same as in the land_table 
        self.assertEqual(dim_users_column_list[1:], test_table_to_upload_to_database.columns.to_list())



    @unittest.skip
    def test_clean_store_data(self):
        # Compare the tables between test database and dev sales_data database 
        test_table_to_upload_to_database = self.test_data_cleaner_test.clean_store_data(
            self.source_data_table_store_details,
            self.source_database_config_file_name,
            self.datastore_store_details_table_name

        )

        reading_rds_table_dev = self.test_data_extractor_dev.read_rds_table(self.datastore_store_details_table_name,
                                                                             self.database_credentials_dev)
        
        print(f"Number of rows in dim_store_details from Dev Database : {len(reading_rds_table_dev)}")
        print(f"Number of rows in dim_store_details from Test Database : {len(test_table_to_upload_to_database)}")

        # Asserting that the number of rows in uploaded table is the same as dev
        self.assertEqual(len(reading_rds_table_dev), len(test_table_to_upload_to_database))

        print(f" The type of the variable {type(test_table_to_upload_to_database)}")
        # Next, test if the method returns a dataframe 
        self.assertIsInstance(test_table_to_upload_to_database, pd.DataFrame)

        # Assert the index of the cleaned_table
        expected_index = pd.RangeIndex(start=0, stop=len(test_table_to_upload_to_database), step=1)

        # Print the length of the expected_index and the length of the table_index
        print(f"Length of the expected_index : {len(expected_index)}")
        print(f"Length of the table_index {len(test_table_to_upload_to_database.index)}")

        self.assertEqual(test_table_to_upload_to_database.index.equals(expected_index), True)

        expected_columns  = [
            'store_key', 'store_address','longitude','latitude','city',
            'store_code','number_of_staff','opening_date','store_type','country_code','region'
        ]
        # print a comparison between the test_list of columns and the expected list of columns
        print(f"The test list: {test_table_to_upload_to_database.columns.tolist()}")
        print(f"The expected list : {expected_columns}")

        # Assert if the two lists of columns are equal 
        self.assertEqual(test_table_to_upload_to_database.columns.tolist(), expected_columns)

    @unittest.skip
    def test_clean_card_details(self):
        # Compare the tables between test database and dev sales_data database 
        test_table_to_upload_to_database = self.test_data_cleaner_test.clean_card_details(
           self.pdf_link,
           self.datastore_card_details_table_name
        )

        reading_rds_table_dev = self.test_data_extractor_dev.read_rds_table(self.datastore_card_details_table_name,
                                                                             self.database_credentials_dev)
        # Print the number of rows in each table 
        print(f"Number of rows in dim_card_details from Dev Database : {len(reading_rds_table_dev)}")
        print(f"Number of rows in dim_card_details from Test Database : {len(test_table_to_upload_to_database)}")

        # Asserting that the number of rows in uploaded table is the same as dev
        self.assertEqual(len(reading_rds_table_dev), len(test_table_to_upload_to_database))

        print(f" The type of the variable {type(test_table_to_upload_to_database)}")
        # Next, test if the method returns a dataframe 
        self.assertIsInstance(test_table_to_upload_to_database, pd.DataFrame)

        # Assert the index of the cleaned_table
        expected_index = pd.RangeIndex(start=0, stop=len(test_table_to_upload_to_database), step=1)

        # Print the length of the expected_index and the length of the table_index
        print(f"Length of the expected_index : {len(expected_index)}")
        print(f"Length of the table_index : {len(test_table_to_upload_to_database.index)}")

        self.assertEqual(test_table_to_upload_to_database.index.equals(expected_index), True)

        expected_columns  = [
            'card_key','card_number','expiry_date','card_provider','date_payment_confirmed'
        ]
        # print a comparison between the test_list of columns and the expected list of columns
        print(f"The test list: {test_table_to_upload_to_database.columns.tolist()}")
        print(f"The expected list : {expected_columns}")

        # Assert if the two lists of columns are equal 
        self.assertEqual(test_table_to_upload_to_database.columns.tolist(), expected_columns)
    
    @unittest.skip
    def test_clean_orders_table(self):
        # Compare the tables between test database and dev sales_data database 
        test_table_to_upload_to_database = self.test_data_cleaner_test.clean_orders_table(
           self.datastore_orders_table_name,
           self.source_database_config_file_name,
           self.datastore_orders_table_name
        )

        reading_rds_table_dev = self.test_data_extractor_dev.read_rds_table(self.datastore_orders_table_name, 
                                                                            self.database_credentials_dev)
        # Print the number of rows in each table 
        print(f"Number of rows in orders_table from Dev Database : {len(reading_rds_table_dev)}")
        print(f"Number of rows in orders_table from Test Database : {len(test_table_to_upload_to_database)}")

        # Asserting that the number of rows in uploaded table is the same as dev
        self.assertEqual(len(reading_rds_table_dev), len(test_table_to_upload_to_database))

        print(f" The type of the variable {type(test_table_to_upload_to_database)}")
        # Next, test if the method returns a dataframe 
        self.assertIsInstance(test_table_to_upload_to_database, pd.DataFrame)

        # Assert the index of the cleaned_table
        expected_index = pd.RangeIndex(start=0, stop=len(test_table_to_upload_to_database), step=1)

        # Print the length of the expected_index and the length of the table_index
        print(f"Length of the expected_index : {len(expected_index)}")
        print(f"Length of the table_index {len(test_table_to_upload_to_database.index)}")

        self.assertEqual(test_table_to_upload_to_database.index.equals(expected_index), True)

        expected_columns  = [
            'order_key', 'date_uuid', 'user_uuid', 'card_number', 'store_code', 'product_code', 'product_quantity'
        ]
        # print a comparison between the test_list of columns and the expected list of columns
        print(f"The test list: {test_table_to_upload_to_database.columns.tolist()}")
        print(f"The expected list : {expected_columns}")

        # Assert if the two lists of columns are equal 
        self.assertEqual(test_table_to_upload_to_database.columns.tolist(), expected_columns)

         
    @unittest.skip
    def test_clean_time_event_table(self):
        # Compare the tables between test database and dev sales_data database 
        test_table_to_upload_to_database = self.test_data_cleaner_test.clean_time_event_table(
           self.json_s3_link,
           self.datastore_date_time_details_table_name
        )

        reading_rds_table_dev = self.test_data_extractor_dev.read_rds_table(self.datastore_date_time_details_table_name
                                                                            , self.database_credentials_dev)
        # Print the number of rows in each table 
        print(f"Number of rows in orders_table from Dev Database : {len(reading_rds_table_dev)}")
        print(f"Number of rows in orders_table from Test Database : {len(test_table_to_upload_to_database)}")

        # Asserting that the number of rows in uploaded table is the same as dev
        self.assertEqual(len(reading_rds_table_dev), len(test_table_to_upload_to_database))

        print(f" The type of the variable {type(test_table_to_upload_to_database)}")
        # Next, test if the method returns a dataframe 
        self.assertIsInstance(test_table_to_upload_to_database, pd.DataFrame)

        # Assert the index of the cleaned_table
        expected_index = pd.RangeIndex(start=0, stop=len(test_table_to_upload_to_database), step=1)

        # Print the length of the expected_index and the length of the table_index
        print(f"Length of the expected_index : {len(expected_index)}")
        print(f"Length of the table_index {len(test_table_to_upload_to_database.index)}")

        self.assertEqual(test_table_to_upload_to_database.index.equals(expected_index), True)

        expected_columns  = [
            'time_key','timestamp','day', 'month', 'year', 'time_period','date_uuid'
        ]
        # print a comparison between the test_list of columns and the expected list of columns
        print(f"The test list: {test_table_to_upload_to_database.columns.tolist()}")
        print(f"The expected list : {expected_columns}")

        # Assert if the two lists of columns are equal 
        self.assertEqual(test_table_to_upload_to_database.columns.tolist(), expected_columns)
    
    @unittest.skip
    def test_clean_product_table(self):
        # Compare the tables between test database and dev sales_data database 
        test_table_to_upload_to_database = self.test_data_cleaner_test.clean_product_table(
           self.csv_s3_link,
           self.datastore_product_details_table_name
        )

        reading_rds_table_dev = self.test_data_extractor_dev.read_rds_table(self.datastore_product_details_table_name
                                                                            , self.database_credentials_dev)
        # Print the number of rows in each table 
        print(f"Number of rows in orders_table from Dev Database : {len(reading_rds_table_dev)}")
        print(f"Number of rows in orders_table from Test Database : {len(test_table_to_upload_to_database)}")

        # Asserting that the number of rows in uploaded table is the same as dev
        self.assertEqual(len(reading_rds_table_dev), len(test_table_to_upload_to_database))

        print(f" The type of the variable {type(test_table_to_upload_to_database)}")
        # Next, test if the method returns a dataframe 
        self.assertIsInstance(test_table_to_upload_to_database, pd.DataFrame)

        # Assert the index of the cleaned_table
        expected_index = pd.RangeIndex(start=0, stop=len(test_table_to_upload_to_database), step=1)

        # Print the length of the expected_index and the length of the table_index
        print(f"Length of the expected_index : {len(expected_index)}")
        print(f"Length of the table_index {len(test_table_to_upload_to_database.index)}")

        self.assertEqual(test_table_to_upload_to_database.index.equals(expected_index), True)

        expected_columns  = [
            "product_key", "EAN", "product_name", "product_price", "weight",
            "category", "date_added", "uuid","availability","product_code"
        ]
        # print a comparison between the test_list of columns and the expected list of columns
        print(f"The test list: {test_table_to_upload_to_database.columns.tolist()}")
        print(f"The expected list : {expected_columns}")

        # Assert if the two lists of columns are equal 
        self.assertEqual(test_table_to_upload_to_database.columns.tolist(), expected_columns)
    
    @unittest.skip
    def test_clean_currency_table(self):
        # Compare the tables between test database and dev sales_data database 
        test_table_to_upload_to_database = self.test_data_cleaner_test.clean_currency_table(
            self.json_file_path, 
            self.json_file_path_subset, 
            self.json_local_datastore_table_name 
        )

        reading_rds_table_dev = self.test_data_extractor_dev.read_rds_table(self.json_local_datastore_table_name, self.database_credentials_dev)
        # Print the number of rows in each table 
        print(f"Number of rows in orders_table from Dev Database : {len(reading_rds_table_dev)}")
        print(f"Number of rows in orders_table from Test Database : {len(test_table_to_upload_to_database)}")

        # Asserting that the number of rows in uploaded table is the same as dev
        self.assertEqual(len(reading_rds_table_dev), len(test_table_to_upload_to_database))

        print(f" The type of the variable {type(test_table_to_upload_to_database)}")
        # Next, test if the method returns a dataframe 
        self.assertIsInstance(test_table_to_upload_to_database, pd.DataFrame)

        # Assert the index of the cleaned_table
        expected_index = pd.RangeIndex(start=0, stop=len(test_table_to_upload_to_database), step=1)

        # Print the length of the expected_index and the length of the table_index
        print(f"Length of the expected_index : {len(expected_index)}")
        print(f"Length of the table_index {len(test_table_to_upload_to_database.index)}")

        self.assertEqual(test_table_to_upload_to_database.index.equals(expected_index), True)

        expected_columns  = [
            "currency_key", "country_name", "currency_code", "country_code", "currency_symbol"
        ]
        # print a comparison between the test_list of columns and the expected list of columns
        print(f"The test list: {test_table_to_upload_to_database.columns.tolist()}")
        print(f"The expected list : {expected_columns}")

        # Assert if the two lists of columns are equal 
        self.assertEqual(test_table_to_upload_to_database.columns.tolist(), expected_columns)

    
    def test_convert_to_kg_valid_weights(self):
        list_of_weights = self.list_of_weights

        test_list = []
        for item in list_of_weights:
            weight = self.test_data_cleaner_test.convert_to_kg(item)
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
            invalid_weight = self.test_data_cleaner_test.convert_to_kg(weight)
            self.assertIsNone(invalid_weight)
      
   
    def test_clean_dates(self):
        
        formatted_date_list = []

        for date in self.list_of_dates:
            formatted_date = self.test_data_cleaner_test.clean_dates(date)
            formatted_date_list.append(formatted_date)

        expected_output = [Timestamp('2013-10-14 00:00:00'),
                            Timestamp('2010-07-17 00:00:00'),
                            Timestamp('2000-06-15 00:00:00'),
                            Timestamp('2017-01-20 00:00:00'),
                            Timestamp('2006-09-03 00:00:00')]

        # Asserting if the created list of formatted dates is the same as the expected output
        self.assertEqual(formatted_date_list, expected_output)
            


if __name__ == "__main__":
    unittest.main(argv=[''], verbosity=2, exit=False)