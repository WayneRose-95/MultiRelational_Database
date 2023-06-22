import unittest
from unittest import mock
from unittest.mock import patch, MagicMock 
from sqlalchemy.exc import OperationalError, DBAPIError
import pandas as pd 
from data_extraction import DatabaseExtractor
from database_utils import DatabaseConnector

class DatabaseExtractionTest(unittest.TestCase):

    @classmethod 
    def setUpClass(cls):
        cls.config_file_name = 'db_creds.yaml'
        cls.config_file_name_wrong = 'testing.yaml'
        cls.table_name = 'legacy_users'
        cls.table_name_wrong = 'legacy_uxers'
        cls.pdf_link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
        cls.s3_json_link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"
        cls.test_url = "s3://data-handling-public/products.csv"
        cls.test_extractor = DatabaseExtractor() 
        cls.test_connector = DatabaseConnector()
        pass 


     
    def test_list_db_tables(self):
        # create a test_list of table names using the method 
        test_list = self.test_extractor.list_db_tables(self.config_file_name)
        # List the expected outputs 
        expected_output = ['legacy_store_details', 'legacy_users', 'orders_table']
        # Testing if the output is a list 
        self.assertIsInstance(test_list, list)
        # Testing if the test_list is the same as the expected output 
        self.assertEqual(test_list, expected_output)

        # Testing if an exception is raised when the wrong file_name is passed
        with self.assertRaises(Exception): 
            self.test_extractor.list_db_tables(self.config_file_name_wrong)

       
    def test_read_rds_table(self):

        test_read = self.test_extractor.read_rds_table(self.table_name, self.config_file_name)
        # Testing if a dataframe is returned
        self.assertIsInstance(test_read, pd.DataFrame)
        #Testing if a ValueError is raised when the wrong credentials are passed 
        with self.assertRaises(ValueError):
            self.test_extractor.read_rds_table(self.table_name_wrong, self.config_file_name_wrong)
            

   
    def test_retrieve_pdf_data(self):
        
        test_pdf_table = self.test_extractor.retrieve_pdf_data(
            self.pdf_link
        )
        # Testing if a pandas dataframe is returned from the method 
        self.assertIsInstance(test_pdf_table, pd.DataFrame)

     
    def test_read_json_from_s3(self):

        test_dataframe_from_json = self.test_extractor.read_json_from_s3(
            self.s3_json_link 
        )
        # Testing if a pandas dataframe is returned from the method 
        self.assertIsInstance(test_dataframe_from_json, pd.DataFrame)
        
        # Testing if the method fails when given the wrong url string
        with self.assertRaises(Exception):
            self.test_extractor.read_json_from_s3(
                "This is not a json url"
            )

   
    def test_parse_s3_url(self):
        # Testing if the method returns a tuple 
        self.assertIsInstance(self.test_extractor._parse_s3_url(self.test_url), tuple)

        # Testing if the expected output matches the output of the method. 
        expected_output = ('data-handling-public', 'products.csv')
        self.assertEqual(self.test_extractor._parse_s3_url(self.test_url), expected_output)

if __name__ == '__main__':
    unittest.main(argv=[''], verbosity=2, exit=False)