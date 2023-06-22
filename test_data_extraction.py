import unittest
from unittest import mock
from unittest.mock import patch 
from sqlalchemy.exc import OperationalError, DBAPIError
import pandas as pd 
from data_extraction import DatabaseExtractor

class DatabaseExtractionTest(unittest.TestCase):

    @classmethod 
    def setUpClass(cls):
        cls.config_file_name = 'db_creds.yaml'
        cls.config_file_name_wrong = 'testing.yaml'
        cls.table_name = 'legacy_users'
        cls.table_name_wrong = 'legacy_uxers'
        cls.test_extractor = DatabaseExtractor() 
        pass 


   
    def test_list_db_tables(self):
        test_list = self.test_extractor.list_db_tables(self.config_file_name)
        expected_output = ['legacy_store_details', 'legacy_users', 'orders_table']
        self.assertIsInstance(test_list, list)
        self.assertEqual(test_list, expected_output)

        with self.assertRaises(Exception): 
            self.test_extractor.list_db_tables(self.config_file_name_wrong)
    
     
    def test_read_rds_table(self):

        test_read = self.test_extractor.read_rds_table(self.table_name, self.config_file_name)

        self.assertIsInstance(test_read, pd.DataFrame)

        with self.assertRaises(ValueError):
            self.test_extractor.read_rds_table(self.table_name_wrong, self.config_file_name_wrong)
            print('Exception Raised')
            
if __name__ == '__main__':
    unittest.main(argv=[''], verbosity=2, exit=False)