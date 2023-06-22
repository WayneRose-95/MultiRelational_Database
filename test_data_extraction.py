import unittest
from unittest import mock
from unittest.mock import patch, MagicMock 
from sqlalchemy.exc import OperationalError, DBAPIError
from sqlalchemy import create_engine
import pandas as pd 
from data_extraction import DatabaseExtractor
from database_utils import DatabaseConnector
import tabula 

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
        cls.test_url_wrong = "s3://data-handling-private/prod.csv"
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

        # Testing to see if the code raises a ValueError on passing an invalid url
        with self.assertRaises(ValueError):
            self.test_extractor.retrieve_pdf_data("Not a link to a PDF")

    
    @patch('data_extraction.DatabaseExtractor._is_valid_url')
    @patch('data_extraction.tabula.read_pdf')
    def test_mock_retrieve_pdf_data(self, mock_read_pdf, mock_is_valid_url):

        # Mock the return value of _is_valid_url
        mock_is_valid_url.return_value = True

        # Mock the return value of tabula.read_pdf
        mock_table1 = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]})
        mock_table2 = pd.DataFrame({'A': [7, 8, 9], 'B': [10, 11, 12]})
        mock_read_pdf.return_value = [mock_table1, mock_table2]


        # Call the method with a mock PDF link
        result = self.test_extractor.retrieve_pdf_data('mock_pdf_link')

        # Verify the expected behavior
        # Assuming 2 tables with 3 rows each
        self.assertEqual(len(result), 6)
        self.assertTrue(mock_is_valid_url.called)  
        self.assertTrue(mock_read_pdf.called)
        mock_read_pdf.assert_called_with('mock_pdf_link', multiple_tables=True, pages='all', lattice=True)


     
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
    
    def test_read_s3_bucket_to_dataframe(self):
        test_data = self.test_extractor.read_s3_bucket_to_dataframe(
            self.test_url
        )
        self.assertIsInstance(test_data, pd.DataFrame)

        with self.assertRaises(ValueError):
            self.test_extractor.read_s3_bucket_to_dataframe(
                "This is not an S3 bucket"
            )
        
        with self.assertRaises(Exception):
            self.test_extractor(
                self.test_url_wrong
            )
    
    
    @patch('data_extraction.boto3.client')
    def test_mock_read_s3_bucket_to_dataframe(self, mock_client):
        # Mock the response from s3_client.get_object
        mock_body = MagicMock()
        mock_body.read.return_value.decode.return_value = 'col1,col2\nvalue1,value2\n'
        mock_response = {'Body': mock_body}
        mock_client.return_value.get_object.return_value = mock_response

        # Define the expected DataFrame
        expected_df = pd.DataFrame({'col1': ['value1'], 'col2': ['value2']})

        # Call the method under test
        s3_url = 's3://my-bucket-name/my-data.csv'
        result = self.test_extractor.read_s3_bucket_to_dataframe(s3_url)

        # Assert the result matches the expected DataFrame
        pd.testing.assert_frame_equal(result, expected_df)

        # Assert that the boto3 client was called with the correct arguments
        mock_client.assert_called_once_with('s3')
        mock_client.return_value.get_object.assert_called_once_with(Bucket='my-bucket-name', Key='my-data.csv')

     
    def test_parse_s3_url(self):
        # Testing if the method returns a tuple 
        self.assertIsInstance(self.test_extractor._parse_s3_url(self.test_url), tuple)

        # Testing if the expected output matches the output of the method. 
        expected_output = ('data-handling-public', 'products.csv')
        self.assertEqual(self.test_extractor._parse_s3_url(self.test_url), expected_output)

if __name__ == '__main__':
    unittest.main(argv=[''], verbosity=2, exit=False)