import unittest
from unittest import mock
from unittest.mock import patch, MagicMock 
from sqlalchemy.exc import OperationalError, DBAPIError
from sqlalchemy import create_engine
import pandas as pd 
from database_scripts.data_extraction import DataExtractor
from database_scripts.database_utils import DatabaseConnector
from database_scripts.file_handler import get_absolute_file_path
import tabula 
import os 


class TestDatabaseExtraction(unittest.TestCase):

    @classmethod 
    def setUpClass(cls):
        cls.config_file_name = get_absolute_file_path('db_creds.yaml', 'credentials') # r'credentials\db_creds.yaml'
        cls.database_name = 'postgres'
        cls.database_name_wrong = 'not a database'
        cls.config_file_name_wrong = 'testing.yaml'
        cls.table_name = 'legacy_users'
        cls.table_name_wrong = 'legacy_uxers'
        cls.pdf_link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
        cls.s3_json_link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"
        cls.test_url = "s3://data-handling-public/products.csv"
        cls.test_url_wrong = "s3://data-handling-private/prod.csv"
        cls.test_json_filepath = get_absolute_file_path('country_data.json', 'source_data_files') #r"source_data_files\country_data.json"
        cls.test_json_filepath_wrong = "data.json"
        cls.test_extractor = DataExtractor() 
        cls.test_connector = DatabaseConnector()
        cls.test_source_database_engine = cls.test_connector.initialise_database_connection(cls.config_file_name, True, cls.database_name)
        pass 


      
    def test_read_rds_table(self):

        test_read = self.test_extractor.read_rds_table(self.table_name, self.test_source_database_engine)
        # Testing if a dataframe is returned
        self.assertIsInstance(test_read, pd.DataFrame)
        #TODO: Add another test to raise an error below 

     
    
    def test_retrieve_pdf_data(self):
        
        test_pdf_table = self.test_extractor.retrieve_pdf_data(
            self.pdf_link
        )
        # Testing if a pandas dataframe is returned from the method 
        self.assertIsInstance(test_pdf_table, pd.DataFrame)

        # Testing to see if the code raises a ValueError on passing an invalid url
        with self.assertRaises(ValueError):
            self.test_extractor.retrieve_pdf_data("Not a link to a PDF")

    
    @patch('database_scripts.data_extraction.DataExtractor._is_valid_url')
    @patch('database_scripts.data_extraction.tabula.read_pdf')
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
    
    
    @patch('database_scripts.data_extraction.boto3.client')
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

    def test_read_json_local(self):
        
        sample_json_dataframe = self.test_extractor.read_json_local(self.test_json_filepath)
        print(type(sample_json_dataframe))
        # Testing if the file returned is a pandas dataframe
        self.assertIsInstance(sample_json_dataframe, pd.DataFrame)

        with self.assertRaises(Exception):
            # Testing if putting an incorrect file path throws an exception
            self.test_extractor.read_json_local(self.test_json_filepath_wrong)




if __name__ == '__main__':
    unittest.main(argv=[''], verbosity=2, exit=False)