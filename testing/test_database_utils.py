import unittest
from unittest import mock
from unittest.mock import patch 
from sqlalchemy import create_engine
from database_scripts.database_utils import DatabaseConnector
from database_scripts.file_handler import get_absolute_file_path
import yaml
import sys
import os  



class TestDatabaseConnector(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Mocking the create_engine function
        cls.create_engine_patch = mock.patch('sqlalchemy.create_engine')
        cls.mock_create_engine = cls.create_engine_patch.start()
        cls.source_database_credentials = get_absolute_file_path("db_creds.yaml", "credentials") 
        cls.test_connection = DatabaseConnector()


    @classmethod
    def tearDownClass(cls):
        cls.create_engine_patch.stop()


    def test_read_database_credentials(self):
        
        database_credentials = self.test_connection.read_database_credentials(self.source_database_credentials)
        
        self.assertIsInstance(database_credentials, dict)

        # Testing with invalid .yaml file
        with self.assertRaises(yaml.YAMLError):
            self.test_connection.read_database_credentials('test_config_yaml.yaml')
        # Testing to restrict users to only use .yaml files
        with self.assertRaises(ValueError):
            self.test_connection.read_database_credentials('test_file.txt')
        # Testing with a file which cannot be found. 
        with self.assertRaises(FileNotFoundError):
            self.test_connection.read_database_credentials("random_file.yaml")
        
    
    def test_create_connection_string(self):
        # Pass a valid connection_string from the output of the create_connection_string method
        test_connection_string_valid = self.test_connection.create_connection_string(self.source_database_credentials)
        # Assert if the output is a string 
        self.assertIsInstance(test_connection_string_valid, str)
        

    @mock.patch('database_scripts.database_utils.create_engine')
    def test_initialise_database_connection(self, mock_create_engine):
        # Create a mock engine with set return values 
        mock_engine = mock.MagicMock()
        mock_engine.connect.return_value = True
        mock_create_engine.return_value = mock_engine
         

        # Call the method being tested
        result = self.test_connection.initialise_database_connection(self.source_database_credentials)

        # Assert that create_engine was called with the correct connection string
        expected_connection_string = self.test_connection.create_connection_string(self.source_database_credentials)
        print(expected_connection_string)
        expected_connection_string_without_isolation = expected_connection_string.split('?')[0]
        print(expected_connection_string_without_isolation)
        mock_create_engine.assert_called_once_with(expected_connection_string_without_isolation, isolation_level='AUTOCOMMIT')

        # Assert that engine.connect() was called
        mock_engine.connect.assert_called_once()

        # Assert that the method returns the database engine
        self.assertEqual(result, mock_engine)


    @classmethod
    def tearDownClass(cls):
        mock.patch.stopall()

if __name__ == '__main__':
    unittest.main(argv=[''], verbosity=2, exit=False)

