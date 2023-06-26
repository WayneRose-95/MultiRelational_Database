import unittest 
import pandas as pd 
from currency_rate_extraction import CurrencyRateExtractor
from selenium.common.exceptions import InvalidArgumentException
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import InvalidSelectorException
from time import sleep
import os 
class TestCurrencyRateExtraction(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print("This is the setup class")
        cls.currency_extractor = CurrencyRateExtractor()
        cls.test_url = "https://www.x-rates.com/table/?from=GBP&amount=1"
        cls.test_url_incorrect = "This is not a url"
        cls.timestamp_xpath = '//*[@id="content"]/div[1]/div/div[1]/div[1]/span[2]'
        cls.timestamp_xpath_incorrect = 'This is not an xpath'
        cls.table_body_xpath = '//table[@class="tablesorter ratesTable"]/tbody'
        cls.table_body_xpath_incorrect = "/table/div"
        cls.table_headers = ["currency_name", "conversion_rate", "conversion_rate_percentage"]
        cls.empty_data_list = []
        cls.empty_data_dict = {}
        cls.file_name = "test_file"
        cls.test_webpage = cls.currency_extractor.land_first_url(cls.test_url)
        cls.extract_table = cls.currency_extractor.extract_table(cls.table_body_xpath)


    
    
    def test_land_first_page(self):
        # Testing if the wedriver has landed the first page correctly
        current_webpage_url = self.currency_extractor.driver.current_url
        self.assertEqual(current_webpage_url, self.test_url)
        # Testing if the following exception is raised 


   
    def test_extract_table(self):
        
        test_table = self.extract_table
        
        # Testing if the table returned is a list from the website 
        self.assertIsInstance(self.extract_table, list)

        # Testing if the length of the table is the same as the desired number
        expected_length_of_table = 64 
        self.assertEqual(len(test_table), expected_length_of_table)
    
    
    def test_data_to_dataframe(self):

        test_dataframe = self.currency_extractor.data_to_dataframe(
            self.extract_table,
            self.table_headers,
            self.file_name

        )

        self.assertIsInstance(test_dataframe, pd.DataFrame)
        
    
    def test_extract_timestamp(self):

        test_time_stamp = self.currency_extractor.extract_timestamp(self.timestamp_xpath)
        # Testing if a string is returned
        self.assertIsInstance(test_time_stamp, str)

        
    @classmethod
    def tearDownClass(cls):
        os.remove(f"{cls.file_name}.csv")
        print("End of tests")
        cls.currency_extractor.driver.quit()

if __name__ =="__main__":
    unittest.main(argv=[''], verbosity=2, exit=False)
