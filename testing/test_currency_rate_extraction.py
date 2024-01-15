import unittest 
import pandas as pd 
from database_scripts.currency_rate_extraction_poc import CurrencyExtractor
from time import sleep
import os 
class TestCurrencyRateExtraction(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print("This is the setup class")
        cls.test_url = "https://www.x-rates.com/table/?from=GBP&amount=1"
        cls.currency_extractor = CurrencyExtractor(cls.test_url)
        cls.table_headers = ["currency_name", "conversion_rate", "conversion_rate_percentage"]
        cls.file_name = "test_file"
        cls.html_data = cls.currency_extractor.read_html_data()

    def test_html_to_dataframe(self):

        test_html_datatable1 = self.currency_extractor.html_to_dataframe(self.html_data, 0)

        test_html_datatable2 = self.currency_extractor.html_to_dataframe(self.html_data, 1)

        self.assertIsInstance(test_html_datatable1, pd.DataFrame)
        self.assertIsInstance(test_html_datatable2, pd.DataFrame)


    def test_merge_dataframes(self): 

        test_html_datatable1 = self.currency_extractor.html_to_dataframe(self.html_data, 0)

        test_html_datatable2 = self.currency_extractor.html_to_dataframe(self.html_data, 1)

        combined_table = self.currency_extractor.merge_dataframes("right", test_html_datatable1, test_html_datatable2)

        self.assertIsInstance(combined_table, pd.DataFrame)

    
    def test_save_data(self):

        test_html_datatable1 = self.currency_extractor.html_to_dataframe(self.html_data, 0)

        test_html_datatable2 = self.currency_extractor.html_to_dataframe(self.html_data, 1)

        combined_table = self.currency_extractor.merge_dataframes("right", test_html_datatable1, test_html_datatable2)

        saved_data = self.currency_extractor.save_data(combined_table, self.file_name, ["currency_name", "conversion_rate", "conversion_rate_percentage"])
        
        # Get the current directory 
        current_directory = os.getcwd()

        # Making the full file path 
        file_path = os.path.join(current_directory, f"{self.file_name}.csv")

        # The expected file_path
        file_exists = os.path.exists(file_path)

        self.assertTrue(file_exists, f"The file {self.file_name} does not exist in {file_path}")

        
    @classmethod
    def tearDownClass(cls):
        os.remove(f"{cls.file_name}.csv")
        print("End of tests")


if __name__ =="__main__":
    unittest.main(argv=[''], verbosity=2, exit=False)
