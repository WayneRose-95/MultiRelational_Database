import unittest
import HtmlTestRunner
from test_data_cleaning import get_absolute_file_path
from test_data_cleaning import TestDataCleaning
from test_data_extraction import TestDatabaseExtraction
from test_database_utils import TestDatabaseConnector
from test_currency_rate_extraction import TestCurrencyRateExtraction
from datetime import datetime 
import os
# Create a Test Suite
test_suite = unittest.TestSuite()

# Add individual test cases to the Test Suite
test_suite.addTest(unittest.makeSuite(TestDatabaseConnector))
test_suite.addTest(unittest.makeSuite(TestDataCleaning))
test_suite.addTest(unittest.makeSuite(TestDatabaseExtraction))
test_suite.addTest(unittest.makeSuite(TestCurrencyRateExtraction))

# Get the current date in the format "YYYY-MM-DD"
current_date = datetime.now().strftime("%Y-%m-%d")

# Create the directory path
directory_path = get_absolute_file_path(current_date, "test_results")
# Create the directory path
# directory_path = f"test_results/{current_date}"

# Create the directory if it doesn't exist
os.makedirs(directory_path, exist_ok=True)

# Set the report file path with the directory and file name
report_file = f"{directory_path}"


# Create a test runner with HTMLTestRunner
test_runner = HtmlTestRunner.HTMLTestRunner(output=report_file)

# Run the Test Suite using the test runner
test_runner.run(test_suite)
# # Run the Test Suite
# test_runner = unittest.TextTestRunner()
# test_runner.run(test_suite)
