import unittest
import HtmlTestRunner
from test_data_cleaning import TestDataCleaning
from test_data_extraction import TestDatabaseExtraction
from test_database_utils import TestDatabaseConnector

# Create a Test Suite
test_suite = unittest.TestSuite()

# Add individual test cases to the Test Suite
test_suite.addTest(unittest.makeSuite(TestDatabaseConnector))
test_suite.addTest(unittest.makeSuite(TestDataCleaning))
test_suite.addTest(unittest.makeSuite(TestDatabaseExtraction))

report_file = 'test_report.html'

# Create a test runner with HTMLTestRunner
test_runner = HtmlTestRunner.HTMLTestRunner(output=report_file)

# Run the Test Suite using the test runner
test_runner.run(test_suite)
# # Run the Test Suite
# test_runner = unittest.TextTestRunner()
# test_runner.run(test_suite)
