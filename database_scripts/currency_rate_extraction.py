from selenium.webdriver import Chrome
from selenium.webdriver import ChromeOptions
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import NoSuchElementException
from database_scripts.file_handler import get_absolute_file_path

# from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.chrome.service import Service
import os
import logging
import pandas as pd


"""
LOG DEFINITION
"""

log_filename = get_absolute_file_path(
    "currency_rate_extractor.log", "logs"
)  # "logs/currency_rate_extractor.log"
if not os.path.exists(log_filename):
    os.makedirs(os.path.dirname(log_filename), exist_ok=True)

currency_rate_logger = logging.getLogger(__name__)

# Set the default level as DEBUG
currency_rate_logger.setLevel(logging.DEBUG)

# Format the logs by time, filename, function_name, level_name and the message
format = logging.Formatter(
    "%(asctime)s:%(filename)s:%(funcName)s:%(levelname)s:%(message)s"
)
file_handler = logging.FileHandler(log_filename)

# Set the formatter to the variable format

file_handler.setFormatter(format)

currency_rate_logger.addHandler(file_handler)


class CurrencyRateExtractor:
    def __init__(self):

        chrome_options = ChromeOptions()

        # chrome_options.add_argument(generate_user_agent())
        # caps = DesiredCapabilities().CHROME
        # caps["pageLoadStrategy"] = "normal"
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--remote-debugging-port=9222")
        self.driver = Chrome(
            service=Service(ChromeDriverManager().install()), options=chrome_options
        )  #

    def land_first_url(self, page_url: str):
        """
        Method to get the url of the website

        Parameters:
        page_url : str
        The url of the webpage

        Returns an exception if the url is invalid.
        """

        try:
            initial_webpage = self.driver.get(page_url)
            currency_rate_logger.info("Successfully landed first page")
            currency_rate_logger.debug(page_url)
        except:
            currency_rate_logger.exception("Error accessing first page")

    def extract_timestamp(self, timestamp_xpath: str):
        """
        Method to extract the timestamp from the website

        Parameters:
        timestamp_xpath : str
        The xpath representing the timestamp web element

        Returns:
        timestamp : str
        The timestamp extracted from the website as a string
        """
        currency_rate_logger.debug("Extracting timestamp from website")
        timestamp = self.driver.find_element(By.XPATH, timestamp_xpath).text
        currency_rate_logger.info(f"Timestamp extracted : {timestamp}")
        return timestamp

    def extract_table(self, table_body_xpath: str):
        """
        Method to extract the table data from the website using an xpath

        Parameters:
        table_body_xpath : str
        The xpath representing the table on the website

        Returns data : list
        A list of lists containing the extracted table data
        """
        data = []
        try:
            currency_rate_logger.debug("Extracting data table from website")
            table_container = self.driver.find_element(By.XPATH, table_body_xpath)
            currency_rate_logger.debug(table_container)
            print(table_container)

            for tr_tag in table_container.find_elements(By.XPATH, "//tr"):
                row = [item.text for item in tr_tag.find_elements(By.XPATH, ".//td")]
                data.append(row)

            print(data)
            print(len(data))
            currency_rate_logger.info("Data table extracted")
            currency_rate_logger.debug(f"Number of rows : {len(data)}")
            return data

        except:
            currency_rate_logger.warning("No data found. Returning empty list")
            return []

    def data_to_dataframe(self, data: list or dict, headers: list, file_name: str):
        """
        Method to convert the data extracted from the website
        to a pandas DataFrame.

        Parameters:
        data : list or dict
        The data extracted from the webiste

        headers : list
        The list of headers for the pandas DataFrame

        file_name : str
        The name of the file to be exported to .csv
        """
        currency_rate_logger.info
        raw_data = pd.DataFrame(data, columns=headers)
        raw_data.to_csv(f"{file_name}.csv", columns=headers)
        currency_rate_logger.info(f"{file_name}.csv successfully exported")
        print(f"{file_name}.csv successfully exported")
        return raw_data

    def scrape_information(
        self,
        page_url: str,
        table_body_xpath: str,
        timestamp_xpath: str,
        data_headers: list,
        file_name: str,
    ):
        """
        Method to combine all other methods to scrape information from the website.

        Parameters:
        page_url : str
        The url to the webpage

        table_body_xpath : str
        The xpath for the table on the website

        timestamp_xpath : str
        The xpath for the timestamp on the website

        data_headers : list
        The list of headers for the DataFrame

        file_name : str
        The name of the file exported to .csv

        Returns:
        dataframe, timestamp
        A tuple containing the dataframe created from the data,
        and the timestampe extracted from the website.

        """

        currency_rate_logger.info(f"Attempting to land {page_url}")
        self.land_first_url(page_url)

        currency_rate_logger.info(f"Attempting to extract table from {page_url}")
        data = self.extract_table(table_body_xpath)

        currency_rate_logger.info("Extracting timestamp")
        timestamp = self.extract_timestamp(timestamp_xpath)

        currency_rate_logger.info("Converting data extracted to DataFrame")
        dataframe = self.data_to_dataframe(data, data_headers, file_name)
        currency_rate_logger.info(f"Table scraped. Please check {file_name}.csv file")

        print(f"Table scraped. Please check {file_name}.csv file")
        self.driver.quit()
        return dataframe, timestamp


if __name__ == "__main__":
    currency_conversions_file = get_absolute_file_path(
        "currency_conversions", "source_data_files"
    )
    sample_extractor = CurrencyRateExtractor()
    dataframe, timestamp = sample_extractor.scrape_information(
        "https://www.x-rates.com/table/?from=GBP&amount=1",
        '//table[@class="tablesorter ratesTable"]/tbody',
        '//*[@id="content"]/div[1]/div/div[1]/div[1]/span[2]',
        ["currency_name", "conversion_rate", "conversion_rate_percentage"],
        currency_conversions_file,
    )
