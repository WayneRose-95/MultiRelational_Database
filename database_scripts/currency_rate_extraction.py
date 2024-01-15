import urllib.request
import pandas as pd
from datetime import datetime
from database_scripts.file_handler import get_absolute_file_path



class CurrencyExtractor:
    '''
    A class which extracts currency data from a url. 

    The website needs to have a table of currencies. 

    Attributes 

    url: The url of the website 

    Methods 

    read_html_data()
    html_to_dataframe()
    merge_dataframes()
    convert_columns() 
    save_data() 
    '''
    def __init__(self, url):
        self.url = url

    def read_html_data(self):
        '''
        Method to read in html data using urllib.request 

        Given a url to a website, the method will read the html code of said website 

        Returns: 

        html : _UrlopenRet 

        The html from the website 
        '''
        with urllib.request.urlopen(self.url) as webpage:
            html = webpage.read()

        return html

    def html_to_dataframe(self, html, index_number: int):
        '''
        Method to read in html data into a pandas dataframe. 

        The method uses pd.read_html() to read in html data into a dataframe 

        Parameters: 

        html : _UrlopenRet 

        The html data extracted from the website. 

        index_number : int 

        The index representing the html table extracted 

        Returns:

        html_datatable : pd.DataFrame 
        A dataframe containing html data 

        Example Usage:

        c = CurrencyExtractor(https://www.sampleurl.com)

        html_data = c.read_html_data()

        df = c.html_to_dataframe(html_data, 0)
        '''
        html_datatable = pd.read_html(html)[index_number]

        return html_datatable

    def merge_dataframes(
        self, join_clause: str, first_df: pd.DataFrame, second_df : pd.DataFrame
    ):
        '''
        Method to merge two dataframes 

        The method takes in two dataframes and a join_clause  

        The result is a merged dataframe object. 

        Parameters:

        join_clause : str 

        The join_clause used to join the two tables together. 

        Accepted Values: "left", "right", "inner", "cross", "outer"

        first_df : pd.DataFrame 

        The first dataframe to be used. 

        This is the lead table for the join clause 

        second_df : pd.DataFrame 

        The second dataframe to be used  

        '''
        combined_df = pd.merge(first_df, second_df, how=join_clause)

        return combined_df

    @staticmethod
    def convert_columns(dataframe: pd.DataFrame, column_names: list):
        '''
        Staticmethod to convert columns into set column names 

        Parameters: 

        dataframe : pd.DataFrame 

        The dataframe which contains the columns 

        column_names : list 

        The names of the columns 

        Returns: 

        column_names: list

        The names of the columns returned. 
        '''
        dataframe.columns = column_names

        return column_names

    def save_data(self, raw_data: pd.DataFrame, file_name: str, headers: list):
        '''
        Method to save the data. 

        Currently, only .csv files are supported 

        Parameters: 

        raw_data : pd.DataFrame 
        
        The dataframe to be saved into a .csv file 

        file_name : str 

        The name of the file to save 

        headers : list 
        
        The names of the column headers

        Returns: 

        None

        '''
        column_headers = self.convert_columns(dataframe=raw_data, column_names=headers)
        raw_data.to_csv(f"{file_name}.csv", columns=headers)
        print(f"{file_name}.csv successfully exported")


if __name__ == "__main__":
    file_path = get_absolute_file_path("currency_conversions_poc ", "source_data_files")
    c_ext = CurrencyExtractor("https://www.x-rates.com/table/?from=GBP&amount=1")
    html_data = c_ext.read_html_data()
    first_table = c_ext.html_to_dataframe(html_data, 0)
    second_table = c_ext.html_to_dataframe(html_data, 1)

    combined_table = c_ext.merge_dataframes("right", first_table, second_table)

    c_ext.save_data(
        combined_table,
        file_path,
        ["currency_name", "conversion_rate", "conversion_rate_percentage"],
    )
    print("Success?")
