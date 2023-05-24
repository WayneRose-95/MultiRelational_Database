from database_utils import DatabaseConnector 
from sqlalchemy import inspect
from sqlalchemy import select
from sqlalchemy import Table
from sqlalchemy import MetaData
import pandas as pd 

class DatabaseExtractor:

    def __init__(self):
        pass

    def list_db_tables(self):
        # Create an instance of the DatabaseConnector Class 
        database_connection = DatabaseConnector()

        # Initialise the connection 
        engine = database_connection.initialise_database_connection()

        # Use the inspect method of sqlalchemy to get an inspector element 
        inspector = inspect(engine)

        # Get the table names using the get_table_names method 
        table_names = inspector.get_table_names()
        
        # print the table names to the console 
        print(table_names)

        # Output: ['legacy_store_details', 'legacy_users', 'orders_table']




    def read_rds_table(self, table_name :str):
        # Instantiate an instance of the DatabaseConnector class 
        database_connector = DatabaseConnector()

        # Initialise the connection 
        connection = database_connector.initialise_database_connection()

        # Connect to the database 
        connection = connection.connect() 

        # Initialise a MetaData object 
        metadata = MetaData() 

        # Set a user table object 
        user_table = Table(table_name, metadata, autoload_with=connection)

        # Show the table
        print(metadata.tables.keys())

        # Do a select statement to select all rows of the table 
        print(select(user_table))

        # Declare a select statement on the table to select all rows of the table
        select_statement = str(select(user_table))

        # Pass this select statement into a pandas function, which reads the sql query 
        dataframe_table = pd.read_sql(select_statement, con=connection)

        # Return the dataframe_table as an output of the method
        return dataframe_table
            
       
        
    
if __name__ == "__main__":
    extract = DatabaseExtractor() 
    extract.read_rds_table('legacy_users')

            

     
     
    

