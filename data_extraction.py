from database_utils import DatabaseConnector 
from sqlalchemy import inspect
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




    def read_rds_table(self, table_name):
        database_connection = DatabaseConnector()

        # Initialise the connection 
        engine = database_connection.initialise_database_connection()

        table_query = engine.execute(f"""SELECT * FROM {table_name}""").fetchall()
        dataframe = pd.DataFrame(table_query)

        with pd.option_context('display.max_rows', None,
                       'display.max_columns', None,
                       'display.precision', 3,
                       ):
            

            return dataframe 
       
        
    
if __name__ == "__main__":
    extract = DatabaseExtractor() 
    extract.read_rds_table('legacy_users')

            

     
     
    

