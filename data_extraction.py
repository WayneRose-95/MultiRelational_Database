from database_utils import DatabaseConnector 
from sqlalchemy import inspect


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




    def read_rds_table(self):
        pass

    def clean_user_data(self):
        pass

    
if __name__ == "__main__":
    extract = DatabaseExtractor() 
    extract.list_db_tables()

            

     
     
    

