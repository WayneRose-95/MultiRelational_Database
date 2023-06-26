from database_utils import DatabaseConnector
from sqlalchemy import create_engine
from sqlalchemy import text 
from sqlalchemy.orm import sessionmaker
import logging
import os 

log_filename = "logs/sql_transformations.log"
if not os.path.exists(log_filename):
    os.makedirs(os.path.dirname(log_filename), exist_ok=True)

sql_transformations_logger = logging.getLogger(__name__)

# Set the default level as DEBUG
sql_transformations_logger.setLevel(logging.DEBUG)

# Format the logs by time, filename, function_name, level_name and the message
format = logging.Formatter(
    "%(asctime)s:%(filename)s:%(funcName)s:%(levelname)s:%(message)s"
)
file_handler = logging.FileHandler(log_filename)

# Set the formatter to the variable format

file_handler.setFormatter(format)

sql_transformations_logger.addHandler(file_handler)


class SQLAlterations: 

    def __init__(self, datastore_config_file : str):
        # Create an instance of the DatabaseConnetor class
        self.connector = DatabaseConnector()
        # Intialise the connection
        self.engine = self.connector.initialise_database_connection(datastore_config_file)

    def connect_to_database(self):
        sql_transformations_logger.debug(f"Connecting to datastore using {self.engine}")
        # Connect to the database using the Engine returned from the method 
        self.engine.connect() 

    def alter_and_update(self, sql_file_path : str):
        # Create a session object using sessionmaker 
        sql_session = sessionmaker(bind=self.engine)
        session = sql_session()

        try:
            with open(sql_file_path, 'r') as file:
                sql_statement = file.read()
                print(sql_statement)
            sql_transformations_logger.info(sql_statement)
            sql_transformations_logger.info("Executing and committing sql_statement")
            session.execute(text(sql_statement))
            session.commit()
            sql_transformations_logger.info('SQL statement submitted to database. Please verify.')
            print('SQL statement submitted to database. Please verify.')
        except:
            sql_transformations_logger.exception(f'Error when running sql_statement. The sql statement submitted was {sql_statement}')
            print(
                f'Error when running sql_statement. The sql statement submitted was {sql_statement}'
            )
            raise Exception

        finally:
            sql_transformations_logger.info("Closing Session")
            session.close()

def perform_database_operations(target_datastore_config_file_name):
    sql_statements = SQLAlterations(target_datastore_config_file_name)
    sql_statements.connect_to_database()
    # TEST usage 
    # sql_statements.alter_and_update(r'MultiRelational_Database\sales_data\DDL\alter_dim_card_details_table_schema.sql') 
    # Alter the table_schema of every table except dim_product_details 
    sql_statements.alter_and_update(r'sales_data\DDL\alter_table_schema.sql') 
    # Add in the necessary columns 
    sql_statements.alter_and_update(r'sales_data\DDL\column_additions.sql') 
    # SQL script to create the logic for the weight_class column added previously. 
    sql_statements.alter_and_update(r'sales_data\DML\add_weight_class_column_script.sql')
    # Alter the schema of the dim_product_details table 
    sql_statements.alter_and_update(r'sales_data\DDL\alter_dim_product_details_table_schema.sql') 
    # Next, add the primary keys to the tables 
    sql_statements.alter_and_update(r'sales_data\DDL\add_primary_keys.sql')
    # Then add the foreign key constraints to the orders_table 
    sql_statements.alter_and_update(r'sales_data\DDL\orders_table_FK_constraints.sql')
    # Map the dimension keys in the dim tables to the foreign keys in the orders_table 
    sql_statements.alter_and_update(r'sales_data\DML\update_orders_table_foreign_keys.sql')
    # Update the foreign key constraints in the dim_currency table with the dim-currency_conversion table
    sql_statements.alter_and_update(r'sales_data\DDL\dim_currency_FK_constraint.sql')
    # Update the foreign keys in the dim_currency table with the primary keys from the dim_currency_conversion_table
    sql_statements.alter_and_update(r'sales_data\DML\update_dim_currency_table_foreign_keys.sql')




if __name__ == "__main__":
    perform_database_operations('sales_data_creds_test.yaml')







        
