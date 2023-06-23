from database_utils import DatabaseConnector
from sqlalchemy import create_engine
from sqlalchemy import text 
from sqlalchemy.orm import sessionmaker
from logger import DatabaseLogger

sql_transformations_logger = DatabaseLogger("logs/sql_transformations.log")
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

   



if __name__ == "__main__":
    sql_statements = SQLAlterations('sales_data_creds_dev.yaml')
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
    # Lastly map the dimension keys in the dim tables to the foreign keys in the orders_table 
    sql_statements.alter_and_update(r'sales_data\DML\update_orders_table_foreign_keys.sql')







        
