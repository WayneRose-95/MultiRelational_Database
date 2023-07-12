from sqlalchemy import create_engine
from sqlalchemy import text 

# Connection parameters
host = 'localhost'
port = '5432'
username = 'Sales_Data_Admin'
password = 'password'
new_db_name = "Test_Database"

# Create the database URL without specifying the database name
db_url = f'postgresql+psycopg2://{username}:{password}@{host}:{port}/postgres'
# f"postgresql+psycopg2://{database_credentials['RDS_USER']}:{database_credentials['RDS_PASSWORD']}@{database_credentials['RDS_HOST']}:{database_credentials['RDS_PORT']}"

# Create the engine and connect to the server
engine = create_engine(db_url)
connection = engine.connect()

if connection: 
    print("Connection Successful")

# Disable transactional behavior
connection.execution_options(isolation_level="AUTOCOMMIT")

# Create a new database
create_db_stmt = text(f'CREATE DATABASE {new_db_name}')
connection.execute(create_db_stmt)

# Close the connection
connection.close()

# text()