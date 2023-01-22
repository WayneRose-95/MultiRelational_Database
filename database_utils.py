import yaml 
from sqlalchemy import create_engine

class DataExtractor: 

    def read_db_creds(self):
        try:
            with open('db_creds.yaml') as file:
                creds = yaml.safe_load(file)
                DATABASE_TYPE = creds["DATABASE_TYPE"]
                RDS_HOST = creds["RDS_HOST"]
                RDS_PASSWORD = creds["RDS_PASSWORD"]
                DBAPI = creds['DBAPI']
                RDS_USER = creds["RDS_USER"]
                RDS_DATABASE = creds["RDS_DATABASE"]
                RDS_PORT = creds["RDS_PORT"]

            self.engine = create_engine(
                f"{DATABASE_TYPE}+{DBAPI}://{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}:{RDS_PORT}/{RDS_DATABASE}"
            )
            print('Connection Successful')
        except:
            print('There was an error')
            raise Exception

if __name__ == "__main__":
    new_database = DataExtractor()
    new_database.read_db_creds()