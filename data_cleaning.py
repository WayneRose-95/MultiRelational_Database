from data_extraction import DatabaseExtractor
from database_utils import DatabaseConnector
from sqlalchemy import create_engine
import pandas as pd 
import yaml

class DataCleaning: 

    def __init__(self):
        with open("sales_data_creds.yaml") as file:
            creds = yaml.safe_load(file)
            DATABASE_TYPE = creds['DATABASE_TYPE']
            DBAPI = creds['DBAPI']
            RDS_USER = creds['RDS_USER']
            RDS_PASSWORD = creds['RDS_PASSWORD']
            RDS_HOST = creds['RDS_HOST']
            RDS_PORT = creds['RDS_PORT']
            DATABASE = creds['DATABASE']

        try:    
            self.engine = create_engine(
                 f"{DATABASE_TYPE}+{DBAPI}://{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}:{RDS_PORT}/{DATABASE}"
            )
            print("connection successful")
        except:
            print("There was an error")
            raise Exception    

    def clean_user_data(self):
        '''
        Method to clean the user data table 

        Parameters:
        None 

        Returns: 
        legacy_users_dataframe: Dataframe 
        Pandas dataframe used to upload to the database. 
        '''
        # Instantiating an instance of the Database Extractor class 
        extractor = DatabaseExtractor()
        # Reading in the table from the AWS database 
        legacy_users_dataframe = extractor.read_rds_table("legacy_users")
        # Naming the columns in the dataframe 
        legacy_users_dataframe.columns = [
                'user_key', 
                'first_name', 
                'last_name', 
                'birth_date', 
                'company', 
                'e-mail_address', 
                'address', 
                'country', 
                'country_index',
                'phone_number', 
                'join_date', 
                'unique_id'
            ] 
        legacy_users_dataframe["join_date"] = pd.to_datetime(legacy_users_dataframe['join_date'], errors='coerce').dt.strftime('%d/%m/%Y')
        legacy_users_dataframe["birth_date"] = pd.to_datetime(legacy_users_dataframe['birth_date'], errors='coerce').dt.strftime('%d/%m/%Y')
        
        # Renaming the columns as appropriate data types 
        legacy_users_dataframe = legacy_users_dataframe.astype(
            {
                "first_name": "string",
                "last_name": "string",
                "company": "string",
                "e-mail_address": "string",
                "address": "string", 
                "country": "string",
                "country_index": "string", 
                "phone_number": "string"
            }
        )
        # Drop all columns with nulls in their dates from the birth and join date columns 
        legacy_users_dataframe = legacy_users_dataframe.dropna(subset=['birth_date', 'join_date'])

        # Reset the index if desired
        legacy_users_dataframe = legacy_users_dataframe.reset_index(drop=True)
        legacy_users_dataframe['user_key'] = legacy_users_dataframe.index 
        upload = DatabaseConnector() 
        try:
            upload.upload_to_db(legacy_users_dataframe, self.engine, 'dim_users')
            print(f"Table uploaded")
        except: 
            print("there was an error")
            raise Exception 
        return legacy_users_dataframe
    
    def clean_store_data(self):
        # Instantiating an instance of the Database Extractor class 
        extractor = DatabaseExtractor()
        # Reading in the table from the AWS database 
        legacy_store_dataframe = extractor.read_rds_table("legacy_store_details")
        
        # State the column names for the table 
        legacy_store_dataframe.columns = [
            'store_key',
            'store_address', 
            'longitude', 
            'null_column', 
            'city', 
            'store_code', 
            'store_number', 
            'opening_date', 
            'store_type', 
            'latitude',
            'country_code', 
            'region'
        
        ]
        
        # Drop the null_column column within the table 
        legacy_store_dataframe = legacy_store_dataframe.drop('null_column', axis=1)

        # Reorder the column order 
        column_order = [
            'store_key',
            'store_address',
            'longitude',
            'latitude',
            'city',
            'store_code',
            'store_number',
            'opening_date',
            'store_type',
            'country_code',
            'region'
        ]

        # remake the dataframe with the column order in place 
        legacy_store_dataframe = legacy_store_dataframe[column_order]

        # Change the date format of the opening_date column to dd/mm/yyyy 
        legacy_store_dataframe["opening_date"] = pd.to_datetime(legacy_store_dataframe['opening_date'], errors='coerce').dt.strftime('%d/%m/%Y')

        # Drop dates in the opening_date which are null 
        legacy_store_dataframe = legacy_store_dataframe.dropna(subset=['opening_date'])

        # Reset the index if desired
        legacy_store_dataframe = legacy_store_dataframe.reset_index(drop=True)
        
        # Set the store_key column as the index column to reallign it with the index column 
        legacy_store_dataframe['store_key'] = legacy_store_dataframe.index 

        # Lastly, drop the store_number column
        legacy_store_dataframe = legacy_store_dataframe.drop("store_number", axis=1)

        legacy_store_dataframe.replace('eeEurope', 'Europe')
        legacy_store_dataframe.replace('eeAmerica', 'America')

        upload = DatabaseConnector() 
        try:
            upload.upload_to_db(legacy_store_dataframe, self.engine, 'dim_store_details')
            print(f"Table uploaded")
        except: 
            print("there was an error")
            raise Exception 
        return legacy_store_dataframe
        

if __name__=="__main__":
    cleaner = DataCleaning()
    # cleaner.clean_user_data()
    cleaner.clean_store_data() 
 
