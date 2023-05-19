from data_extraction import DatabaseExtractor
import pandas as pd 

class DataCleaning: 

    def __init__(self):
        pass

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

        return legacy_users_dataframe
    

if __name__=="__main__":
    cleaner = DataCleaning()
    cleaner.clean_user_data() 
