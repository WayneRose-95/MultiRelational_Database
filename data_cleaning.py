from data_extraction import DatabaseExtractor
from database_utils import DatabaseConnector
from sqlalchemy import create_engine
import pandas as pd 
import tabula
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

        #TODO: Fix the bug where the replace function is not replacing the correct region
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
        
    def clean_card_details(self, link_to_pdf : str):
        # Instantiating an instance of the Database Extractor class
        extractor = DatabaseExtractor()

        # Read in the pdf data for the card details 
        card_details_table = extractor.retrieve_pdf_data(link_to_pdf)

        # Convert the date_payment_confirmed column into a datetime 
        card_details_table["date_payment_confirmed"] = pd.to_datetime(card_details_table['date_payment_confirmed'], errors='coerce').dt.strftime('%d/%m/%Y')

        # For any null values, drop them. 
        card_details_table = card_details_table.dropna(subset=['date_payment_confirmed'])

        # Add a new column called card_key, which is the length of the card_details table 
        card_details_table['card_key'] = (range(len(card_details_table)))

        # Rearrange the order of the columns 
        column_order = [
            'card_key',
            'card_number',
            'expiry_date',
            'card_provider',
            'date_payment_confirmed'
        ]

        # Set the order of the columns in the table 
        card_details_table = card_details_table[column_order]

        # Lastly, try to upload the table to the database. 
        upload = DatabaseConnector() 
        try:
            upload.upload_to_db(card_details_table, self.engine, 'dim_card_details')
            print(f"Table uploaded")
        except: 
            print("there was an error")
            raise Exception 

        pass

    def clean_orders_table(self):
        # Instantiating an instance of the Database Extractor class
        extractor = DatabaseExtractor()

        # Read in the table from the RDS database 
        orders_dataframe = extractor.read_rds_table("orders_table")

        # State the names of the columns 
        orders_dataframe.columns = [
            'order_key',
            'null_key',
            'date_uuid',
            'first_name',
            'last_name',
            'user_uuid',
            'card_number',
            'store_code',
            'product_code',
            'null_column',
            'product_quantity'
            
        ]

        # Drop the following columns within the dataframe if they exist 
        orders_dataframe.drop(["null_key", "first_name", "last_name", "null_column"], axis=1, inplace=True)

        # Lastly, try to upload the cleaned table to the database 
        upload = DatabaseConnector() 
        try:
            upload.upload_to_db(orders_dataframe, self.engine, 'orders_table')
            print(f"Table uploaded")
        except: 
            print("there was an error")
            raise Exception 

    def clean_time_event_table(self):
        # Instantiate an instance of the DatabaseExtractor() class 
        extractor = DatabaseExtractor()

        # Read in the json data from the s3 bucket 
        time_df = extractor.read_json_from_s3("https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json")

        # Filter out non-numeric values and convert the column to numeric using boolean mask 
        numeric_mask = time_df['month'].apply(pd.to_numeric, errors='coerce').notna()
        # copy the dataframe then convert the month column to a numeric datatype 
        time_df = time_df[numeric_mask].copy()
        time_df['month'] = pd.to_numeric(time_df['month'])
        # Afterwards, drop any values which are not null or not numeric 
        time_df = time_df.dropna(subset=['month'])

        #TODO: Write a piece of code which verifies that the number of rows in the orders_table 
        # is equal to the number of rows in the dim_date_times table 
        # Currently, the operation drops 38 rows 
        # 120161 - 120123 = 38 
        # Correct number because the orders table has 120123 rows, so we have a time event per order.

        upload = DatabaseConnector() 
        try:
            upload.upload_to_db(time_df, self.engine, 'dim_date_times')
            print(f"Table uploaded")
        except: 
            print("there was an error")
            raise Exception 

    def clean_product_table(self):
        extraction = DatabaseExtractor() 
        products_table = extraction.read_s3_bucket_to_dataframe("s3://data-handling-public/products.csv")
        values = list(products_table["removed"].unique())
        print(values)
        products_table = products_table[~products_table['removed'].isin(values[-3:])]
        products_table["date_added"] = pd.to_datetime(products_table['date_added'], errors='coerce')
        products_table['product_price'] = products_table['product_price'].str.replace('£', '')
        products_table['weight'] = products_table['weight'].apply(self.convert_to_kg)
        products_table = products_table.dropna(subset=["weight"])
        products_table["product_key"] = range(len(products_table))
        products_table = products_table.drop("Unnamed: 0", axis=1)

        column_order = [
    
            "product_key",
            "EAN",
            "product_name", 
            "product_price",
            "weight",
            "category",
            "date_added",
            "uuid",
            "removed",
            "product_code"
            
        ]

        products_table = products_table[column_order]

        upload = DatabaseConnector() 
        try:
            upload.upload_to_db(products_table, self.engine, 'dim_product_details')
            print(f"Table uploaded")
        except: 
            print("there was an error")
            raise Exception 

    
    def convert_to_kg(self, weight):
        if isinstance(weight, float):
            return weight  # Return the original float value if weight is already a float
        
        clean_weight = str(weight)
        # Removes punctuation from string
    
        
        if 'kg' in weight:
            return float(clean_weight.strip('kg'))
        elif ' x ' in weight:
            value, unit = clean_weight.split(' x ')
            combined_value = float(value) * float(unit[:-1])
            return combined_value
        elif 'g' in clean_weight or 'ml' in clean_weight:
            try:
                clean_weight = clean_weight.replace('.', " ")
                clean_weight = clean_weight.strip('g').strip('ml')
                return float(clean_weight) / 1000
            except:
                modified_weight = clean_weight.strip('.').replace(" ", "").strip('g').strip('ml')
                return float(modified_weight) / 1000
        elif 'oz' in clean_weight:
            clean_weight = clean_weight.strip('oz')
            return round(float(clean_weight) * 0.028349523, 2)
        else:
            return None
        
if __name__=="__main__":
    cleaner = DataCleaning()
    # cleaner.clean_user_data()
    # cleaner.clean_store_data()
    # cleaner.clean_card_details(
    #     "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
    # ) 
    # cleaner.clean_orders_table() 
    # cleaner.clean_time_event_table()
    cleaner.clean_product_table() 
 
