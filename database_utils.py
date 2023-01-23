import yaml 
from sqlalchemy import create_engine 


class DatabaseConnector:

    def read_database_credentials(self):
        # Read the yaml file 
        with open('db_creds.yaml') as file:
            database_credentials = yaml.safe_load(file)
            # print(database_credentials)

        # Return the yaml file as a dictionary 
        return dict(database_credentials)



    
    def initialise_database_connection(self):

        # Call the database details method to use the dictionary as an output 
        database_dictionary = self.read_database_credentials()

        # Initialise an empty list representing the connection string needed for create engine

        connection_list = []

        # Append each of the values of the dictionary to the list 
        for value in database_dictionary.values():
            connection_list.append(value)


        # Insert the following characters at these points inside the list 
        connection_list.insert(1,"+")
        connection_list.insert(3,"://")
        connection_list.insert(5, ":")
        connection_list.insert(7,"@")
        connection_list.insert(9, ':')
        connection_list.insert(11,"/")

        # print(connection_list)

        # Use a list comprehension to cast each of the elements to a string before joining them together 

        connection_string = "".join(str(e) for e in connection_list)

        # Expected output 

        # postgresql+psycopg2://aicore_admin:AiCore2022@data-handling-project-readonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com:5432/postgres

        print(connection_string)

        # Lastly try to connect to the database using your connection string variable 
        try:
            database_engine = create_engine(connection_string)
            database_engine.connect() 
            print("connection successful")
        except:
            print('There was an error')
            raise Exception 

    def upload_to_db(self, dataframe, table_name):
        pass

if __name__ == "__main__":
    new_database = DatabaseConnector()
    new_database.initialise_database_connection()