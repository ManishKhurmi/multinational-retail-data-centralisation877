# %%
import yaml 
import sqlalchemy
from sqlalchemy import create_engine, inspect, text  
import pandas as pd


class DatabaseConnector:
    ''' connects with the database to upload the data'''
    # task 3 step 2 
    @staticmethod
    def read_db_creds(file_path):
        """Reads the database credentials from a YAML file and returns them as a dictionary."""
        try:
            with open(file_path, 'r') as file:
                creds = yaml.safe_load(file)
            return creds
        except Exception as e:
            print("Error reading the credentials file:", e)
            return None
        
    @staticmethod
    def local_init_engine(creds):
        """Initializes the SQLAlchemy engine using provided database credentials."""
        return create_engine(f"{creds['DATABASE_TYPE']}+{creds['DBAPI']}://{creds['USER']}:{creds['PASSWORD']}@{creds['HOST']}:{creds['PORT']}/{creds['DATABASE']}")
        
    # task 3 step 3     
    @staticmethod
    def init_db_engine(creds):
        """Initializes and returns an SQLAlchemy database engine using credentials from the YAML file."""
        if creds: # incase credentials yaml file is empty
            # Create the database URL for SQLAlchemy
            db_url = f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}"
            
            # Initialize the SQLAlchemy engine
            engine = create_engine(db_url)
            return engine
        else:
            print("Failed to initialize database engine due to missing or invalid credentials.")
            return None
        
    @staticmethod
    def list_db_tables(engine):
        '''lists all tables in the database'''
        inspector = inspect(engine)
        return print(inspector.get_table_names())
    
    @staticmethod
    def upload_to_db(df, table_name: str, engine):
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        
if __name__ == '__main__':
    #### Local Postgres DB ####

    print('Step 1: Initialise connection to Local DB \n')
    local_db_connect = DatabaseConnector()

    print('Step 2: Read Local DB credentials \n')
    local_raw_creds = local_db_connect.read_db_creds('db_creds_raw.yaml')
    # local_raw_creds = local_db_connect.read_db_creds('db_creds_cleaned.yaml')
    print("Credentials: \n")
    print(local_raw_creds)  # This should print out the dictionary of credentials

    print('Step 3: Create Engine \n')
    local_engine = local_db_connect.local_init_engine(local_raw_creds)
    print('Engine: \n')
    print(local_engine)

    print('Step 4: List Tables in Local DB \n')
    local_db_connect.list_db_tables(local_engine)
    print('END')

    # #### Upload DF to Local DB####
    # print('Upload DF to Local DB \n')
    # df_legacy_users = pd.read_csv('legacy_users.csv')
    # df_card_details = pd.read_csv('card_details.csv')
    # local_db_connect.upload_to_db(df_legacy_users, table_name='dim_users', engine=local_engine)
    # local_db_connect.upload_to_db(df_card_details, table_name='dim_card_details', engine=local_engine)
    
    # print('List of Tables in Local DB: \n') 
    # local_db_connect.list_db_tables(local_engine)

# %%
