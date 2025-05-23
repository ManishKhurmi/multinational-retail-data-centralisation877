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
    

# ###########################################################################################

if __name__ == '__main__':

    ### no longer needed as we are only using the local DB ###
    # print('Step 1: Initialise connection to AWS RDS DataBase \n')
    # aws_db_connect = DatabaseConnector()

    # print('Step 2: Read Credentials from YAML file \n')
    # aws_creds = aws_db_connect.read_db_creds('db_creds.yaml')
    # print("Credentials: \n")
    # print(aws_creds)  # This should print out the dictionary of credentials

    # print('Step 3: Create Engine \n')
    # aws_engine = aws_db_connect.init_db_engine(aws_creds)
    # print('Engine: \n')
    # print(aws_engine)

    # print('Step 4: List Tables in AWS RDS Data Base \n')
    # aws_db_connect.list_db_tables(aws_engine)
    # print('\n END \n')
    # print('#'*100)

    #### Local Postgres DB ####

    print('Step 1: Initialise connection to Local DB \n')
    local_db_connect = DatabaseConnector()

    print('Step 2: Read Local DB credentials \n')
    # local_creds = local_db_connect.read_db_creds('local_db_creds.yaml')
    local_creds = local_db_connect.read_db_creds('local_db_creds.yaml')
    print("Credentials: \n")
    print(local_creds)  # This should print out the dictionary of credentials

    print('Step 3: Create Engine \n')
    local_engine = local_db_connect.local_init_engine(local_creds)
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

###########################################################################################




# %%
# query = text(f"CREATE TABLE dim_users")
# with engine.connect() as connection:
#     connection.execute(query)

# db_connect.list_db_tables(engine)

# Manish: Task 3, the number of rows to be cleaned are too less and need to come back steps 7 & 8 
# %% Testing upload_to_db
# test_data = {
#     'user_id': [1, 2, 3],
#     'user_name': ['Alice', 'Bob', 'Charlie'],
# }
# test_df = pd.DataFrame(test_data)

# # uppload a test df 
# test_df.to_sql('test_table', con=engine, if_exists='fail', index=False)

# # Verify if the table was created
# inspector = inspect(engine)
# print('Tables in the DB:', inspector.get_table_names())


# Task 3 step 4 
# Using the engine from init_db_engine create a method list_db_tables to list all the tables in the database so you know which tables you can extract data from.
# Develop a method inside your DataExtractor class to read the data from the RDS database.

# from sqlalchemy import inspect 
# inspector = inspect(engine)
# print('/n Table Names:')
# print(inspector.get_table_names())


# def list_db_tables(engine):
#     '''lists all tables in the database'''
#     inspector = inspect(engine)
#     return print(inspector.get_table_names())

# # Test 
# print('/nTesting')
# list_db_tables(engine)

# Test Class

# @staticmethod
# def read_rds_table(engine, table_name = ['legacy_store_details', 'dim_card_details', 'legacy_users', 'orders_table']):
#     query = text(f"SELECT * FROM {table_name}")
#     with engine.connect() as connection:
#         result = connection.execute(query)
#         # convert the result into a pandas df 
#         df = pd.DataFrame(result)
#         # print(f"(\nFirst 5 rows df: \n{df.head()})")
#         return df 

# def upload_to_db(engine):


# Scratch code 
# engine.execution_options(isolation_level='AUTOCOMMIT').connect()
# engine.connect()

# from sqlalchemy import inspect
# inspector = inspect(engine)
# inspector.get_table_names()

# Step 3:
# Now create a method init_db_engine which will read the credentials from the return of read_db_creds and initialise and return an sqlalchemy database engine.
# engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")

# Test using SQLaclchemy 

# DATABASE_TYPE = 'postgresql'
# DBAPI = 'psycopg2'
# HOST = 'localhost'
# USER = 'postgres'
# PASSWORD = 'ginger'
# DATABASE = 'sales_data'
# PORT = 5432
# engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
# engine.execution_options(isolation_level='AUTOCOMMIT').connect()

# connect to Ai core data 
# DATABASE_TYPE = 'postgresql'
# RDS_HOST = 'data-handling-project-readonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com'
# RDS_PASSWORD = 'AiCore2022'
# RDS_USER = 'aicore_admin'
# RDS_DATABASE = 'postgres'
# RDS_PORT = 5432
# DBAPI = 'psycopg2'

# engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{RDS_USER }:{RDS_PASSWORD}@{RDS_HOST}:{RDS_PORT}/{RDS_DATABASE}")
# print(engine)
# engine.execution_options(isolation_level='AUTOCOMMIT').connect()

# creds = read_db_creds(file_path='db_creds.yaml')
# print(creds)
# print(init_db_engine(creds))

# # Usage example
# engine = init_db_engine()
# if engine:
#     print("Database engine initialized successfully!")
# else:
#     print("Database engine initialization failed.")



# def init_db_engine(creds):
#     engine = create_engine(f"{creds}")
#     engine.execution_options(isolation_level="AUTOCOMMIT").connect()
#     return engine 

# Testing 
# with open('db_creds.yaml', 'r') as file:
#     creds = yaml.safe_load(file)

# print(creds)

# try:
#     with open('prime.yaml', 'r') as file:
#         prime_service = yaml.safe_load(file)
# except Exception as e:
#     print("Error reading prime file:", e)
# print(prime_service)
# print(prime_service['prime_numbers'][0])
# print(prime_service['rest']['url'])

# # connect to sales_data
# import psycopg2
# with psycopg2.connect(host='localhost', user='postgres', password='ginger', dbname='sales_data', port=5432) as conn:
#     with conn.cursor() as cur:
#         cur.execute("""SELECT table_name FROM information_schema.tables
#        WHERE table_schema = 'public'""")
#         for table in cur.fetchall():
#             print(table)




# %%
