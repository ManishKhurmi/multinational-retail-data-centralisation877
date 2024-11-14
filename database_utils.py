import yaml 
import sqlalchemy
from sqlalchemy import create_engine
import pandas as pd

# Step 2:
# Create a method read_db_creds this will read the credentials yaml file and return a dictionary of the credentials.
# You will need to pip install PyYAML and import yaml to do this.

class DatabaseConnector:
    ''' connects with the database to upload the data'''
    # step 2 
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


# Example usage
db_connect = DatabaseConnector()
creds = db_connect.read_db_creds('db_creds.yaml')
print("Credentials: \n")
print(creds)  # This should print out the dictionary of credentials
engine = db_connect.init_db_engine(creds)
print('Engine: \n')
print(engine)

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



