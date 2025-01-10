# %%
# connect to db
import sqlalchemy
from sqlalchemy import create_engine
import pandas as pd



DATABASE_TYPE = 'postgresql'
DBAPI = 'psycopg2'
HOST = 'localhost'
USER = 'postgres'
PASSWORD = 'ginger'
DATABASE = 'sales_data'
PORT = 5432
engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
# should return no error 

engine.execution_options(isolation_level='AUTOCOMMIT').connect()
engine.connect()

# %%
# create test data and upload as a table to our LOCAL DB 
from sqlalchemy import inspect
inspector = inspect(engine)
inspector.get_table_names()

# # %%
test_data = {
    'user_id': [1, 2, 3],
    'user_name': ['Alice', 'Bob', 'Charlie'],
}
test_df = pd.DataFrame(test_data)

# uppload a test df 
test_df.to_sql('test_table', con=engine, if_exists='fail', index=False)
# Verify if the table was created
inspector = inspect(engine)
print('Tables in the DB:', inspector.get_table_names())


# %%
# Refactoring
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
        
    @staticmethod
    def list_db_tables(engine):
        '''lists all tables in the database'''
        inspector = inspect(engine)
        return print(inspector.get_table_names())
    

local_db_connect = DatabaseConnector()
# read credentials from the yaml file
creds = local_db_connect.read_db_creds('local_db_creds.yaml')
print("Credentials: \n")
print(creds)  # This should print out the dictionary of credentials

# create an engine to connect to the db
engine = local_db_connect.local_init_engine(creds)
print('Engine: \n')
print(engine)

# list tables in local db 
local_db_connect.list_db_tables(engine)

# %% 
creds['PASSWORD']



# %%
# file_path = 'local_db_creds.yaml'
# with open(file_path, 'r') as file:
#     creds = yaml.safe_load(file)
# print(creds)

# creat an engine to connect to the db
engine = db_connect.init_db_engine(creds)
print('Engine: \n')
print(engine)

# # List the tables in the database 
# db_connect.list_db_tables(engine)


# %%
