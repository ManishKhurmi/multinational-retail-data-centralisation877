import yaml 
import sqlalchemy
from sqlalchemy import create_engine, inspect, text  
import pandas as pd
from database_utils import DatabaseConnector


class DataExtractor:
    ''' 
    This is a utility class. 
    It houses methods that help extract data from different sources
    Data Sources include: CSV files, APIs & AWS S3 buckets.
    '''
    # def __init__(self):
        # self.csv_file
    
    @staticmethod
    def read_rds_table(engine, table_name = ['legacy_store_details', 'dim_card_details', 'legacy_users', 'orders_table']):
        query = text(f"SELECT * FROM {table_name}")
        with engine.connect() as connection:
            result = connection.execute(query)
            # convert the result into a pandas df 
            df = pd.DataFrame(result)
            # print(f"(\nFirst 5 rows df: \n{df.head()})")
            return df 
        
    
    
# connect to database 
db_connect = DatabaseConnector()
creds = db_connect.read_db_creds('db_creds.yaml')
print("Credentials: \n")
print(creds)  # This should print out the dictionary of credentials
print('#'*200)

# create engine 
engine = db_connect.init_db_engine(creds)
print('Engine: \n')
print(engine)
print('#'*200)

# view tables in data base 
print('List of Tables in Database: ')
db_connect.list_db_tables(engine)
print('#'*200)

# read data from one of the tables 
extract = DataExtractor()
df = extract.read_rds_table(engine, table_name='legacy_store_details')
print(df.head())



# def read_rds_table(engine, table_name):
#     query = text(f"SELECT * FROM {table_name}")
#     with engine.connect() as connection:
#         result = connection.execute(query)
#         # convert the result into a pandas df 
#         df = pd.DataFrame(result)
#         # print(f"(\nFirst 5 rows df: \n{df.head()})")
#         return df 

# df = read_rds_table(engine, table_name='legacy_users')
# print(df)

# # testing 
# extract = DataExtractor()
# extract.read_rds_table(engine)

# read_rds_table(engine)

