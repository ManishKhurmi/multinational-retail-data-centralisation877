# %%
# Library
import yaml 
import sqlalchemy
from sqlalchemy import create_engine, inspect, text  
import pandas as pd
from database_utils import DatabaseConnector
from data_extraction import DataExtractor
import numpy as np 

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
df = extract.read_rds_table(engine, table_name='legacy_users')
print(df.head())


print('#' * 10)
print('#' * 10)
print('#' * 10)




# %%
class DataCleaning:
    ''' Clean data from each of the sources '''
    
    # def __init__(self):
    #     self = # data cleaning methods 

    @staticmethod
    def clean_user_data(df):

        print('#' * 100)
        print('Executing Step 1: Converting NULL Strings to NaN dtypes')
        # change the 'NULL" string to NULL data types 
        null_as_string_count_pre_filter = (df == 'NULL').sum().sum()
        print(f'Count of NULL as string pre filter {null_as_string_count_pre_filter}')
        df.replace('NULL', np.nan, inplace=True)
        null_as_string_count_post_filter = (df == 'NULL').sum().sum()
        print(f'Count of NULL as string pre filter {null_as_string_count_post_filter}')
        print('Completed Step 1')
        print('#' * 10)

        print('#' * 100)
        print('Executing Step 2: Removing Rows with NaN Values')
        # removing rows with NaN values 
        number_rows_pre_filter = df.shape[0] 
        df.dropna(how = 'any', inplace=True) # Here I've used 'any'  if any values in the row are 'NaN' the row will be dropped
        print(f'Number of rows removed {number_rows_pre_filter - df.shape[0]}')
        print(f'Shape of DF {df.shape}')
        print('Step 2 complete')
        print('#' * 10)

        print('#'* 100)
        print('Step 3: convert column join_date into date_time')
        print(f'join_date : /n {df['join_date'].info()}')
        df['join_date'] = pd.to_datetime(df['join_date'], errors='coerce')
        print(f'join date after transformation:/m {df['join_date'].info()}')
        print('#' * 10)
        print('Step 3: complete')
        print('#' * 10)


data_cleaning = DataCleaning()
data_cleaning.clean_user_data(df)

# %% 
df.info

# %%
class DataInformation:
    def __init__(self, df):
         self.df = df 

    def percentage_of_null(self):
        '''Prints Percentage of NULL pandas values'''
        percentage_of_null = self.df.isnull().sum() / len(self.df) * 100
        print(f'% of pandas NULL value types in DF: \n{percentage_of_null}')

    def null_as_string_count(self):
        null_as_string_count = (self.df == 'NULL').sum().sum()
        print(f'Number of NULL as strings in the DF: \n{null_as_string_count}') 

    def filter_rows_containing_null_as_string(self):
        '''Filters rows that have 'NULL' as a str() value'''
        rows_with_null = self.df[self.df.map(lambda x: x == 'NULL').any(axis =1)]
        print(f'Rows that contain NULL as a string type in DF: \n{rows_with_null}')
        # type(rows_with_null.iloc[0][0]) # proves that the 'NULL' is a string value 
     
# # Example usage 



# %% 
# Clean Card Data 
# read data 
df = pd.read_csv("card_data.csv")
df.head()

# # Card Details Cleaning Requirements:
# # - **Change "NULL" strings data type into NULL data type** --> 0 found 
# # - **Remove NULL values** 
# # - **Remove duplicate card numbers**
# # - **Remove non-numerical card numbers**
# # - **Convert "date_payment_confirmed" column into a datetime data type**

print(df.head())
info = DataInformation(df)
info.null_as_string_count() # 0 
info.filter_rows_containing_null_as_string() # 0 
info.percentage_of_null() # >0 


# MK : pick up here # # - **Remove NULL values** 
