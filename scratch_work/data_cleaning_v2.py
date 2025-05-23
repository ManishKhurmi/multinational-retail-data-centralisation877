# %%
# Library
import yaml 
import sqlalchemy
from sqlalchemy import create_engine, inspect, text  
import pandas as pd
from scratch_work.database_utils import DatabaseConnector
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

(df == 'NULL').sum()
df.to_csv('test')
test_df = pd.read_csv('test')
print((test_df == 'NULL').sum())


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

# Scratch work 
# %%
class DataInformation:
    def __init__(self, df):
         self.df = df 
    def percentage_of_null(self):
        percentage_of_null = self.df.isnull().sum() / len(self.df) * 100
        print(percentage_of_null)
    def null_as_string_count(self):
        null_as_string_count = (self.df == 'NULL').sum().sum()
        print(null_as_string_count) 
    def filter_rows_containing_null(self):
        rows_with_null = self.df[self.df.map(lambda x: x == 'NULL').any(axis =1)]
        print(rows_with_null)
        type(rows_with_null.iloc[0][0]) # proves that the 'NULL' is a string value 
     
# Example usage 
print(df.head())
info = DataInformation(df)
info.percentage_of_null()
info.null_as_string_count()
info.filter_rows_containing_null()


# %% 
# # Initial look at df 
# df_users.info()
# print(df_users.iloc[0])

# # percentage of null in df 
# percentage_of_null = df_users.isnull().sum() / len(df_users) * 100
# print(percentage_of_null)

# # checking dtypes of values of interest 
# type(df_users['lat'][0]) # This is a Python None Type 
# print(df_users['lat']) # This is a Python None Type 

# # 'NULL' values in df  
# null_as_string_count = (df_users == 'NULL').sum().sum()
# print(null_as_string_count) # there are 33 'NULL' strings

# %%
# # filter rows containing null 
# df = df_users 
# rows_with_null = df[df.map(lambda x: x == 'NULL').any(axis =1)]
# print(rows_with_null)
# type(rows_with_null['address'].iloc[0]) # proves that the 'NULL' is a string value 

# %%
# replace the 'NULL' to be NaN values 
# df = df_users 
# null_as_string_count_pre_filter = (df == 'NULL').sum().sum()
# print(f'Count of NULL as string pre filter {null_as_string_count_pre_filter}')
# df.replace('NULL', np.nan, inplace=True)
# null_as_string_count_post_filter = (df == 'NULL').sum().sum()
# print(f'Count of NULL as string post filter {null_as_string_count_post_filter}')


# %% 
# # removing rows with NaN values 
# print(f"DF shape: {df.shape}")
# # Here I've used 'any'  if any values in the row are 'NaN' the row will be dropped
# df.dropna(how = 'any', inplace=True)
# print(f"DF shape after dropping rows with NaN values {df.shape}")



# %% 
# number_rows_pre_filter = df.shape[0] 
# df.dropna(how = 'any', inplace=True) # Here I've used 'any'  if any values in the row are 'NaN' the row will be dropped
# print(f'Number of rows removed {number_rows_pre_filter - df.shape[0]}')
# print(f'Shape of DF {df.shape}')


# %% 
# find which table has the join_data coloumn 
# extract = DataExtractor()
# df = extract.read_rds_table(engine, 'legacy_users') # contains join_date
# df.columns
# type(df['join_date'])

# df['test_date'] = pd.to_datetime(df['join_date'], errors='coerce')

# print(df['test_date'])
# type(df['test_date'][0])
# print(f'join_date : /n {df['join_date'].info()}')
# df['join_date'] = pd.to_datetime(df['join_date'], errors='coerce')
# print(f'join date after transformation:/m {df['join_date'].info()}')

# invalid_rows = df[~pd.to_datetime(df['join_date'], errors='coerce').notna()]
# invalid_rows

# %% 
# def clean_user_data(df):

#     # change the 'NULL" string to NULL data types 
#     null_as_string_count_pre_filter = (df == 'NULL').sum().sum()
#     print(f'Count of NULL as string pre filter {null_as_string_count_pre_filter}')
#     df.replace('NULL', np.nan, inplace=True)
#     null_as_string_count_post_filter = (df == 'NULL').sum().sum()
#     print(f'Count of NULL as string pre filter {null_as_string_count_post_filter}')

#     # removing rows with NaN values 
#     number_rows_pre_filter = df.shape[0] 
#     df.dropna(how = 'any', inplace=True) # Here I've used 'any'  if any values in the row are 'NaN' the row will be dropped
#     print(f'Number of rows removed {number_rows_pre_filter - df.shape[0]}')
#     print(f'Shape of DF {df.shape}')

#     print(f'join_date : /n {df['join_date'].info()}')
#     df['join_date'] = pd.to_datetime(df['join_date'], errors='coerce')
#     print(f'join date after transformation:/m {df['join_date'].info()}')

# clean_user_data(df)
