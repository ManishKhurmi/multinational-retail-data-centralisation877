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

    def count_of_null_rows(self):
        return self.df.isnull().sum()     
# # Example usage 



# %% 
############################################################################
# Clean Card Data 
# read data 
df = pd.read_csv("card_data.csv")
df.head()

# # Card Details Cleaning Requirements:
# # - **Change "NULL" strings data type into NULL data type** --> 0 found 
# # - **Remove NULL values** --> removed 11 rows
# # - **Remove duplicate card numbers**
# # - **Remove non-numerical card numbers**
# # - **Convert "date_payment_confirmed" column into a datetime data type**

############
# Step 1: Change 'NULL' strings to NULL data types 
print(df.head())
info = DataInformation(df)
info.null_as_string_count() # 0 
info.filter_rows_containing_null_as_string() # 0 
info.percentage_of_null() # >0 


############
# Step 2: Remove NULL values 
# df.isnull().sum() # there are 11 rows that are null 
# info.count_of_null_rows() # 11 

def remove_null_values(df): # TODO: make part of a class 
    # removing rows with NaN values 
    number_rows_pre_filter = df.shape[0] 
    df.dropna(how = 'any', inplace=True) # Here I've used 'any'  if any values in the row are 'NaN' the row will be dropped
    print(f'Number of rows removed {number_rows_pre_filter - df.shape[0]}')
    print(f'Shape of DF {df.shape}')
    return df

# remove_null_values(df)


############
# Step 3: Remove duplicate card numbers   
# df['card_number']
# df.head(2)

# # count duplicates in one column 
# count_duplicates = len(df['card_number']) - len(df['card_number'].drop_duplicates())
# count_duplicates


# # test 
# df = pd.DataFrame({'col1': [1, 2, 3, 4],
#                    'col2': [1, 2, 4, 4] 
#                    })
# df['col2'].drop_duplicates()
# df.drop_duplicates('col2')


def remove_duplicates(df, column_name):
    count_duplicates = len(df[column_name]) - len(df[column_name].drop_duplicates())
    print(f'Number of Duplicates in {column_name}: \n{count_duplicates}')
    filtered_df = df.drop_duplicates(column_name) 
    return filtered_df

# remove_duplicates(df, 'col2')
# remove_duplicates(df, 'card_number')


############
# Step 4: remove non-numerical card number 
# type(df['card_number'].iloc[0])

def remove_non_numeric_card_numbers(df, column_name):
    '''
    Removes rows if the specified column contains non-numeric values. 
    Handling NaN values:
        If a value is NaN then it will replaced with a FALSE with df[column].notna()
    '''
    filtered_df = df[df["card_number"].notna() & df["card_number"].str.isnumeric()]

    print(f'Number of removed rows: {len(df) - len(filtered_df)}')
    return filtered_df 

# remove_non_numeric_card_numbers(df, 'card_number')

############
# Step 5: Convert "date_payment_confirmed" column into a datetime data type

# print(f'join_date : /n {df['join_date'].info()}')
# df['join_date'] = pd.to_datetime(df['join_date'], errors='coerce')
# print(f'join date after transformation:/m {df['join_date'].info()}')


def convert_col_to_datetime(df, column_name):
    df[column_name] = pd.to_datetime(df[column_name], errors='coerce')
    return df 

# df = convert_col_to_datetime(df, 'date_payment_confirmed')
# df.head()
# df.columns
# df.info()
# MK TODO: cleaning class to have helper functions that are then called in each of the specific data cleaning applications, this may not work as the cases are different.



# support functions 

# %%
df = pd.read_csv("card_data.csv")
df.head()

class DataCleaningTwo:
    '''currently only contains cleaning for card_details'''
    def __init__(self, df):
        self.df = df 

    def remove_null_values(self): 
        # removing rows with NaN values 
        number_rows_pre_filter = self.df.shape[0] 
        self.df.dropna(how = 'any', inplace=True) # Here I've used 'any'  if any values in the row are 'NaN' the row will be dropped
        print(f'Number of rows removed {number_rows_pre_filter - self.df.shape[0]}')
        print(f'Shape of DF {df.shape}')
        return self.df

    def remove_duplicates(self, column_name):
        count_duplicates = len(self.df[column_name]) - len(self.df[column_name].drop_duplicates())
        print(f'Number of Duplicates in {column_name}: \n{count_duplicates}')
        filtered_df = self.df.drop_duplicates(column_name) 
        return filtered_df

    def remove_non_numeric_card_numbers(self, column_name, show_removed_rows = True):
        '''
        Removes rows if the specified column contains non-numeric values. 
        Handling NaN values:
            If a value is NaN then it will replaced with a FALSE with df[column].notna()
        '''
        mask = self.df[column_name].notna() & self.df[column_name].str.isnumeric()
        filtered_df = self.df[mask]
        print(f'Number of removed rows: {len(self.df) - len(filtered_df)}')

        if show_removed_rows == True:
            print(df[~mask])

        return filtered_df 

    def convert_col_to_datetime(self, column_name):
        self.df[column_name] = pd.to_datetime(self.df[column_name], errors='coerce')
        return df 

    def clean_card_data(self):
        # step 2: remove null values 
        print('step 2')
        df = remove_null_values(self.df)
        print(df.shape)
        # step 3: remove dupes in card_number column 
        print('step 3')
        df = remove_duplicates(df, 'card_number')
        print(df.shape)
        # step 4: remove non numeric card numbers in card_number 
        print('step 4')
        df = remove_non_numeric_card_numbers(df, 'card_number')
        print(df.shape)
        # step 5: convert date_payment_confirmed to datetime
        print('step 5')
        df = convert_col_to_datetime(df, 'date_payment_confirmed')
        print(df.shape)

clean = DataCleaningTwo(df)
clean.clean_card_data()

# MK TODO: 
    # make the steps in the cleaning algorithm clearer (print statements )
    # Ask engineers why after step 4 the DF is reduced more than they have said. 

# Step 4 deep dive
# clean.remove_non_numeric_card_numbers('card_number', show_removed_rows=True) # 

# df = pd.read_csv('card_data.csv')
# column_name = 'card_number'
# mask = df[column_name].notna() & df[column_name].str.isnumeric()
# # filtered_df = df[mask]
# df[~mask]    




# %%
