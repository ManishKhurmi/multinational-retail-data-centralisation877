# %%
import yaml 
import sqlalchemy
from sqlalchemy import create_engine, inspect, text  
import pandas as pd
from database_utils import DatabaseConnector
from data_extraction import DataExtractor
import numpy as np 

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
    
class DataCleaning:
    
    def __init__(self, df):
        self.df = df
    def null_as_string_count(self):
        null_as_string_count = (self.df == 'NULL').sum().sum()
        print(f'Number of NULL as strings in the DF: \n{null_as_string_count}') 
    
    def filter_rows_containing_null_as_string(self):
        '''Filters rows that have 'NULL' as a str() value'''
        rows_with_null = self.df[self.df.map(lambda x: x == 'NULL').any(axis =1)]
        print(f'Rows that contain NULL as a string type in DF: \n{rows_with_null}')

    def replace_null_strings_with_null_data_types(self):
        null_as_string_count_pre_filter = (self.df == 'NULL').sum().sum()
        print(f'Count of NULL as string pre filter {null_as_string_count_pre_filter}')
        self.df = self.df.replace('NULL', np.nan)
        print(f'Count of NULL as string post filter {null_as_string_count_pre_filter}')
        return self.df

    def remove_rows_with_nan_values(self):
        number_rows_pre_filter = self.df.shape[0] 
        self.df = self.df.dropna(how = 'any') # Here I've used 'any'  if any values in the row are 'NaN' the row will be dropped
        print(f'Number of rows removed {number_rows_pre_filter - self.df.shape[0]}')
        print(f'Shape of DF {self.df.shape}')
        return self.df

    def convert_col_to_datetime(self, column_name):
        self.df[column_name] = pd.to_datetime(self.df[column_name], errors='coerce')
        print(f'Information on DF: {self.df.info()}')
        return self.df 

    def remove_duplicates(self, column_name):
        count_duplicates = len(self.df[column_name]) - len(self.df[column_name].drop_duplicates())
        print(f'Number of Duplicates in {column_name}: \n{count_duplicates}')
        filtered_df = self.df.drop_duplicates(column_name) 
        return filtered_df

    def remove_rows_containing_non_numeric_values(self, column_name, show_removed_rows = True):
        '''
        Removes rows if the specified column contains non-numeric values. 
        Handling NaN values:
            If a value is NaN then it will replaced with a FALSE with df[column].notna()
        '''
        mask = self.df[column_name].notna() & self.df[column_name].str.isnumeric()
        filtered_df = self.df[mask]
        print(f'Number of removed rows: {len(self.df) - len(filtered_df)}')

        if show_removed_rows == True:
            print(self.df[~mask])

        return filtered_df 

    def clean_user_data(self):
        print('Step 1')
        self.replace_null_strings_with_null_data_types()
        print(self.df.shape)

        print('Step 2')
        self.remove_rows_with_nan_values()
        print(self.df.shape)

        print('Step 3 ')
        self.convert_col_to_datetime('join_date')
        print(self.df.shape)

        return self.df

    def clean_card_data(self):
        ''' 
        This function is for filtering the card_data.
        This function relies on the support functions.
        defined in the class by updating the input self.df in each step.

        '''
        print('step 1: Count of NULL as string count in DF')
        self.null_as_string_count()
        
        print('step 2: Remove Rows with NULL Values')
        # self.df = self.remove_null_values() # each time we are updating the input df by using self.df
        self.df = self.remove_rows_with_nan_values()
        print('\n')
        
        print('step 3: Remove Rows with Dupliates in card_number')
        self.df = self.remove_duplicates('card_number')
        print(f'Shape of DF {self.df.shape}\n')
        
        print('step 4: Remove non-numeric card numbers')
        self.df = self.remove_rows_containing_non_numeric_values('card_number')
        print(f'Shape of DF {self.df.shape}\n')
        
        print('step 5: change date_payment_confirmed to datetime')
        self.df = self.convert_col_to_datetime('date_payment_confirmed')
        print(self.df.info())
        print(f'Shape of DF {self.df.shape}\n')

        return self.df

########################################################################################################################################################

# %% Cleaning Legacy Users 
df_legacy_users = pd.read_csv('legacy_users.csv')
df_legacy_users.head()
cleaning = DataCleaning(df_legacy_users)
# rename df to reflect the cleaning step
df_legacy_users_cleaned = cleaning.clean_user_data()
print(df_legacy_users_cleaned.info())

########################################################################################################################################################

# %% Cleaning Card Data 
df_card_data = pd.read_csv('card_data.csv')
df_card_data.head()
cleaning = DataCleaning(df_card_data)
df_card_data_cleaned = cleaning.clean_card_data()
print(df_card_data_cleaned.info())

