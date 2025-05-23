# # %%
# import yaml 
# import sqlalchemy
# from sqlalchemy import create_engine, inspect, text  
# import pandas as pd
# import tabula
# from database_utils import DatabaseConnector
# import requests
# import pdfplumber

# # %%
# class DataExtractor:
#     ''' 
#     This is a utility class. 
#     It houses methods that help extract data from different sources
#     Data Sources include: CSV files, APIs & AWS S3 buckets.
#     '''
#     # def __init__(self):
#         # self.csv_file
    
#     @staticmethod
#     def read_rds_table(engine, table_name = ['legacy_store_details', 'dim_card_details', 'legacy_users', 'orders_table']):
#         ''' Read Data from an AWS RDS Table'''
#         query = text(f"SELECT * FROM {table_name}")
#         with engine.connect() as connection:
#             result = connection.execute(query)
#             # convert the result into a pandas df 
#             df = pd.DataFrame(result)
#             # print(f"(\nFirst 5 rows df: \n{df.head()})")
#             return df 
    
#     @staticmethod
#     def retrieve_pdf_data(pdf_url, file_name):
#         '''Retrieve Data from a PDF URL'''
#         local_pdf = file_name


#         # Download the PDF
#         with open(local_pdf, 'wb') as f:  # Opens the file in binary write mode
#             f.write(requests.get(pdf_url).content)  # Downloads the PDF content

#         # Extract tables from all pages
#         all_tables = []  # List to store extracted tables

#         with pdfplumber.open(local_pdf) as pdf:
#             for page in pdf.pages:  # Iterate through all pages in the PDF
#                 table = page.extract_table()
#                 if table:  # Only process non-empty tables
#                     all_tables.append(pd.DataFrame(table[1:], columns=table[0]))  # Use first row as headers

#         # Combine all tables into one DataFrame
#         df = pd.concat(all_tables, ignore_index=True)  # Combine all extracted DataFrames into one
#         return df 

# ####################################################################################

# if __name__ == '__main__':
#     # connect to Data Base 
#     db_connect = DatabaseConnector()
#     creds = db_connect.read_db_creds('db_creds.yaml')
#     print("Credentials: \n")
#     print(creds)  # This should print out the dictionary of credentials
#     print('#'*200)

#     # create engine 
#     engine = db_connect.init_db_engine(creds)
#     print('Engine: \n')
#     print(engine)
#     print('#'*200)

#     # view a list of all tables in data base 
#     print('List of Tables in Database: ')
#     db_connect.list_db_tables(engine)
#     print('#'*200)

#     # read data from one of the tables 
#     extract = DataExtractor()
#     df_legacy_users = extract.read_rds_table(engine, table_name='legacy_users')
#     print(df_legacy_users.head())
#     df_legacy_users.to_csv('test_users.csv', index=False)

#     # Extract the card_details pdf into a pandas df 
#     pdf_url = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
#     df_card_details = extract.retrieve_pdf_data(pdf_url, 'card_details.pdf')
#     df_card_details.head()
#     df_card_details.to_csv('card_details.csv', index=False)
# ####################################################################################

# if __name__ == "__main__":
# %%
import requests 
import pandas as pd 

API_KEY = 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'
HEADERS = {'x-api-key': API_KEY}
# requests.get(url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores', headers= HEADERS).json() # number stores 

# store_number = 1 
# requests.get(url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}', headers = HEADERS).json() # retrieve a store 

def retrieve_store_data(url, headers, store_number):
    """Retrieve a single store's data."""
    url = f'{url}{store_number}'
    response = requests.get(url, headers=headers)
    return response.json()  # Extract store data

retrieve_store_data(url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/', headers=HEADERS, store_number=1)


# url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'
# store_number = 1 
# new_url = f'{url}{store_number}'
# new_url

def list_number_of_stores(url_number_stores, headers_dict):
    response = requests.get(url = url_number_stores, headers = headers_dict).json()
    number_of_stores = response['number_stores']
    return number_of_stores

number_of_stores = list_number_of_stores(url_number_stores='https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores', headers_dict= HEADERS)


# def retrieve_stores_data(url_store_number, store_number, headers_dict):
#     response = requests.get(url_store_number)


class DataExtractor: 
    @staticmethod
    def list_number_of_stores(url_number_stores, headers_dict):
        response = requests.get(url = url_number_stores, headers = headers_dict).json()
        number_of_stores = response['number_stores']
        return number_of_stores

    @staticmethod
    def retrieve_store_data(url, headers, store_number):
        """Retrieve a single store's data."""
        url = f'{url}{store_number}'
        response = requests.get(url, headers=headers)
        return response.json()  # Extract store data
        
    @staticmethod
    def retrieve_all_stores_data(url_store_details, headers_dict, total_stores):
        all_stores = []
        for i in range(total_stores):
            store = DataExtractor.retrieve_stores_data(f'{url_store_details}{i}', headers_dict, store_number = i)
            all_stores.append(store)
        df = pd.DataFrame(all_stores)
        return df

# Constants 
API_KEY = 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'
HEADERS = {'x-api-key': API_KEY}
NUMBER_STORES_URL = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
STORE_DETAILS_URL = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'


# url_store_details = STORE_DETAILS_URL
# i = 1
# test = f'{url_store_details}{i}'
# test

extract = DataExtractor()

# total number of stores 
number_of_stores = extract.list_number_of_stores(NUMBER_STORES_URL, HEADERS)
number_of_stores

# get the data of one store 
# store_number = 1 
# extract.get_store_data(url_store_number=f'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}', headers_dict=HEADERS)




# %%


# extract.list_number_of_stores(url='https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores', headers=HEADERS)

# store_number = 1
# store_1 = extract.retrieve_store_data(f'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}', HEADERS)
# store_1


# requests.get(url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/1', headers = HEADERS).json() # retrieve a store 

# df = pd.DataFrame([store_1])
# df

import requests 
import pandas as pd 

API_KEY = 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'
HEADERS = {'x-api-key': API_KEY}

class DataExtractor: 
    @staticmethod
    def retrieve_store_data(url, headers):
        """Retrieve a single store's data."""
        response = requests.get(url, headers=headers)
        return response.json()  # Extract store data

extract= DataExtractor()

def list_number_of_stores(url_number_stores, headers_dict):
    response = requests.get(url = url_number_stores, headers = headers_dict).json()
    number_of_stores = response['number_stores']
    return number_of_stores

total_stores = list_number_of_stores(url_number_stores='https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores', headers_dict= HEADERS)
# total_stores = 5

def retrieve_all_stores_data_df(total_stores):
    all_stores = []
    for i in range(total_stores + 1):
        store = extract.retrieve_store_data(f'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{i}', HEADERS)
        all_stores.append(store)
    df = pd.DataFrame(all_stores)
    return df

df_all_stores = retrieve_all_stores_data_df(total_stores)
df_all_stores

df_all_stores.to_csv('store_details_uncleaned.csv')


# for i in range(2):
#     print(i)

    
###########

# %% GPT generated, Working.
import requests 
import pandas as pd

class DataExtractor: 
    @staticmethod
    def list_number_of_stores(url_number_stores, headers_dict):
        response = requests.get(url=url_number_stores, headers=headers_dict).json()
        number_of_stores = response['number_stores']
        return number_of_stores

    @staticmethod
    def retrieve_store_data(url, headers, store_number):
        """Retrieve a single store's data."""
        full_url = f'{url}/{store_number}'  # construct the store-specific URL
        response = requests.get(full_url, headers=headers)
        if response.status_code == 200:
            try:
                return response.json()  # Extract store data
            except ValueError:
                return {"error": "Invalid JSON response"}  # Handle invalid JSON
        else:
            return {"error": f"HTTP {response.status_code}: {response.text}"}

    @staticmethod
    def retrieve_all_stores_data(url_store_details, headers_dict, total_stores):
        """Retrieve all stores' data and return as a DataFrame."""
        all_stores = []
        for store_number in range(1, total_stores + 1):  # Loop from 1 to total_stores inclusive
            store = DataExtractor.retrieve_store_data(url_store_details, headers_dict, store_number)
            if isinstance(store, dict):  # Ensure the response is a dictionary
                all_stores.append(store)
            else:
                print(f"Skipping store {store_number}: Invalid response format")
        return pd.DataFrame(all_stores)
    


# Constants
API_KEY = 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'
HEADERS = {'x-api-key': API_KEY}
NUMBER_STORES_URL = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
STORE_DETAILS_URL = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details'

# Usage
extractor = DataExtractor()
total_stores = extractor.list_number_of_stores(NUMBER_STORES_URL, HEADERS)  # Get total number of stores
print(f"Total stores: {total_stores}")

# Retrieve all stores and convert to a DataFrame
stores_df = extractor.retrieve_all_stores_data(STORE_DETAILS_URL, HEADERS, total_stores)
print(stores_df.head())

stores_df.to_csv('stores_details_uncleaned.csv') 

# MK: Check the final row
# %%

