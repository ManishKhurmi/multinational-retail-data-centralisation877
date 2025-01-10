# %%
import yaml 
import sqlalchemy
from sqlalchemy import create_engine, inspect, text  
import pandas as pd
import tabula
from database_utils import DatabaseConnector
import requests
import pdfplumber

# %%
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
    
    @staticmethod
    def retrieve_pdf_data(pdf_url):
        local_pdf = "card_details.pdf"


        # Download the PDF
        with open(local_pdf, 'wb') as f:  # Opens the file in binary write mode
            f.write(requests.get(pdf_url).content)  # Downloads the PDF content

        # Extract tables from all pages
        all_tables = []  # List to store extracted tables

        with pdfplumber.open(local_pdf) as pdf:
            for page in pdf.pages:  # Iterate through all pages in the PDF
                table = page.extract_table()
                if table:  # Only process non-empty tables
                    all_tables.append(pd.DataFrame(table[1:], columns=table[0]))  # Use first row as headers

        # Combine all tables into one DataFrame
        df = pd.concat(all_tables, ignore_index=True)  # Combine all extracted DataFrames into one
        return df 

# %%
####################################################################################
# # Example Usage     
# # connect to database 
# db_connect = DatabaseConnector()
# creds = db_connect.read_db_creds('db_creds.yaml')
# print("Credentials: \n")
# print(creds)  # This should print out the dictionary of credentials
# print('#'*200)

# # create engine 
# engine = db_connect.init_db_engine(creds)
# print('Engine: \n')
# print(engine)
# print('#'*200)

# # view a list of all tables in data base 
# print('List of Tables in Database: ')
# db_connect.list_db_tables(engine)
# print('#'*200)

# # read data from one of the tables 
# extract = DataExtractor()
# df = extract.read_rds_table(engine, table_name='legacy_store_details')
# print(df.head())

# # Extract the card_details pdf into a pandas df 
# pdf_url = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
# card_details_df = extract.retrieve_pdf_data(pdf_url)
# card_details_df.head()
####################################################################################




# %%
# Manish: T4 extrating data from the PDF in AWS 
# pdf_path = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
# df = tabula.read_pdf(pdf_path, stream=True)
# print(df)
# print(len(df))

# testing 
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


