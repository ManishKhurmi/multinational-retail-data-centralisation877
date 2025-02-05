
# %%
import requests
import pdfplumber
import pandas as pd


# %%
# Extracting all data from the pages 
pdf_url = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
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

print(df)

# %%
# Export the DataFrame to a CSV file
output_csv = "card_data.csv"  # Specify the output CSV file name
df.to_csv(output_csv, index=False)  # Save the DataFrame without the index column

print(f"DataFrame has been exported to {output_csv}")
# %%
df = pd.read_csv('/Users/manishkhurmi/Desktop/data_centralisation/card_data.csv')
print(df.head(200))


# %%
class DataExtractor:
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

pdf_url = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
extractor = DataExtractor
df = extractor.retrieve_pdf_data(pdf_url)

df.head()

# %%
