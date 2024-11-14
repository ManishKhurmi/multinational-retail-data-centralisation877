import yaml 

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
        
    
# Example usage
db_connect = DatabaseConnector()
creds = db_connect.read_db_creds('db_creds.yaml')
print(creds)  # This should print out the dictionary of credentials

# Testing 




# # connect to sales_data
# import psycopg2
# with psycopg2.connect(host='localhost', user='postgres', password='ginger', dbname='sales_data', port=5432) as conn:
#     with conn.cursor() as cur:
#         cur.execute("""SELECT table_name FROM information_schema.tables
#        WHERE table_schema = 'public'""")
#         for table in cur.fetchall():
#             print(table)

