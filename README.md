# multinational-retail-data-centralisation877

## To-do's

### Low Priority 
- Task 3: extract and clean the user data. After cleaning rows are currently 15299 when they should be 15284
- M2T4: in the card_data -> the 'card_number' when removing non numeric, removes 51 rows. This means we are left with 15258, however Ai Core mentions the total after all filtering is 15284, check with engineers 
- cleaningv3. some functions return filtered_df vs some return self.df -> decide on design choice and make it consistent 

### High Priority 
[X] MK: M2T4 -> clean_card_data -> cleaningv4.py-> card details needs the step in cleaning algorithm 'replace_null_strings_with_null_data_types'-> Then this is good to now upload the table to postgres local DB
[X] MK: use of __main__ in the scripts
[X] MK Clean up file structure
[X] MK: upload to local DB 
[X] Continue cleaning for the other data sets 
[] Check the final row of the stores_detials_uncleaned_csv
[] Understand what changes were needed to the code 
[] Step 4: Create a method in the DataCleaning class called_clean_store_data which cleans the data retrieve from the API and returns a pandas DataFrame.

## Learnings 
- When you save the DataFrame to a CSV and read it back, Pandas may interpret 'NULL' strings as missing values (NaN) due to its default behavior. By default:
    - Pandas treats certain strings ('NULL', 'N/A', 'NA', etc.) as missing values when reading a CSV file.