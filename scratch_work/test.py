from data_cleaning import DataInformation, pd
df = pd.read_csv('card_data.csv')
info = DataInformation(df)
info.percentage_of_null()