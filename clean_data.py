import pandas as pd

data_file = pd.read_csv("fin_data_2023.csv")
new_df = data_file.dropna()

new_df.to_csv("shangHai_2023_cleaned.csv")
