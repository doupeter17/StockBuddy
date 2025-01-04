import pandas as pd

df = pd.read_csv("shanghai.csv")
ids_to_remove = df.loc[(df == "-").any(axis=1), "Comp_code"].unique()
cleaned_df = df[~df["Comp_code"].isin(ids_to_remove)]
print(cleaned_df)
cleaned_df.to_csv("shanghai_cleaned.csv")
