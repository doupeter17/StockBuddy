import pandas as pd
import numpy as np

# Load the dataset
dt = pd.read_csv("vietnam_data.csv")

# Filter companies with Market Cap > 10 trillion
comp_list = dt[(dt["Market Cap"] > 10000000000000)]

# Get unique company identifiers
large_comp = comp_list["Comp_data"].unique()

# Filter the original DataFrame for these companies
large_company_data = dt[dt["Comp_data"].isin(large_comp)]

# Check for rows with NaN and year not equal to 2024
rows_with_na = large_company_data.isna().any(axis=1) & (
    large_company_data["Year"] != 2024
)

# Drop rows with NaN
noNa_company = large_company_data.dropna()

# Map quarters to the last day of the quarter and create a "Time" column
quarter_end_map = {1: "03-31", 2: "06-30", 3: "09-30", 4: "12-31"}
noNa_company["Time"] = pd.to_datetime(
    noNa_company["Year"].astype(str)
    + "-"
    + noNa_company["Quarter"].map(quarter_end_map)
)

# Calculate SIZE as the natural logarithm of Total Assets
noNa_company["SIZE"] = np.log(noNa_company["Total Assets"])

# Save the cleaned DataFrame to a CSV file
noNa_company.to_csv("Vietnam_large_cleaned.csv", index=False)

print("Data saved to 'Vietnam_large_cleaned.csv'")
