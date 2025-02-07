import pandas as pd
import numpy as np

# Load the data
dt = pd.read_csv("shanghai.csv")

# Replace "Na" with 0 in specific columns
dt["DIV"].replace("Na", 0, inplace=True)
dt["DY"].replace("Na", 0, inplace=True)

# Replace "Na" with NaN in the entire DataFrame
dt.replace("Na", np.nan, inplace=True)

# Convert "Total Debt" and "Total Assets" to numeric, coercing errors to NaN
dt["Total Debt"] = pd.to_numeric(dt["Total Debt"], errors="coerce")
dt["Total Assets"] = pd.to_numeric(dt["Total Assets"], errors="coerce")

# Identify rows with any NaN values
na_rows = dt[dt.isna().any(axis=1)]

# Get unique company codes with NaN values
comp_with_na = na_rows["Comp_code"].unique()
print("Company codes with NaN values:", comp_with_na)

# Filter out rows where Comp_code is in the list of codes with NaN values
noNa_df = dt[~dt["Comp_code"].isin(comp_with_na)]
print("DataFrame without rows containing NaN values:")

# Calculate Debt-to-Asset Ratio (DAR)
# Handle division by zero by replacing infinities with NaN
noNa_df["DAR"] = noNa_df["Total Debt"] / noNa_df["Total Assets"]
noNa_df["DAR"].replace([np.inf, -np.inf], np.nan, inplace=True)

# Calculate SIZE as the natural logarithm of Total Assets
# Ensure Total Assets is positive before applying log
noNa_df["SIZE"] = np.log(
    noNa_df["Total Assets"].where(noNa_df["Total Assets"] > 0, np.nan)
)

print(noNa_df)

# Save the cleaned DataFrame to a new CSV file
noNa_df.to_csv("cleaned_shanghai.csv", index=False)
