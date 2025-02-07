import pandas as pd
import numpy as np

# Load the data
dt = pd.read_csv("cleaned_shanghai.csv")

# Ensure the "Market Cap" column is numeric, coercing errors to NaN
dt["Market Cap"] = pd.to_numeric(dt["Market Cap"], errors="coerce")

# Define market cap thresholds in Yuan (CNY)
LARGE_CAP_THRESHOLD = 70000  # 70 billion CNY
MEDIUM_CAP_THRESHOLD = 14000  # 14 billion CNY

# Filter companies by market cap
large_cap = dt[dt["Market Cap"] > LARGE_CAP_THRESHOLD]
medium_cap = dt[
    (dt["Market Cap"] >= MEDIUM_CAP_THRESHOLD)
    & (dt["Market Cap"] <= LARGE_CAP_THRESHOLD)
]
small_cap = dt[dt["Market Cap"] < MEDIUM_CAP_THRESHOLD]

# Print results
print("Large Cap Companies:")
print(large_cap)

print("\nMedium Cap Companies:")
print(medium_cap)

print("\nSmall Cap Companies:")
print(small_cap)

# Save filtered data to separate CSV files (optional)
large_cap.to_csv("large_cap_companies.csv", index=False)
medium_cap.to_csv("medium_cap_companies.csv", index=False)
small_cap.to_csv("small_cap_companies.csv", index=False)
