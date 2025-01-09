import pandas as pd
import statsmodels.api as sm
from linearmodels.panel import PanelOLS, RandomEffects
import numpy as np

# Load the dataset
df = pd.read_csv("VN_data2.csv")
df["time"] = pd.to_datetime(df["Time"])
# Ensure there is an entity and time index for panel models
df["entity"] = df[
    "Comp_code"
]  # Replace with actual entity identifier column  # Replace with the time identifier column
df = df.set_index(["entity", "time"])

# Define dependent variable (Year End Price) and independent variables
y = df["Price"]  # Dependent variable
x = df[
    [
        "EPS",
        "BVPS",
        "ROA",
        "ROE",
        "DAR",
        "DIV",
        "SIZE",
    ]
]  # Independent variables
x = sm.add_constant(x)  # Add constant for intercept

# OLS Model
ols_result = sm.OLS(y, x.astype(float)).fit()
print("OLS Results:")
print(ols_result.summary())

# GLS Model
gls_model = sm.GLS(y, x.astype(float)).fit()
print("\nGLS Results:")
print(gls_model.summary())

# Fixed Effects Model (FEM)
fem_model = PanelOLS(y, x, entity_effects=True, check_rank=False).fit()
print("\nFixed Effects Model (FEM) Results:")
print(fem_model)

# Random Effects Model (REM)
rem_model = RandomEffects(y, x).fit()
print("\nRandom Effects Model (REM) Results:")
print(rem_model)
