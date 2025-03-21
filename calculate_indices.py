import pandas as pd
import numpy as np

df = pd.read_csv("data/final_water_data.csv")

df["consumption_rainfall_ratio"] = df.apply(
    lambda r: r["Total Water Consumption (Billion Cubic Meters)"] / r["Rainfall Impact (Annual Precipitation in mm)"]
    if r["Rainfall Impact (Annual Precipitation in mm)"] != 0 else np.nan, axis=1)

df["investment_consumption_ratio"] = df.apply(
    lambda r: r["Investment (Million USD)"] / r["Total Water Consumption (Billion Cubic Meters)"]
    if r["Total Water Consumption (Billion Cubic Meters)"] != 0 else np.nan, axis=1)

df["groundwater_stress"] = df.apply(
    lambda r: r["Per Capita Water Use (Liters per Day)"] * r["Groundwater Depletion Rate (%)"], axis=1)

df["usage_balance"] = df.apply(
    lambda r: r["Agricultural Water Use (%)"] + r["Industrial Water Use (%)"] + r["Household Water Use (%)"], axis=1)

df["water_stress_index"] = df.apply(
    lambda r: (r["CrisisSeverity"] + r["consumption_rainfall_ratio"] + r["groundwater_stress"] + r["usage_balance"]) /
    (r["investment_consumption_ratio"] + 1)
    if not pd.isnull(r["investment_consumption_ratio"]) else np.nan, axis=1)

df.to_csv("data/enriched_water_data.csv", index=False)
print("Indices added and file saved.")