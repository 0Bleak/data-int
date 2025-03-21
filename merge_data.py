import pandas as pd

water_consumption_df = pd.read_csv("data/cleaned_global_water_consumption.csv")
water_crisis_df = pd.read_csv("data/cleaned_water_crisis.csv")
water_policies_df = pd.read_csv("data/transformed_water_policies.csv")

merged_df = water_consumption_df.merge(water_crisis_df, on=["Country", "Year"], how="left")
merged_df = merged_df.merge(water_policies_df, on=["Country", "Year"], how="left")

merged_df.fillna({
    "CrisisSeverity": 0,
    "MitigationEfforts": "Unknown",
    "Policy": "No Policy",
    "Investment (Million USD)": 0,
    "Regulations": "None"
}, inplace=True)

merged_df.to_csv("data/final_water_data.csv", index=False)

print("Data merging completed. Final dataset saved as 'data/final_water_data.csv'.")
