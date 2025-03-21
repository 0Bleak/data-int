import pandas as pd


water_crisis_df = pd.read_csv("data/standardized_water_crisis.csv")
water_policies_df = pd.read_csv("data/standardized_water_policies.csv")


severity_mapping = {"Low": 1, "Medium": 2, "High": 3}
water_crisis_df["CrisisSeverity"] = water_crisis_df["CrisisSeverity"].map(severity_mapping)


water_crisis_df.fillna({"MitigationEfforts": "Unknown"}, inplace=True)
water_policies_df.fillna({"Policy": "No Policy", "Investment (Million USD)": 0}, inplace=True)

water_crisis_df.drop_duplicates(inplace=True)
water_policies_df.drop_duplicates(inplace=True)


water_crisis_df.to_csv("data/cleaned_water_crisis.csv", index=False)
water_policies_df.to_csv("data/cleaned_water_policies.csv", index=False)

print("Data cleaning completed.")
