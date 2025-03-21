import pandas as pd


water_policies_df = pd.read_csv("data/cleaned_water_policies.csv")


water_policies_df["Regulations"] = water_policies_df["Regulations"].apply(
    lambda x: ', '.join(eval(x)) if isinstance(x, str) else "None"
)



water_policies_df.to_csv("data/transformed_water_policies.csv", index=False)

print("Data transformation completed.")
