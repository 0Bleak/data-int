import pandas as pd
import numpy as np
from scipy.stats import zscore
import seaborn as sns
import matplotlib.pyplot as plt

file1_path = "./data/cleaned_global_water_consumption_inspected.csv"
file2_path = "./data/water_sdg_indicators.csv"

df1 = pd.read_csv(file1_path)
df2 = pd.read_csv(file2_path)

print("Missing values:")
print(df1.isnull().sum())
print(df2.isnull().sum())

if 'Total Water Consumption (Billion Cubic Meters)' in df1.columns:
    df1['zscore'] = np.abs(zscore(df1['Total Water Consumption (Billion Cubic Meters)']))
    outliers = df1[df1['zscore'] > 3]  
    print("Potential outliers:")
    print(outliers)

if all(col in df1.columns for col in ['Agricultural Water Use (%)', 'Industrial Water Use (%)', 'Household Water Use (%)']):
    df1['Total_Percentage'] = df1['Agricultural Water Use (%)'] + df1['Industrial Water Use (%)'] + df1['Household Water Use (%)']
    inconsistent_rows = df1[(df1['Total_Percentage'] > 100.5) | (df1['Total_Percentage'] < 99.5)]
    print("Inconsistent percentage rows:")
    print(inconsistent_rows)

if 'Per Capita Water Use (Liters per Day)' in df1.columns:
    df1['Per Capita Water Use (m³ per Day)'] = df1['Per Capita Water Use (Liters per Day)'] / 1000

if all(col in df1.columns for col in ['Agricultural Water Use (%)', 'Industrial Water Use (%)', 'Household Water Use (%)']):
    df1['Agricultural Water Use Ratio'] = df1['Agricultural Water Use (%)'] / 100
    df1['Industrial Water Use Ratio'] = df1['Industrial Water Use (%)'] / 100
    df1['Household Water Use Ratio'] = df1['Household Water Use (%)'] / 100

if all(col in df1.columns for col in ['Total Water Consumption (Billion Cubic Meters)', 'Rainfall Impact (Annual Precipitation in mm)']):
    df1['Water Stress Index'] = df1['Total Water Consumption (Billion Cubic Meters)'] / df1['Rainfall Impact (Annual Precipitation in mm)']
    df1['Water Stress Normalized'] = (df1['Water Stress Index'] - df1['Water Stress Index'].min()) / (df1['Water Stress Index'].max() - df1['Water Stress Index'].min())

    def classify_water_scarcity(wsi):
        if wsi < 0.2:
            return "No Stress"
        elif 0.2 <= wsi < 0.4:
            return "Low Stress"
        elif 0.4 <= wsi < 0.7:
            return "Moderate Stress"
        else:
            return "High Stress"
    
    df1['Water Scarcity Level'] = df1['Water Stress Index'].apply(classify_water_scarcity)

df1.to_csv("./data/cleaned_transformed_water_data.csv", index=False)

sns.histplot(df1['Water Stress Index'].dropna(), bins=20, kde=True)
plt.title("Distribution of Water Stress Index")
plt.show()

print("Data transformation completed! Cleaned file saved.")
