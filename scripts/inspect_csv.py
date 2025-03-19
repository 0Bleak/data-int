import pandas as pd

def inspect_dataset(file_path):
    """Function to inspect a dataset."""
    print(f"\nInspecting: {file_path}")
    df = pd.read_csv(file_path)
    
    print("\nBasic Info:")
    print(df.info())
    
    print("\nFirst 5 rows:")
    print(df.head())
    
    print("\nMissing Values:")
    print(df.isnull().sum())
    
    print("\nDuplicate Rows:")
    print(df.duplicated().sum())
    
    print("\nSummary Statistics:")
    print(df.describe())
    
    print("\nUnique Values per Column:")
    for col in df.columns:
        print(f"{col}: {df[col].nunique()} unique values")
    
    return df

data_files = [
    "./data/cleaned_global_water_consumption_inspected.csv",
    "./data/water_sdg_indicators.csv"
]

for file in data_files:
    try:
        inspect_dataset(file)
    except Exception as e:
        print(f"Error inspecting {file}: {e}")