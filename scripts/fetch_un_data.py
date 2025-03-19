import requests
import json
import pandas as pd
import os

BASE_URL = "https://unstats.un.org/sdgs/UNSDGAPIV5/v1/sdg"

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

def get_sdg_indicators():
    """Fetch only water-related SDG indicators."""
    url = f"{BASE_URL}/Indicator/List"
    response = requests.get(url)
    
    if response.status_code != 200:
        print(f"Failed to fetch SDG indicators: {response.status_code}")
        return None

    try:
        data = response.json()
        water_indicators = [ind for ind in data if ind["goal"] == "6"]
        print(f"Found {len(water_indicators)} water-related indicators.")
        return water_indicators
    except json.JSONDecodeError:
        print("Error decoding SDG indicators JSON response.")
        return None

def save_to_csv(data, filename):
    """Convert JSON data to a structured DataFrame and save as CSV."""
    if not data:
        print(f"No data to save for {filename}.")
        return

    print(f"Raw JSON preview for {filename}:")
    print(json.dumps(data, indent=4)[:1000])  

    try:
        df = pd.json_normalize(data)  
        file_path = os.path.join(DATA_DIR, filename)
        df.to_csv(file_path, index=False)
        print(f"Data saved successfully to {file_path}")
    
    except Exception as e:
        print(f"Error converting JSON to DataFrame: {e}")

if __name__ == "__main__":
    print("Fetching Water-Related UN SDG Indicators...")
    water_indicators = get_sdg_indicators()
    save_to_csv(water_indicators, "water_sdg_indicators.csv")

