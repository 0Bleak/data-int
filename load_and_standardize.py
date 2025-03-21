import pandas as pd
import xml.etree.ElementTree as ET
import json


csv_path = "data/cleaned_global_water_consumption.csv"
xml_path = "data/water_crisis.xml"
json_path = "data/water_policies.json"


water_consumption_df = pd.read_csv(csv_path)


tree = ET.parse(xml_path)
root = tree.getroot()
water_crisis_data = []

for country in root.findall('Country'):
    water_crisis_data.append({
        "Country": country.get('name'),
        "Year": int(country.get('year')),
        "CrisisSeverity": country.find('CrisisSeverity').text,
        "MitigationEfforts": country.find('MitigationEfforts').text
    })

water_crisis_df = pd.DataFrame(water_crisis_data)


with open(json_path, 'r') as f:
    water_policies_data = json.load(f)

water_policies_df = pd.DataFrame(water_policies_data)


water_crisis_df.to_csv("data/standardized_water_crisis.csv", index=False)
water_policies_df.to_csv("data/standardized_water_policies.csv", index=False)

print("Data successfully loaded and standardized.")
