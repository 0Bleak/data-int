import sqlite3
import pandas as pd
import json
import xml.etree.ElementTree as ET

csv_file = "/home/bleak/Desktop/Global-water/data/cleaned_global_water_consumption.csv"
xml_file = "/home/bleak/Desktop/Global-water/data/water_crisis.xml"
json_file = "/home/bleak/Desktop/Global-water/data/water_policies.json"

db_file = "/home/bleak/Desktop/Global-water/global_water.db"
conn = sqlite3.connect(db_file)
cursor = conn.cursor()

df_csv = pd.read_csv(csv_file)

# Clean CSV Data
df_csv.columns = df_csv.columns.str.strip().str.lower().str.replace(" ", "_")  
df_csv.drop_duplicates(inplace=True)  
df_csv.fillna("", inplace=True)  

df_csv.to_sql("water_consumption", conn, if_exists="replace", index=False)

tree = ET.parse(xml_file)
root = tree.getroot()

data_xml = []
for country in root.findall("Country"):
    country_name = country.get("name")
    year = int(country.get("year"))
    crisis_severity = country.find("CrisisSeverity").text
    mitigation_efforts = country.find("MitigationEfforts").text

    data_xml.append([country_name, year, crisis_severity, mitigation_efforts])

df_xml = pd.DataFrame(data_xml, columns=["country", "year", "crisis_severity", "mitigation_efforts"])

df_xml.drop_duplicates(inplace=True)
df_xml["crisis_severity"] = df_xml["crisis_severity"].str.lower()
df_xml["mitigation_efforts"] = df_xml["mitigation_efforts"].str.lower()

df_xml.to_sql("water_crisis", conn, if_exists="replace", index=False)

with open(json_file, "r") as file:
    data_json = json.load(file)

data_json_flat = []
for record in data_json:
    country = record["Country"]
    year = record["Year"]
    policy = record["Policy"]
    investment = record["Investment (Million USD)"]
    regulations = ", ".join(record["Regulations"])  

    data_json_flat.append([country, year, policy, investment, regulations])

df_json = pd.DataFrame(data_json_flat, columns=["country", "year", "policy", "investment_million_usd", "regulations"])


df_json.drop_duplicates(inplace=True)
df_json["policy"] = df_json["policy"].str.lower()
df_json["regulations"] = df_json["regulations"].str.lower()

df_json.to_sql("water_policies", conn, if_exists="replace", index=False)


cursor.execute("CREATE INDEX IF NOT EXISTS idx_water_consumption_country ON water_consumption(country);")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_water_crisis_country_year ON water_crisis(country, year);")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_water_policies_country_year ON water_policies(country, year);")


conn.commit()
conn.close()

print("Data Integration and Transformation Completed Successfully!")
