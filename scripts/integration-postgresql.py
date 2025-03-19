import pandas as pd
import psycopg2
import json
import xml.etree.ElementTree as ET
from sqlalchemy import create_engine

DB_PARAMS = {
    "dbname": "global_water",
    "user": "postgres",
    "password": "root",
    "host": "localhost",
    "port": "5432"
}

DB_URI = f"postgresql+psycopg2://{DB_PARAMS['user']}:{DB_PARAMS['password']}@{DB_PARAMS['host']}:{DB_PARAMS['port']}/{DB_PARAMS['dbname']}"
engine = create_engine(DB_URI)

csv_file = "/home/bleak/Desktop/data-int/data/cleaned_global_water_consumption.csv"
xml_file = "/home/bleak/Desktop/data-int/data/water_crisis.xml"
json_file = "/home/bleak/Desktop/data-int/data/water_policies.json"

conn = psycopg2.connect(**DB_PARAMS)
cursor = conn.cursor()

df_csv = pd.read_csv(csv_file)

df_csv.columns = df_csv.columns.str.strip().str.lower().str.replace(" ", "_")  
df_csv.drop_duplicates(inplace=True)  
df_csv.fillna("", inplace=True)  

cursor.execute("""
    CREATE TABLE IF NOT EXISTS water_consumption (
        country TEXT,
        year INT,
        consumption FLOAT
    );
""")
conn.commit() 

df_csv.to_sql("water_consumption", engine, if_exists="replace", index=False, method="multi")

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

cursor.execute("""
    CREATE TABLE IF NOT EXISTS water_crisis (
        country TEXT,
        year INT,
        crisis_severity TEXT,
        mitigation_efforts TEXT
    );
""")
conn.commit()

df_xml.to_sql("water_crisis", engine, if_exists="replace", index=False, method="multi")

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

cursor.execute("""
    CREATE TABLE IF NOT EXISTS water_policies (
        country TEXT,
        year INT,
        policy TEXT,
        investment_million_usd FLOAT,
        regulations TEXT
    );
""")
conn.commit()

df_json.to_sql("water_policies", engine, if_exists="replace", index=False, method="multi")

cursor.execute("CREATE INDEX IF NOT EXISTS idx_water_consumption_country ON water_consumption(country);")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_water_crisis_country_year ON water_crisis(country, year);")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_water_policies_country_year ON water_policies(country, year);")

conn.commit()
cursor.close()
conn.close()

print("Data Integration and Transformation Completed Successfully!")
