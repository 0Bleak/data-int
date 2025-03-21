import os
import pandas as pd
import psycopg2

DB_PARAMS = {
    "dbname": os.getenv("DB_NAME", "global_water"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "root"),
    "host": os.getenv("DB_HOST", "localhost"),  # "db" si Docker
    "port": os.getenv("DB_PORT", "5432")
}

conn = psycopg2.connect(**DB_PARAMS)
cur = conn.cursor()


df_crisis = pd.read_csv("data/cleaned_water_crisis.csv")

mitigation_mapping = {
    "Moderate conservation policies": 2,
    "Unknown": 0
}

cur.execute("DROP TABLE IF EXISTS water_crisis;")
cur.execute("""
CREATE TABLE water_crisis (
    country TEXT,
    year INT,
    crisisseverity INT,
    mitigationefforts INT
);
""")

for _, row in df_crisis.iterrows():
    try:
        csev = int(row["CrisisSeverity"])
    except:
        csev = None
    
    raw_effort = row.get("MitigationEfforts", "").strip()
    effort_val = mitigation_mapping.get(raw_effort, 0)

    cur.execute("""
    INSERT INTO water_crisis (
        country, year, crisisseverity, mitigationefforts
    ) VALUES (%s, %s, %s, %s);
    """, (
        row.get("Country"),
        row.get("Year"),
        csev,
        effort_val
    ))
conn.commit()
print("Data loaded into 'water_crisis' successfully.")


df_policies = pd.read_csv("data/transformed_water_policies.csv")
cur.execute("DROP TABLE IF EXISTS transformed_water_policies;")
cur.execute("""
CREATE TABLE water_policies (
    country TEXT,
    year INT,
    policy TEXT,
    "Investment (Million USD)" FLOAT,
    regulations TEXT
);
""")

for _, row in df_policies.iterrows():
    cur.execute("""
    INSERT INTO water_policies (
        country, year, policy, "Investment (Million USD)", regulations
    ) VALUES (%s, %s, %s, %s, %s);
    """, (
        row.get("Country"),
        row.get("Year"),
        row.get("Policy"),  
        row.get("Investment (Million USD)"),
        row.get("Regulations")
    ))
conn.commit()
print("Data loaded into 'water_policies' successfully.")



df_water = pd.read_csv("data/final_water_data.csv")

column_mapping = {
    "Total Water Consumption (Billion Cubic Meters)": "WaterConsumption",
    "Per Capita Water Use (Liters per Day)": "PerCapitaWaterUse",
    "Agricultural Water Use (%)": "AgriculturalWaterUse",
    "Industrial Water Use (%)": "IndustrialWaterUse",
    "Household Water Use (%)": "HouseholdWaterUse",
    "Rainfall Impact (Annual Precipitation in mm)": "RainfallImpact",
    "Groundwater Depletion Rate (%)": "GroundwaterDepletionRate",
    "Investment (Million USD)": "Investment"
}
df_water.rename(columns=column_mapping, inplace=True)

cur.execute("DROP TABLE IF EXISTS global_water_consumption;")
cur.execute("""
CREATE TABLE global_water_consumption (
    country TEXT,
    year INT,
    waterconsumption FLOAT,
    percapitawateruse FLOAT,
    agriculturalwateruse FLOAT,
    industrialwateruse FLOAT,
    householdwateruse FLOAT,
    rainfallimpact FLOAT,
    groundwaterdepletionrate FLOAT,
    crisisseverity INT,
    mitigationefforts TEXT,
    policy TEXT,
    investment FLOAT,
    regulations TEXT
);
""")

for _, row in df_water.iterrows():
    try:
        csev = int(row["CrisisSeverity"])
    except:
        csev = None
    
    cur.execute("""
    INSERT INTO global_water_consumption (
        country, year, waterconsumption, percapitawateruse,
        agriculturalwateruse, industrialwateruse, householdwateruse,
        rainfallimpact, groundwaterdepletionrate, crisisseverity,
        mitigationefforts, policy, investment, regulations
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """, (
        row.get("Country"),
        row.get("Year"),
        row.get("WaterConsumption"),
        row.get("PerCapitaWaterUse"),
        row.get("AgriculturalWaterUse"),
        row.get("IndustrialWaterUse"),
        row.get("HouseholdWaterUse"),
        row.get("RainfallImpact"),
        row.get("GroundwaterDepletionRate"),
        csev,
        row.get("MitigationEfforts"),  
        row.get("Policy"),             
        row.get("Investment"),
        row.get("Regulations")
    ))
conn.commit()
print("Data loaded into 'global_water_consumption' successfully.")



df_indicators = pd.read_csv("data/indicators.csv")
df_indicators.columns = [c.strip() for c in df_indicators.columns]

cur.execute("DROP TABLE IF EXISTS indicators;")
cur.execute("""
CREATE TABLE indicators (
    country TEXT,
    year INT,
    consumption_rainfall_ratio FLOAT,
    investment_consumption_ratio FLOAT,
    groundwater_stress FLOAT,
    usage_balance FLOAT,
    water_stress_index FLOAT,
    PRIMARY KEY (country, year)
);
""")

for _, row in df_indicators.iterrows():
    cur.execute("""
    INSERT INTO indicators (
        country, year,
        consumption_rainfall_ratio, investment_consumption_ratio,
        groundwater_stress, usage_balance, water_stress_index
    ) VALUES (%s, %s, %s, %s, %s, %s, %s);
    """, (
        row.get("Country"),
        row.get("Year"),
        row.get("consumption_rainfall_ratio"),
        row.get("investment_consumption_ratio"),
        row.get("groundwater_stress"),
        row.get("usage_balance"),
        row.get("water_stress_index")
    ))
conn.commit()
print("Data loaded into 'indicators' successfully.")


df_enriched = pd.read_csv("data/enriched_water_data.csv")
df_enriched.rename(columns={
    "Total Water Consumption (Billion Cubic Meters)": "WaterConsumption",
    "Per Capita Water Use (Liters per Day)": "PerCapitaWaterUse",
    "Agricultural Water Use (%)": "AgriculturalWaterUse",
    "Industrial Water Use (%)": "IndustrialWaterUse",
    "Household Water Use (%)": "HouseholdWaterUse",
    "Rainfall Impact (Annual Precipitation in mm)": "RainfallImpact",
    "Groundwater Depletion Rate (%)": "GroundwaterDepletionRate",
    "Investment (Million USD)": "Investment"
}, inplace=True)

cur.execute("DROP TABLE IF EXISTS water_data;")
cur.execute("""
CREATE TABLE water_data (
    Country TEXT,
    Year INT,
    WaterConsumption FLOAT,
    PerCapitaWaterUse FLOAT,
    AgriculturalWaterUse FLOAT,
    IndustrialWaterUse FLOAT,
    HouseholdWaterUse FLOAT,
    RainfallImpact FLOAT,
    GroundwaterDepletionRate FLOAT,
    CrisisSeverity INT,
    MitigationEfforts TEXT,
    Policy TEXT,
    Investment FLOAT,
    Regulations TEXT,
    consumption_rainfall_ratio FLOAT,
    investment_consumption_ratio FLOAT,
    groundwater_stress FLOAT,
    usage_balance FLOAT,
    water_stress_index FLOAT
);
""")

for _, row in df_enriched.iterrows():
    try:
        csev = int(row["CrisisSeverity"])
    except:
        csev = None

    cur.execute("""
    INSERT INTO water_data (
        Country, Year, WaterConsumption, PerCapitaWaterUse,
        AgriculturalWaterUse, IndustrialWaterUse, HouseholdWaterUse,
        RainfallImpact, GroundwaterDepletionRate, CrisisSeverity,
        MitigationEfforts, Policy, Investment, Regulations,
        consumption_rainfall_ratio, investment_consumption_ratio,
        groundwater_stress, usage_balance, water_stress_index
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """, (
        row.get("Country"),
        row.get("Year"),
        row.get("WaterConsumption"),
        row.get("PerCapitaWaterUse"),
        row.get("AgriculturalWaterUse"),
        row.get("IndustrialWaterUse"),
        row.get("HouseholdWaterUse"),
        row.get("RainfallImpact"),
        row.get("GroundwaterDepletionRate"),
        csev,
        row.get("MitigationEfforts"),
        row.get("Policy"),
        row.get("Investment"),
        row.get("Regulations"),
        row.get("consumption_rainfall_ratio"),
        row.get("investment_consumption_ratio"),
        row.get("groundwater_stress"),
        row.get("usage_balance"),
        row.get("water_stress_index")
    ))
conn.commit()
print("Data loaded into 'water_data' successfully.")


cur.close()
conn.close()
print("All four tables created and loaded successfully.")