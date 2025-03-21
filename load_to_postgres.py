import pandas as pd
import psycopg2
import os
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

DB_PARAMS = {
    "dbname": os.getenv("DB_NAME", "postgres"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "root"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432")
}

DB_NAME = "global_water"

final_data_path = "data/enriched_water_data.csv"
df = pd.read_csv(final_data_path)

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
df.rename(columns=column_mapping, inplace=True)

try:
    conn = psycopg2.connect(**DB_PARAMS)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    cur.execute(f"""
        SELECT pg_terminate_backend(pg_stat_activity.pid)
        FROM pg_stat_activity
        WHERE pg_stat_activity.datname = '{DB_NAME}'
          AND pid <> pg_backend_pid();
    """)
    cur.execute(f"DROP DATABASE IF EXISTS {DB_NAME};")
    cur.execute(f"CREATE DATABASE {DB_NAME};")
    cur.close()
    conn.close()
except Exception as e:
    print("Error during database reset:", e)

DB_PARAMS["dbname"] = DB_NAME
try:
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS water_data;")
    create_table_query = """
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
    """
    cur.execute(create_table_query)
    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO water_data (
                Country, Year, WaterConsumption, PerCapitaWaterUse, AgriculturalWaterUse,
                IndustrialWaterUse, HouseholdWaterUse, RainfallImpact, GroundwaterDepletionRate,
                CrisisSeverity, MitigationEfforts, Policy, Investment, Regulations,
                consumption_rainfall_ratio, investment_consumption_ratio, groundwater_stress,
                usage_balance, water_stress_index
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """, (
            row["Country"], row["Year"], row.get("WaterConsumption", None), row.get("PerCapitaWaterUse", None),
            row.get("AgriculturalWaterUse", None), row.get("IndustrialWaterUse", None), row.get("HouseholdWaterUse", None),
            row.get("RainfallImpact", None), row.get("GroundwaterDepletionRate", None),
            row["CrisisSeverity"], row["MitigationEfforts"], row["Policy"],
            row.get("Investment", None), row["Regulations"],
            row.get("consumption_rainfall_ratio", None), row.get("investment_consumption_ratio", None),
            row.get("groundwater_stress", None), row.get("usage_balance", None),
            row.get("water_stress_index", None)
        ))
    conn.commit()
    cur.close()
    conn.close()
except Exception as e:
    print("Error during data load:", e)