import psycopg2
import pandas as pd

DB_PARAMS = {
    "dbname": "water_consumption",
    "user": "postgres",
    "password": "root",
    "host": "localhost",
    "port": "5432"
}

csv_file = "./data/cleaned_transformed_water_data.csv"
df = pd.read_csv(csv_file)

df.rename(columns={"Country": "region"}, inplace=True)

def connect_db():
    return psycopg2.connect(**DB_PARAMS)

def create_table():
    with connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute('''
                CREATE TABLE IF NOT EXISTS water_usage (
                    id SERIAL PRIMARY KEY,
                    region TEXT,
                    year INTEGER,
                    usage_type TEXT,
                    consumption FLOAT NULL,
                    unit TEXT
                );
            ''')
            cur.execute('''
                CREATE TABLE IF NOT EXISTS water_scarcity (
                    id SERIAL PRIMARY KEY,
                    region TEXT,
                    year INTEGER,
                    scarcity_level TEXT
                );
            ''')
            conn.commit()

def insert_data():
    with connect_db() as conn:
        with conn.cursor() as cur:
            for _, row in df.iterrows():
                data = [
                    ("Total Water Consumption", row["Total Water Consumption (Billion Cubic Meters)"], "Billion Cubic Meters"),
                    ("Per Capita Water Use", row["Per Capita Water Use (Liters per Day)"], "Liters per Day"),
                    ("Agricultural Water Use", row["Agricultural Water Use (%)"], "%"),
                    ("Industrial Water Use", row["Industrial Water Use (%)"], "%"),
                    ("Household Water Use", row["Household Water Use (%)"], "%"),
                    ("Rainfall Impact", row["Rainfall Impact (Annual Precipitation in mm)"], "mm"),
                    ("Groundwater Depletion Rate", row["Groundwater Depletion Rate (%)"], "%"),
                    ("Water Stress Index", row["Water Stress Index"], "Index"),
                ]
                
                for usage_type, consumption, unit in data:
                    if isinstance(consumption, (int, float)) or str(consumption).replace(".", "").isdigit():
                        cur.execute('''
                            INSERT INTO water_usage (region, year, usage_type, consumption, unit)
                            VALUES (%s, %s, %s, %s, %s);
                        ''', (row["region"], row["Year"], usage_type, float(consumption), unit))

                if isinstance(row["Water Scarcity Level"], str):
                    cur.execute('''
                        INSERT INTO water_scarcity (region, year, scarcity_level)
                        VALUES (%s, %s, %s);
                    ''', (row["region"], row["Year"], row["Water Scarcity Level"]))

        conn.commit()

if __name__ == "__main__":
    create_table()
    insert_data()
    print("Data successfully inserted into PostgreSQL!")
