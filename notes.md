Project Overview

This project involved cleaning, transforming, and integrating multiple datasets (CSV, XML, JSON) related to global water consumption, policies, and crises. The final dataset was stored in PostgreSQL, ensuring consistency and accessibility for future analysis.
Data Sources

We worked with three datasets in different formats:

    cleaned_global_water_consumption.csv → CSV format
    water_crisis.xml → XML format
    water_policies.json → JSON format

Each dataset contained different but related information on water usage, crisis severity, and governmental policies.
Step-by-Step Breakdown of the Workflow
Step 1: Data Loading & Standardization

Script: load_and_standardize.py

Why?

    Different file formats (CSV, XML, JSON) required standardization.
    Column names needed to be uniform across datasets.
    Data types (e.g., numbers, text) needed consistency.

What We Did:

    Loaded CSV, XML, and JSON data into pandas DataFrames.
    Extracted relevant data from XML & JSON.
    Standardized column names.
    Converted severity levels into numeric values:
        Low = 1, Medium = 2, High = 3
    Standardized country names and date formats.

Outcome: Clean, structured CSV, XML, and JSON files ready for further processing.
Step 2: Data Cleaning

Script: clean_data.py

Why?

    Missing values and inconsistencies could impact analysis.
    Duplicate records needed removal.
    Different investment values had formatting issues.

What We Did:

    Filled missing values (e.g., "Unknown" for mitigation efforts).
    Dropped duplicates to ensure unique records.
    Rounded investment values for consistency.
    Ensured all columns had expected data types (e.g., numbers stayed numeric).

Outcome: Clean, deduplicated datasets with no missing values.
Step 3: Data Transformation

Script: transform_data.py

Why?

    Some fields needed transformation before merging.
    Regulations were stored as lists in JSON, which PostgreSQL doesn’t support.

What We Did:

    Converted JSON arrays into comma-separated strings ("Agriculture, Industry, Household").
    Ensured all numerical data was properly formatted.
    Mapped column names for consistency.

Outcome: Transformed datasets ready for integration.
Step 4: Data Merging

Script: merge_data.py

Why?

    The datasets shared common keys (Country, Year) but contained different aspects of water data.
    We needed a single unified dataset.

What We Did:

    Merged all three datasets using Country and Year.
    Ensured no missing data after merging.
    Saved the final cleaned dataset as final_water_data.csv.

Outcome: Fully integrated dataset ready for database storage.
Step 5: Database Setup & Data Insertion

Script: load_to_postgres.py

Why?

    We needed a structured relational database for future queries.
    PostgreSQL provides scalability and robust querying capabilities.
    Old data needed removal before inserting the new version.

What We Did:

    Dropped the old database (global_water) to ensure a clean slate.
    Created a new PostgreSQL database with a properly structured table.
    Mapped CSV column names to match the PostgreSQL table structure.
    Inserted data row by row into PostgreSQL.

Outcome: PostgreSQL now contains fully cleaned, structured water data.
