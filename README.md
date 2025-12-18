# BigDataToolsAndTechniquesCapstoneProject

This is the capstone project for the Big Data Tools and Techniques course. We are utilizing MongoDB Atlas and Python to examine, clean, aggregate, and visualize information from a dataset.

# Dependencies
- pymongo
- streamlit
- plotly

# Architecture of Capstone
US Accidents Dataset  
&emsp;&emsp;&emsp;&emsp;↓  
Ingestion (Python)  
&emsp;&emsp;&emsp;&emsp;↓  
MongoDB Atlas  
&emsp;&emsp;&emsp;&emsp;|- *accidents_raw   (Bronze / Raw)  
&emsp;&emsp;&emsp;&emsp;|- *accidents_clean (Silver / Clean)  
&emsp;&emsp;&emsp;&emsp;|- *accidents_aggregated (Gold / Aggregated)  
&emsp;&emsp;&emsp;&emsp;↓  
Streamlit Dashboard

# Query Modeling & Performance Optimization

To improve query performance on large-scale datasets (7+ million records),
we implemented query modeling using MongoDB indexes.

Indexes were created on commonly queried fields to optimize filtering,
grouping, and aggregation operations.

# Indexes Created
- accidents_clean:
  - Compound index on (State, Severity)
- accidents_aggregated:
  - Compound index on (State, Severity)

These indexes improve:
- Aggregation performance
- Streamlit dashboard filtering
- Read/query efficiency on large collections



# How the Pipeline Works
1. Data Ingestion (Bronze Layer)
* ingest_accidents.py loads the raw dataset.
* Data is stored without modification in *accidents_raw.

2. Data Reading and Schema Read
* db_row_count_schema details row count and summarizes schema through sampling.

3. Schema Validation (Silver Layer)

* validate_accidents_schema.py checks raw data structure.
* Validation results are saved in schema_validation_results.txt.

4. Data Cleaning (Silver Layer)

* silver_cleaning.py handles cleaning the data:
    * Converts date/time columns
    * Converts numeric fields
    * Removes invalid or missing records
    * Cleaned data is saved into *accidents_clean.


5. Data Aggregation (Gold Layer)

- aggregation.py uses MongoDB aggregation pipelines.
- Data is grouped by State and Severity.
- Metrics include:
    - Accident count
    - Average distance
    - Average temperature
- Results are saved to *accidents_aggregated.

6. Data Visualization 
- streamlit_app.py reads from the aggregated collection only.
- Provides interactive charts and filters for analysis.

# Execution Order (Run each scripts in the following order)
1. ingest_accidents.py  -> python ingest_accidents.py

2. db_row_count_schema -> python db_row_count_schema.py

3. validate_accidents_schema.py -> python validate_accidents_schema.py

4. silver_cleaning.py -> python silver_cleaning.py

5. aggregation.py -> python aggregation.py

6. streamlit_app.py -> python -m streamlit run streamlit_app.py 
