import os
import pandas as pd
from src.utils.db import get_db

def ingest_stocks():
    db = get_db()
    collection = db["stocks_raw"]

    raw_path = os.path.join("data", "raw", "stocks")

    print("Loading CSV files from:", raw_path)

    for file in os.listdir(raw_path):
        if file.endswith(".csv"):
            file_path = os.path.join(raw_path, file)

            print(f"Ingesting: {file}")

            df = pd.read_csv(file_path)

            # CONVERT RAW DATA ROWS TO DICTIONARIES
            records = df.to_dict(orient="records")

            if records:
                collection.insert_many(records)

    print("Ingestion Completed")

if __name__ == "__main__":
    ingest_stocks()
