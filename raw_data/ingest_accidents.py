import os
import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    raise ValueError("MONGO_URI not found. Make sure .env is in the capstone folder.")

DB_NAME = "bigdata_capstone"
COL_NAME = "accidents_raw"
CSV_PATH = os.path.join("data", "raw", "US_Accidents_March23.csv")
CHUNK_SIZE = 50_000   # <-------- Keep chunk size below 100k



def main():
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
    client.admin.command("ping")
    print("Connected to MongoDB")

    db = client[DB_NAME]
    col = db[COL_NAME]

    total = 0

    for chunk in pd.read_csv(CSV_PATH, chunksize=CHUNK_SIZE, low_memory=False):
        # NULL STORAGE
        chunk = chunk.where(pd.notnull(chunk), None)

        records = chunk.to_dict("records")
        if records:
            col.insert_many(records, ordered=False)
            total += len(records)
            print(f"Inserted so far: {total:,}")

    print(f"Ingestion Succesful. Total inserted: {total:,}")
    print(f"Collection: {DB_NAME}.{COL_NAME}")

if __name__ == "__main__":
    main()
