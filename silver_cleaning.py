import os
import pandas as pd
from datetime import datetime
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

# --------------------------------------------------------------
# DATABASE CONFIGURATION
# --------------------------------------------------------------
MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    raise ValueError("MONGO_URI not found in .env")

DB_NAME = "bigdata_capstone"
RAW_COL = "accidents_raw"
CLEAN_COL = "accidents_clean"
MAX_RECORDS = 1_000_000



# --------------------------------------------------------------
# TEXT NORMALIZATION HELPER FUNCTIONS
# -------------------------------------------------------------
# Trimming leading/trailing whitespace
def norm_text(x):
    if x is None:
        return None

    if isinstance(x, str):
        s = x.strip()
        if s == "" or s.lower() in {"na", "n/a", "none", "null", "unknown"}:
            return None
        return s

    return x

# Convert date/time strings into Python datetime objects.
def parse_dt(x):
    if x is None:
        return None

    if isinstance(x, datetime):
        return x

    dt = pd.to_datetime(x, errors="coerce")
    if pd.isna(dt):
        return None

    return dt.to_pydatetime()

# prevents MongoDB from storing NaN values
def nan_to_none(x):
    if isinstance(x, float) and pd.isna(x):
        return None
    return x


# --------------------------------------------------------------
# MAIN CLEANING LOGIC 
# Reads from accidents_raw and writes cleaned data into
# accidents_clean
# --------------------------------------------------------------

def main():
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
    client.admin.command("ping")
    print("Connected to MongoDB successfully!")

    db = client[DB_NAME]
    raw = db[RAW_COL]
    clean = db[CLEAN_COL]

    # Clean startto prevent duplicate data on re-runs
    clean.delete_many({})
    print("Cleared accidents_clean")

    # Track IDs to avoid duplicates
    seen_ids = set()

    # Counters for reporting
    inserted = 0
    duplicates = 0

    batch = []

    # Stream data from MongoDB in chunks
    cursor = raw.find({}, no_cursor_timeout=True).batch_size(5000)

    for doc in cursor:
        doc.pop("_id", None)

        # ------------------------------------------------------
        # CLEANING STEP 1: Convert float NaN values to None
        # ------------------------------------------------------
        for k, v in list(doc.items()):
            doc[k] = nan_to_none(v)

        # ------------------------------------------------------
        # CLEANING STEP 2: Normalize text fields
        # ------------------------------------------------------
        for field in ["City", "County", "State", "Weather_Condition",
                      "Wind_Direction", "Street", "Zipcode"]:
            if field in doc:
                doc[field] = norm_text(doc[field])

        # ------------------------------------------------------
        # CLEANING STEP 3: Converts string timestamps into objects
        # ------------------------------------------------------
        for dt_field in ["Start_Time", "End_Time", "Weather_Timestamp"]:
            if dt_field in doc:
                doc[dt_field] = parse_dt(doc[dt_field])

        # ------------------------------------------------------
        # CLEANING STEP 4: Remove duplicate records
        # ------------------------------------------------------
        accident_id = doc.get("ID")
        if not accident_id:
            continue

        if accident_id in seen_ids:
            duplicates += 1
            continue

        seen_ids.add(accident_id)

        # Add cleaned document to batch !!IMPORTANT MILLION LIMIT TRESHOLD HERE 
        batch.append(doc)
        inserted += 1

        if inserted >= MAX_RECORDS:
            if batch:
                clean.insert_many(batch, ordered=False)
                batch = []
            print(f"\nReached max records limit of {MAX_RECORDS}. Stopping cleaning.")
            break

        # Send new batches to MongoDB
        if len(batch) >= 5000:
            clean.insert_many(batch, ordered=False)
            batch = []
            print(f"Inserted clean: {inserted:,} | duplicates skipped: {duplicates:,}")

    # Insert any remaining documents after 5000 batch
    if batch:
        clean.insert_many(batch, ordered=False)

    print("\nCleaning finished")
    print(f"Inserted into accidents_clean: {inserted:,}")
    print(f"Duplicates skipped: {duplicates:,}")


if __name__ == "__main__":
    main()
