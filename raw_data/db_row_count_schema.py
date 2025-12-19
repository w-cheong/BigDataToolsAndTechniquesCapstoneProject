import os
from pymongo import MongoClient
# import pandas as pd
import logging
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logging.getLogger(__name__)

MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    raise ValueError("MONGO_URI not found. Make sure .env exists and has MONGO_URI=...")

client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)

client.admin.command("ping")
logging.info("Connected to MongoDB Atlas")


DB_NAME = os.getenv("DB_NAME", "bigdata_capstone")
COL_NAME = os.getenv("COL_NAME", "accidents_raw")  
SAMPLE_SIZE = int(os.getenv("SAMPLE_SIZE", "10"))

db = client[DB_NAME]
col = db[COL_NAME]
logging.info(f"Using DB={DB_NAME} | Collection={COL_NAME}")


# The following used to be how we showed schema in the code.
# Unfortunately it takes too long with the new dataset
# df = pd.DataFrame(col.find())
# print(df.shape[0])
# logging.info("dataframe table done")

# pymongo built in way to get row count from database
row_count = col.count_documents({})
print(f"row_count: {row_count}")

logging.info("row count found")

# sampling documents for schema read
print("\n--- SAMPLE DOCUMENTS (limited) ---")
sample_docs = list(col.find({}, {"_id": 0}).limit(SAMPLE_SIZE))
for i, doc in enumerate(sample_docs, start=1):
    print(f"\n[{i}] {doc}")

def _type_name(v):
    if v is None:
        return "None"
    return type(v).__name__

schema = {}
for doc in sample_docs:
    for k, v in doc.items():
        schema.setdefault(k, set()).add(_type_name(v))

print("\n--- INFERRED SCHEMA (from sample) ---")
for field in sorted(schema.keys()):
    types_seen = ", ".join(sorted(schema[field]))
    print(f"{field}: {types_seen}")

logging.info("schema collected")

print(f"\nSchema inferred from SAMPLE_SIZE={SAMPLE_SIZE}. (Not the entire collection.)")
