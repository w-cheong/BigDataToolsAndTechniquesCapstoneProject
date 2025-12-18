import os
from pymongo import MongoClient
from dotenv import load_dotenv


def test_raw_layer_has_required_volume():
    """
    Test 2: Proves the RAW layer exists and contains > 1,000,000 documents.
    """
    load_dotenv()
    uri = os.getenv("MONGO_URI")
    assert uri is not None and uri != "", "MONGO_URI missing"

    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    db = client["bigdata_capstone"]
    raw = db["accidents_raw"]

    row_count = raw.count_documents({})
    assert row_count >= 1_000_000, f"Expected >= 1,000,000 docs in accidents_raw, got {row_count:,}"
