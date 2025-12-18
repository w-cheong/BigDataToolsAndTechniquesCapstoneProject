import os
from pymongo import MongoClient
from dotenv import load_dotenv


def test_aggregated_layer_has_expected_fields():
    """
    Test 3: Proves the GOLD layer exists and documents have the expected fields
    """
    load_dotenv()
    uri = os.getenv("MONGO_URI")
    assert uri is not None and uri != "", "MONGO_URI missing."

    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    db = client["bigdata_capstone"]
    agg = db["accidents_aggregated"]

    doc = agg.find_one({})
    assert doc is not None, "accidents_aggregated is empty. Run aggregation.py first."

    # fields created after aggregation
    required_fields = ["State", "Severity", "accident_count", "avg_distance", "avg_temperature"]
    for field in required_fields:
        assert field in doc, f"Missing field '{field}' in aggregated doc. Found keys: {list(doc.keys())}"
