import os
from pymongo import MongoClient
from dotenv import load_dotenv


def test_mongo_connection_ping():
    """
    Test 1: Proves we can connect to MongoDB Atlas using MONGO_URI
    """
    load_dotenv()
    uri = os.getenv("MONGO_URI")
    assert uri is not None and uri != "", "MONGO_URI missing."

    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    # If ping fails, pytest will fail
    client.admin.command("ping")
