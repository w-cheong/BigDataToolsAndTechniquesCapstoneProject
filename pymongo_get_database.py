import os
from pymongo import MongoClient
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")

client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)

client.admin.command("ping")
print("Connected to MongoDB Atlas")

db = client["bigdata_capstone"]
col = db["stocks_raw"]

print("Document count:", col.estimated_document_count())

df = pd.DataFrame(list(col.find().limit(5)))
print(df)


