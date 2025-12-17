from pymongo import MongoClient
import pandas as pd

# MongoDB connection string
uri = "mongodb+srv://salgad58_db_user:BJQVcwi0QhdtDUhG@bigdata-capstone.vox06lb.mongodb.net/?appName=bigdata-capstone"
client = MongoClient(uri)

db = client["stock_market"]
collection = db["AAPL"]

df = pd.DataFrame(list(collection.find()))
print(df.shape[0])
