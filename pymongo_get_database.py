import os
from pymongo import MongoClient
import pandas as pd
import logging 
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logging.getLogger(__name__)

MONGO_URI = os.getenv("MONGO_URI")

client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)

client.admin.command("ping")
logging.info("Connected to MongoDB Atlas")

# access database 
db = client["bigdata_capstone"]
# access table
col = db["stocks_raw"]

df = pd.DataFrame(col.find())
print(df)
logging.info("dataframe table done")

table_count = df.shape[0]
print(table_count)
logging.info("row count and schema")


