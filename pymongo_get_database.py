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
col = db["accidents_raw"]

## The following used to be how we showed schema in the code. 
## Unfortunately it takes too long with the new dataset
# df = pd.DataFrame(col.find())
# print(df.shape[0])
# logging.info("dataframe table done")

# pymongo built in way to get row count from database
row_count = col.count_documents({})
print(f"row_count: {row_count}")

logging.info("row count and schema")


