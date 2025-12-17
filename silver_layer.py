from pydantic import BaseModel, Field, ValidationError
from datetime import datetime
import os
from pymongo import MongoClient
import pandas as pd
import logging 
from dotenv import load_dotenv

def main():
    load_dotenv()

    logging.basicConfig(level=logging.INFO)
    logging.getLogger(__name__)

    MONGO_URI = os.getenv("MONGO_URI")

    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)

    client.admin.command("ping")
    logging.info("Connected to MongoDB Atlas")

    #data from bronze layer
    db = client["bigdata_capstone"]

    
    col = db["stocks_raw"]

    df = pd.DataFrame(col.find())


    # use pydantic BaseModel to validate the schema
    class stock_info(BaseModel):
        _id: int
        Date: datetime
        Open: float
        High: float
        Low: float
        Close: float
        Adj_Close: float = Field(alias='Adj Close')
        Volume: float




if __name__ == "__main__":
    main()