from pydantic import BaseModel, Field, ValidationError
from datetime import datetime
import os
from pymongo import MongoClient
import pandas as pd
import logging
from dotenv import load_dotenv
from typing import Optional


def main():
    load_dotenv()

    # logging setup .txt file
    LOG_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "schema_validation_results.txt"
)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(LOG_PATH),
            logging.StreamHandler()
        ]
    )
    logger = logging.getLogger(__name__)

    # MongoDB connection
    MONGO_URI = os.getenv("MONGO_URI")
    if not MONGO_URI:
        raise ValueError("MONGO_URI not found in .env")

    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    logger.info("Connected to MongoDB Atlas")

    db = client["bigdata_capstone"]
    col = db["accidents_clean"] # Data after cleaning folder

    # ----------------------------------------------------------
    # Pydantic SCHEMA USED TO VALIDATE
    # ----------------------------------------------------------
    class accident_info(BaseModel):
        ID: str
        Source: Optional[str] = None
        Severity: int = Field(ge=1, le=4)

        Start_Time: datetime
        End_Time: datetime
        Weather_Timestamp: Optional[datetime] = None

        Start_Lat: float = Field(ge=-90, le=90)
        Start_Lng: float = Field(ge=-180, le=180)
        End_Lat: Optional[float] = Field(default=None, ge=-90, le=90)
        End_Lng: Optional[float] = Field(default=None, ge=-180, le=180)

        Distance_mi: float = Field(default=None, alias="Distance(mi)")

        City: Optional[str] = None
        County: Optional[str] = None
        State: str = Field(default=None, min_length=2, max_length=2)
        Zipcode: Optional[str] = None
        Country: str = None
        Timezone: Optional[str] = None

        Weather_Condition: Optional[str] = None
        Temperature_F: Optional[float] = Field(default=None, alias="Temperature(F)")
        Visibility_mi: Optional[float] = Field(default=None, alias="Visibility(mi)")
        Wind_Speed_mph: Optional[float] = Field(default=None, alias="Wind_Speed(mph)")

        Traffic_Signal: Optional[bool] = None
        Junction: Optional[bool] = None


    # Validate documents 
    valid_count = 0
    invalid_count = 0
    processed = 0

    BATCH_SIZE = 5000  # <----- Increasing number may affect performance 
    batch = []

    # -----------------------------------
    # MONGO DB DATA TO PYTHON IN BATCHES
    # -----------------------------------
    cursor = col.find({}, no_cursor_timeout=True).batch_size(BATCH_SIZE)

    logger.info("Starting schema validation")

    for doc in cursor:
        # remove Mongo _id before validation
        doc.pop("_id", None)
        batch.append(doc)

        # When batch is full, validate the batch
        if len(batch) >= BATCH_SIZE:
            df = pd.DataFrame(batch)

            for record in df.to_dict(orient="records"):
                try:
                    accident_info(**record)
                    valid_count += 1
                except ValidationError as e:
                    invalid_count += 1
                    logger.warning(
                        f"Schema validation failed for ID={record.get('ID')} | errors={e.errors()[:2]}"
                    )

            processed += len(batch)
            logger.info(f"Processed: {processed:,} | Valid: {valid_count:,} | Invalid: {invalid_count:,}")
            batch = []

    # Validate any remaining docs after running the batch count
    if batch:
        df = pd.DataFrame(batch)

        for record in df.to_dict(orient="records"):
            try:
                accident_info(**record)
                valid_count += 1
            except ValidationError as e:
                invalid_count += 1
                logger.warning(
                    f"Schema validation failed for ID={record.get('ID')} | errors={e.errors()[:2]}"
                )

        processed += len(batch)

    logger.info("Schema Validation Complete")
    logger.info(f"Total processed: {processed:,}")
    logger.info(f"Valid records: {valid_count:,}")
    logger.info(f"Invalid records: {invalid_count:,}")


if __name__ == "__main__":
    main()
