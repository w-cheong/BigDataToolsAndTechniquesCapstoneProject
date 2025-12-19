import os
import logging
from pymongo import MongoClient
from dotenv import load_dotenv


def main():
    load_dotenv()

    # Logging setup
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
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

    clean_col = db["accidents_clean"]
    agg_col = db["accidents_aggregated"]

    # ----------------------------------------------------------
    # QUERY MODELING / INDEXING
    #   Aggregation by State and Severity
    #   Streamlit dashboard filtering
    # ----------------------------------------------------------

    logger.info("Creating index on accidents_clean (State, Severity)")
    clean_col.create_index(
        [("State", 1), ("Severity", 1)],
        name="idx_clean_state_severity"
    )

    logger.info("Creating index on accidents_aggregated (State, Severity)")
    agg_col.create_index(
        [("State", 1), ("Severity", 1)],
        name="idx_agg_state_severity"
    )

    logger.info("Query modeling indexes created successfully")


if __name__ == "__main__":
    main()
