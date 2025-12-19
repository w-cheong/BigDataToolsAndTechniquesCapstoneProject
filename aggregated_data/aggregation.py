from pymongo import MongoClient
import logging
import os
from dotenv import load_dotenv

def main():
    # logging setup
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    load_dotenv()

    MONGO_URI = os.getenv("MONGO_URI")
    if not MONGO_URI:
        raise ValueError("MONGO_URI not found in .env")

    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    logging.info("Connected to MongoDB Atlas")

    db = client["bigdata_capstone"]

    # Input data source 
    clean_col = db["accidents_clean"]

    # Output aggregated data
    agg_col = db["accidents_aggregated"]

    logging.info("Connected to MongoDB collections")

    # ------------------------------------------------------------
    # AGGREGATION PIPELINE:
    #   Aggregate accident data by State and Severity
    #   Compute count, average distance, and average temperature
    # ------------------------------------------------------------
    pipeline = [
        
        {
            "$match": {
                "State": {"$ne": None},
                "Severity": {"$ne": None}
            }
        },
        {
            "$group": {
                "_id": {
                    "State": "$State",
                    "Severity": "$Severity"
                },
                "accident_count": {"$sum": 1},
                "avg_distance": {"$avg": "$Distance(mi)"},
                "avg_temperature": {"$avg": "$Temperature(F)"}
            }
        },
        {
            "$project": {
                "_id": 0,
                "State": "$_id.State",
                "Severity": "$_id.Severity",
                "accident_count": 1,
                "avg_distance": 1,
                "avg_temperature": 1
            }
        },
        {
            "$sort": {
                "State": 1,
                "Severity": 1
            }
        }
    ]

    logging.info("Starting aggregation pipeline")

    # Execute aggregation
    results = list(clean_col.aggregate(pipeline, allowDiskUse=True))

    logging.info(f"Aggregation completed. Records created: {len(results)}")

    # Save aggregated data
    logging.info("Saving aggregated data to accidents_aggregated collection")

    # Clear old aggregated data
    agg_col.delete_many({})

    # Insert new aggregated records
    if results:
        agg_col.insert_many(results)

    logging.info("Aggregation process completed successfully")

if __name__ == "__main__":
    main()
