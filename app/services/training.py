from typing import List, Dict
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from datetime import datetime
from loguru import logger
from app.core.config import settings


def flatten_forecast_data(nested_data: dict) -> list[dict]:
    flat_data = []
    for local_authority, property_data in nested_data.items():
        for property_type, metrics in property_data.items():
            flat_data.append({
                "local_authority": local_authority,
                "property_type": property_type,
                "mae": round(metrics["mae"], 4),
                "mape": round(metrics["mape"], 4),
                "forecast": round(metrics["forecast"], 6),
                "forecast_date": metrics["forecast_date"],
                "best_model": metrics["best_model"],
                "best_time_window": metrics["best_time_window"],
                "model_file": metrics["model_file"],
                "training_date": metrics['training_date']
            })
    return flat_data


def insert_forecast_to_mongodb(
    flat_data: List[Dict],
    db_name: str = "time_seriese_db",
    collection_name: str = "forecast"
) -> int:
    """
    Insert formatted forecast data into MongoDB.

    Args:
        flat_data (List[Dict]): List of flat forecast records.
        db_name (str): MongoDB database name.
        collection_name (str): MongoDB collection name.

    Returns:
        int: Number of records inserted.
    """
    if not flat_data:
        logger.warning("No forecast data provided for insertion.")
        return 0

    try:
        client = MongoClient(settings.DATABASE_URL)
        db = client[db_name]
        collection = db[collection_name]

        # Optional: add timestamp
        now = datetime.utcnow()
        for record in flat_data:
            record["created_at"] = now

        result = collection.insert_many(flat_data)
        logger.info(f"Inserted {len(result.inserted_ids)} forecast records into {collection_name}")
        return len(result.inserted_ids)

    except PyMongoError as e:
        logger.error(f"MongoDB insertion error: {e}")
        raise RuntimeError("Failed to insert forecast data into MongoDB.")

