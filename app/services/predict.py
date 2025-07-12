from typing import Optional
from datetime import datetime

from pymongo import MongoClient
import re

from app.core.config import settings

def get_hpi_by_month(month_of_transfer: str, local_authority: str, property_type: str) -> Optional[float]:
    """
    Query MongoDB for HPI index by month, local authority, and property type.

    Args:
        month_of_transfer (str): Format 'YYYY-MM' (e.g., '1995-02')
        local_authority (str): Name of the local authority (e.g., 'Adur')
        property_type (str): One of ['Terraced', 'Detached', 'SemiDetached', 'Flat']

    Returns:
        Optional[float]: HPI index value if found, else None.

    Raises:
        ValueError: If month_of_transfer format or property_type is invalid.
    """
    try:
        datetime.strptime(month_of_transfer, '%Y-%m')
    except ValueError:
        raise ValueError("month_of_transfer must be in 'YYYY-MM' format")

    valid_property_types = {'Terraced', 'Detached', 'SemiDetached', 'Flat'}
    if property_type not in valid_property_types:
        raise ValueError(f"property_type must be one of {valid_property_types}")

    client = MongoClient(settings.DATABASE_URL)
    try:
        db = client["time_seriese_db"]
        collection = db["external_data_local_authorities"]

        query = {
            "RegionName": {"$regex": f"^{re.escape(local_authority.strip().lower())}$", "$options": "i"},
            "Date": f"{month_of_transfer}-01"
        }

        result = collection.find_one(query)
        if not result:
            return None

        return result.get(f"{property_type}Index")
    except Exception:
        return None
    finally:
        client.close()

def get_hpi_forecast(local_authority: str, property_type: str) -> Optional[float]:
    """
    Query MongoDB for HPI forecast by local authority and property type.

    Args:
        local_authority (str): Name of the local authority (e.g., 'Adur')
        property_type (str): One of ['Terraced', 'Detached', 'SemiDetached', 'Flat']

    Returns:
        Optional[float]: Forecasted HPI value if found, else None.

    Raises:
        ValueError: If property_type is invalid.
    """
    valid_property_types = {'Terraced', 'Detached', 'SemiDetached', 'Flat'}
    if property_type not in valid_property_types:
        raise ValueError(f"property_type must be one of {valid_property_types}")

    client = MongoClient(settings.DATABASE_URL)
    try:
        db = client["time_seriese_db"]
        collection = db["forecast"]

        query = {
            "local_authority": {"$regex": f"^{re.escape(local_authority.strip().lower())}$", "$options": "i"},
            "property_type": f"{property_type}Index"
        }

        result = collection.find_one(query)
        if not result:
            return None

        return result.get("forecast")
    except Exception:
        return None
    finally:
        client.close()

def valuation(history_price: float, last_hpi: float, latest_hpi: float) -> float:
    """
    Calculate property valuation based on historical price and HPI values.

    Args:
        history_price (float): Historical price per square meter
        last_hpi (float): Historical HPI index
        latest_hpi (float): Latest HPI index

    Returns:
        float: Calculated valuation
    """
    if last_hpi == 0:
        return 0
    return history_price * latest_hpi / last_hpi