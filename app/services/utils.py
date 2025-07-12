import re
from datetime import datetime
from typing import List, Dict

import pandas as pd
import requests
from bs4 import BeautifulSoup
from bson import ObjectId
from fastapi import HTTPException, status
from pymongo import MongoClient

from app.core.config import settings


async def search_local_authority_by_postcode(query: str, limit: int = 10) -> List[Dict]:
    if not query:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Postcode cannot be empty")

    client = MongoClient(settings.DATABASE_URL)
    try:
        db = client["time_seriese_db"]
        collection = db["local_authorities"]

        cursor = collection.find({
            "postcode": {"$regex": f"^{re.escape(query.strip().lower())}$", "$options": "i"}
        }).limit(limit)

        result = list(cursor)
        if not result:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                detail="No local authority found for the given postcode")

        return result
    finally:
        client.close()


async def search_external_data_by_local_authority(local_authority: str) -> List[Dict]:
    if not local_authority:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Local authority cannot be empty")

    client = MongoClient(settings.DATABASE_URL)
    try:
        db = client["time_seriese_db"]
        collection = db["external_data_local_authorities"]

        cursor = collection.find({
            "RegionName": {"$regex": f"^{re.escape(local_authority.strip().lower())}$", "$options": "i"}
        })

        result = [
            {key: str(value) if isinstance(value, ObjectId) else value for key, value in doc.items()}
            for doc in cursor
        ]

        if not result:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                detail=f"No data found for local authority: {local_authority}")

        return result
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error")
    finally:
        client.close()


def parse_table(soup: BeautifulSoup) -> List[List[str]]:
    try:
        table = soup.find("table", {"class": "table"})
        if not table:
            return []

        parsed_data = []
        for row in table.select("tr.d-none.d-sm-table-row"):
            cols = row.find_all("td")
            if len(cols) < 6:
                continue

            address = cols[0].get_text(strip=True)
            date_sold = cols[1].get_text(strip=True)
            price = cols[2].get_text(strip=True)

            area_m2_tag = cols[3].find("span", class_="unit-met")
            area_m2 = re.match(r'(\d+)', area_m2_tag.get_text(strip=True)).group(1) if area_m2_tag else ""

            price_per_m2_tag = cols[4].find("span", class_="unit-met")
            price_per_m2 = price_per_m2_tag.get_text(strip=True) if price_per_m2_tag else ""

            property_type = cols[5].get_text(strip=True)

            parsed_data.append([address, date_sold, price, area_m2, price_per_m2, property_type])

        return parsed_data
    except Exception:
        return []


def refactor_data(df: pd.DataFrame) -> List[Dict]:
    if df.empty:
        return []

    def clean_numeric(value: str) -> int:
        if not isinstance(value, str):
            value = str(value)
        cleaned = ''.join(filter(str.isdigit, value))
        return int(cleaned) if cleaned else 0

    def standardize_date(date_str: str) -> str:
        try:
            return datetime.strptime(date_str, '%d %b %Y').strftime('%Y-%m-%d')
        except ValueError:
            return date_str

    def map_property_type(property_type: str) -> str:
        if not property_type:
            return 'Flat'

        property_type = property_type.strip().split('(')[0].strip().lower()
        type_mapping = {
            'flat': 'Flat',
            'detached': 'Detached',
            'semidetached': 'Semi Detached',
            'terraced': 'Terraced',
            'terrace': 'Terraced',
            'semi detached': 'Semi Detached'
        }

        return next((value for key, value in type_mapping.items() if key in property_type), 'Flat')

    df_refactored = df.copy()
    df_refactored['date_of_transfer'] = df_refactored['date_of_transfer'].apply(standardize_date)
    df_refactored['price'] = df_refactored['price'].apply(clean_numeric)
    df_refactored['area'] = df_refactored['area'].apply(clean_numeric)
    df_refactored['property_type'] = df_refactored['property_type'].apply(map_property_type)

    df_refactored['price_per_m2'] = (df_refactored['price'] / df_refactored['area']).round(2)
    df_refactored['price_per_m2'] = df_refactored['price_per_m2'].fillna(0).replace(float('inf'), 0).astype(int)

    columns = ['address', 'date_of_transfer', 'price', 'area', 'price_per_m2', 'property_type']
    return df_refactored[columns].to_dict(orient='records')


def scrape(location: str) -> List[Dict]:
    if not location:
        return []

    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        url = f"https://housemetric.co.uk/results?str_input={location.strip().lower().replace(' ', '%20')}"
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        data = parse_table(soup)

        if not data:
            return []

        columns = ["address", "date_of_transfer", "price", "area", "price_per_m2", "property_type"]
        df = pd.DataFrame(data, columns=columns)
        return refactor_data(df)
    except requests.RequestException:
        return []


def get_current_price_paid(data: List[Dict], property_type: str) -> List[Dict]:
    if not data:
        return []

    current_date = datetime.now()
    dates = [datetime.strptime(entry["date_of_transfer"], "%Y-%m-%d") for entry in data]

    valid_dates = [d for d in dates if d <= current_date]
    if not valid_dates:
        return []

    latest_date = max(valid_dates)
    latest_year_month = (latest_date.year, latest_date.month)

    filtered_data = [
        entry for entry in data
        if datetime.strptime(entry["date_of_transfer"], "%Y-%m-%d").year == latest_year_month[0]
           and datetime.strptime(entry["date_of_transfer"], "%Y-%m-%d").month == latest_year_month[1]
           and entry["property_type"] == property_type
    ]

    if not filtered_data:
        return []

    total_price = sum(entry["price"] for entry in filtered_data)
    total_area = sum(entry["area"] for entry in filtered_data)
    price_per_m2 = round(total_price / total_area) if total_area != 0 else 0

    return [{
        "month_of_transfer": latest_date.strftime("%Y-%m"),
        "price_per_m2": price_per_m2,
        "property_type": property_type
    }]


def remove_matching_entries(array1: List[Dict], array2: List[Dict]) -> List[Dict]:
    if not array1 or not array2:
        return array2

    array1_set = {tuple(sorted(d.items())) for d in array1}
    return [
        item for item in array2
        if tuple(sorted(item.items())) not in array1_set
           and (item.get('area', 0) != 0 or item.get('price_per_m2', 0) != 0)
    ]