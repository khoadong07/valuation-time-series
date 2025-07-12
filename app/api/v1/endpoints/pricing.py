from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel

from app.services.predict import get_hpi_by_month, get_hpi_forecast, valuation
from app.services.training import insert_forecast_to_mongodb
from app.services.utils import search_local_authority_by_postcode, scrape, get_current_price_paid, remove_matching_entries

router = APIRouter(prefix="/pricing", tags=["pricing"])

class PricingRequest(BaseModel):
    full_address: str
    street: str
    postcode: str
    property_type: str
    area: float

class ForecastRecord(BaseModel):
    local_authority: str
    property_type: str
    mae: float
    mape: float
    forecast: float
    forecast_date: str
    best_model: str
    best_time_window: int
    model_file: str

class ForecastResponse(BaseModel):
    last_sales_price: Optional[dict]
    recent_property_sale_history: Optional[dict]
    local_authority: Optional[str]
    max_valuation: Optional[float]
    min_valuation: Optional[float]

@router.post("/insert-forecast", status_code=status.HTTP_201_CREATED)
async def insert_forecast(records: List[ForecastRecord]):
    if not records:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No forecast records provided."
        )
    try:
        data = [record.dict() for record in records]
        inserted_count = insert_forecast_to_mongodb(data)
        return {
            "message": f"Successfully inserted {inserted_count} records.",
            "inserted_count": inserted_count
        }
    except RuntimeError:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to insert forecast records into the database."
        )

@router.post("/valuation", response_model=ForecastResponse)
async def get_pricing_data(request: PricingRequest):
    local_authority_data = await search_local_authority_by_postcode(request.postcode)

    if not local_authority_data:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Local authority not found for the provided postcode."
        )
    local_authority = local_authority_data[0].get('local_authority_label')
    if local_authority.lower() == 'westminster' or local_authority.lower() == 'city of london':
        local_authority = 'Camden'

    if not local_authority:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Local authority label is missing."
        )

    last_sales_price = scrape(request.full_address) or scrape(request.street)
    if not last_sales_price:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No sales price data found for the provided address or street."
        )

    history_data = scrape(request.postcode)
    if not history_data:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No historical sales data found for the provided postcode."
        )
    history_data = remove_matching_entries(last_sales_price, history_data)

    last_sales_price = get_current_price_paid(last_sales_price, request.property_type)
    if not last_sales_price:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No valid sales price data found for the property type."
        )

    recent_sale_history = get_current_price_paid(history_data, request.property_type)
    if not recent_sale_history:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No recent sales history found for the property type."
        )

    hpi_last_sales = get_hpi_by_month(
        month_of_transfer=last_sales_price[0].get('month_of_transfer'),
        property_type=request.property_type,
        local_authority=local_authority
    )
    if not hpi_last_sales:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="HPI data not found for last sales price."
        )

    hpi_recent_sale = get_hpi_by_month(
        month_of_transfer=recent_sale_history[0].get('month_of_transfer'),
        property_type=request.property_type,
        local_authority=local_authority
    )
    if not hpi_recent_sale:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="HPI data not found for recent sales history."
        )

    hpi_forecast = get_hpi_forecast(
        property_type=request.property_type,
        local_authority=local_authority
    )
    if not hpi_forecast:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="HPI forecast data not found."
        )

    price_valuation_1 = request.area * valuation(
        latest_hpi=hpi_forecast,
        last_hpi=hpi_last_sales,
        history_price=last_sales_price[0].get('price_per_m2')
    )
    price_valuation_2 = request.area * valuation(
        latest_hpi=hpi_forecast,
        last_hpi=hpi_recent_sale,
        history_price=recent_sale_history[0].get('price_per_m2')
    )

    return ForecastResponse(
        local_authority=local_authority,
        last_sales_price=last_sales_price[0],
        recent_property_sale_history=recent_sale_history[0],
        max_valuation=max(price_valuation_1, price_valuation_2),
        min_valuation=min(price_valuation_1, price_valuation_2)
    )