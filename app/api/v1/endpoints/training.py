from fastapi import APIRouter, Depends
from pydantic import BaseModel

from app.core.training import train_and_forecast_by_authority
from app.services.utils import search_external_data_by_local_authority

router = APIRouter(prefix="/training", tags=["training"])

class Training(BaseModel):
    local_authority: str

@router.post("/create_training_job")
async def create_training_job(training: Training):
    local_authority = training.local_authority
    get_extra_data = await search_external_data_by_local_authority(local_authority)

    return await train_and_forecast_by_authority(get_extra_data)