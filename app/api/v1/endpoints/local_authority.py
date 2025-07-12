from fastapi import APIRouter, Query, HTTPException, status
from app.schemas.local_authority import LocalAuthorityResponse, LocalAuthority
from app.schemas.utils import Response
from app.services.llm import get_bordering_local_authorities
from app.services.utils import search_local_authority_by_postcode, search_external_data_by_local_authority

router = APIRouter(prefix="/local_authority", tags=["local_authority"])

@router.get("/get_by_postcode", response_model=LocalAuthorityResponse)
async def get_local_authority_by_postcode(
    postcode: str = Query(..., description="Postcode to fetch local authority", min_length=5, max_length=8)
):
    try:
        result = await search_local_authority_by_postcode(postcode.upper())
        if not result:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No local authority found for the given postcode"
            )

        authorities = [
            LocalAuthority(
                authority=item.get("local_authority_label", ""),
                postcode=item.get("postcode", "")
            ) for item in result
        ]

        return LocalAuthorityResponse(
            status="success",
            message="Local authority data retrieved successfully",
            data=authorities
        )

    except ValueError as ve:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid postcode format: {str(ve)}")
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error")

@router.get("/get_external_data", response_model=Response)
async def get_local_authority_data(local_authority: str = Query(...)):
    try:
        result = await search_external_data_by_local_authority(local_authority)
        if not result:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No data found for the given local authority"
            )
        return Response(
            status="success",
            message="Local authority data retrieved successfully",
            data=result
        )
    except ValueError as ve:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid local authority format: {str(ve)}")
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error")

@router.get("/get_nearest_local_authority", response_model=Response)
async def get_nearest_local_authority(local_authority: str = Query(...)):
    try:
        result = await get_bordering_local_authorities(local_authority)
        if not result:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No bordering local authorities found"
            )
        return Response(
            status="success",
            message="Bordering local authority data retrieved successfully",
            data=result
        )
    except ValueError as ve:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid local authority format: {str(ve)}")
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error")