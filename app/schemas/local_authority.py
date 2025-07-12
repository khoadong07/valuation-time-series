from pydantic import BaseModel
from typing import List, Dict, Optional

class LocalAuthority(BaseModel):
    postcode: str
    authority: Optional[str] = None

class LocalAuthorityResponse(BaseModel):
    status: str
    message: str
    data: List[LocalAuthority]