from typing import Optional, Literal

from openai import BaseModel


class Response(BaseModel):
    message: Optional[str] = None
    status: Literal["success", "failure"] = "success"
    data: Optional[dict] = None