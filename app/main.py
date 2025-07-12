from fastapi import FastAPI
from app.api.v1.endpoints import pricing, local_authority, training

app = FastAPI(title="My FastAPI Project")

app.include_router(router=pricing.router, tags=["pricing"])
app.include_router(router=local_authority.router)
app.include_router(router=training.router)
@app.get("/")
async def root():
    return {"message": "Welcome to FastAPI!"}