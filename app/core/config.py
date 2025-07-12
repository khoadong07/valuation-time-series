from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    APP_NAME: str = "My FastAPI Project"
    DEBUG: bool = True
    SECRET_KEY: str
    DATABASE_URL: str
    OPEN_ROUTER_KEY:  str
    OPEN_ROUTER_URL: str

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

settings = Settings()