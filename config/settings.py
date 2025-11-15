from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    POLYGON_KEY: str
    ALPHA_VANTAGE_KEY: str
    NEWS_API_KEY: str
    FRED_API_KEY: str
    
    DB_PATH: str

    class Config:
        env_file = ".env"

settings = Settings()