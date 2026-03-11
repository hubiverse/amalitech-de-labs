from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    tmdb_api_key: str
    tmdb_api_access_token: str
    tmdb_api_base_url: str = "https://api.themoviedb.org/3"

    model_config = SettingsConfigDict(
        env_file=Path(__file__).parent / ".env",
        env_prefix="",
        case_sensitive=False
    )

def get_settings() -> Settings:
    return Settings()

__all__ = ("get_settings", "Settings")