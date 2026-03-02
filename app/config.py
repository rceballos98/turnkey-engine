from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env")

    database_url: str = "postgresql://postgres:postgres@localhost:5432/turnkey"
    internal_api_key: str = "dev-secret-key"
    base_url: str = "http://localhost:8000"
    environment: str = "development"
    anthropic_api_key: str = ""
    firecrawl_api_key: str = ""


settings = Settings()
