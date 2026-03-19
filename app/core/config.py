from pydantic_settings import BaseSettings
from functools import lru_cache

class Settings(BaseSettings):
    database_pool_url: str
    database_direct_url: str
    supabase_url: str
    supabase_anon_key: str
    supabase_service_role_key: str
    supabase_jwt_secret: str
    upstash_redis_rest_url: str
    upstash_redis_rest_token: str
    redis_url: str = ''
    firebase_credentials_path: str = './firebase-service-account.json'
    sentry_dsn: str = ''
    tmdb_api_key: str = ''
    mal_client_id: str = ''
    news_api: str = ''
    app_env: str = 'development'
    invite_key: str = 'B3G1N'
    admin_secret: str = 'CHANGE_ME_IN_PRODUCTION'
    allowed_origins: str = 'http://localhost:3000'

    @property
    def origins_list(self) -> list[str]:
        return [o.strip() for o in self.allowed_origins.split(',')]

    @property
    def is_production(self) -> bool:
        return self.app_env == 'production'

    model_config = {
        "env_file": ".env"
    }

@lru_cache()
def get_settings() -> Settings:
    return Settings()

settings = get_settings()