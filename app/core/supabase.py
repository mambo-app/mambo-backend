from supabase import create_client, Client
from app.core.config import settings

# Anon client — same as Flutter uses
supabase: Client = create_client(
    settings.supabase_url,
    settings.supabase_anon_key,
)

# Admin client — backend only, never send to Flutter
supabase_admin: Client = create_client(
    settings.supabase_url,
    settings.supabase_service_role_key,
)

def get_supabase_client() -> Client:
    """Create a fresh non-persistent client instance to avoid session clobbering."""
    return create_client(
        settings.supabase_url,
        settings.supabase_anon_key,
        # We don't need a persistent storage in the backend
    )