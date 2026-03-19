import logging
import jwt as py_jwt
from jwt import PyJWKClient
from fastapi import HTTPException, status
from app.core.config import settings

logger = logging.getLogger('mambo.security')

# Supabase now issues ES256 (asymmetric ECDSA) tokens.
# We use PyJWKClient to fetch and cache the public key from the JWKS endpoint.
_JWKS_URL = f"{settings.supabase_url}/auth/v1/.well-known/jwks.json"
_jwks_client = PyJWKClient(_JWKS_URL, cache_keys=True)


def verify_supabase_jwt(token: str) -> dict:
    try:
        # 1. Inspect header to determine the signing algorithm
        header = py_jwt.get_unverified_header(token)
        alg = header.get('alg', 'HS256')
        logger.debug(f'JWT Header: {header}')

        if alg in ('ES256', 'RS256'):
            # Asymmetric path: verify using Supabase JWKS public key
            signing_key = _jwks_client.get_signing_key_from_jwt(token)
            payload = py_jwt.decode(
                token,
                signing_key.key,
                algorithms=[alg],
                options={'verify_aud': False},
            )
        else:
            # Symmetric path: verify using the shared JWT secret (HS256)
            payload = py_jwt.decode(
                token,
                settings.supabase_jwt_secret,
                algorithms=['HS256'],
                options={'verify_aud': False},
            )

        return payload

    except Exception as e:
        import traceback
        logger.error(f'JWT verification failed ({type(e).__name__}): {e}')
        logger.debug(traceback.format_exc())
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail='Invalid or expired token',
            headers={'WWW-Authenticate': 'Bearer'},
        )


def extract_user_id(payload: dict) -> str:
    user_id = payload.get('sub')
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail='Token missing subject claim',
        )
    return user_id