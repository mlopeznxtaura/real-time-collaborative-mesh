"""
Auth.js compatible JWT session management.
Multi-provider authentication: GitHub, Google, email/password.
SDKs: python-jose, passlib, FastAPI
"""
import os
import time
import uuid
import hashlib
import hmac
from typing import Optional, Dict, Any, List
from dataclasses import dataclass, field

from jose import jwt, JWTError
from passlib.context import CryptContext
from fastapi import HTTPException, Depends, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials


SECRET_KEY = os.environ.get("AUTH_SECRET", "dev-secret-change-in-production-" + "x" * 32)
ALGORITHM = "HS256"
ACCESS_TOKEN_TTL = 3600        # 1 hour
REFRESH_TOKEN_TTL = 86400 * 30 # 30 days

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
bearer_scheme = HTTPBearer(auto_error=False)


@dataclass
class User:
    id: str
    email: str
    name: str
    provider: str = "credentials"   # "credentials", "github", "google"
    image: Optional[str] = None
    created_at: float = field(default_factory=time.time)
    roles: List[str] = field(default_factory=lambda: ["user"])


@dataclass
class Session:
    user: User
    access_token: str
    refresh_token: str
    expires_at: float


class AuthManager:
    """
    Auth.js-compatible session management for the collaborative mesh.
    Issues JWT access tokens and refresh tokens.
    """

    def __init__(self):
        # In-memory user store (swap for Supabase/DB in prod)
        self._users: Dict[str, User] = {}
        self._password_hashes: Dict[str, str] = {}
        self._refresh_tokens: Dict[str, str] = {}  # token -> user_id

    # ---- Password auth ----

    def register(self, email: str, password: str, name: str = "") -> User:
        """Register a new user with email/password."""
        if email in {u.email for u in self._users.values()}:
            raise ValueError(f"Email already registered: {email}")
        user_id = str(uuid.uuid4())
        user = User(id=user_id, email=email, name=name or email.split("@")[0])
        self._users[user_id] = user
        self._password_hashes[user_id] = pwd_context.hash(password)
        print(f"[Auth] Registered: {email} ({user_id})")
        return user

    def login(self, email: str, password: str) -> Session:
        """Authenticate with email/password and issue tokens."""
        user = next((u for u in self._users.values() if u.email == email), None)
        if not user:
            raise HTTPException(status_code=401, detail="Invalid credentials")
        if not pwd_context.verify(password, self._password_hashes.get(user.id, "")):
            raise HTTPException(status_code=401, detail="Invalid credentials")
        return self._create_session(user)

    # ---- OAuth providers ----

    def oauth_login(
        self, provider: str, provider_user_id: str,
        email: str, name: str, image: Optional[str] = None
    ) -> Session:
        """Handle OAuth login (GitHub, Google). Create user if first time."""
        composite_id = f"{provider}:{provider_user_id}"
        user = self._users.get(composite_id)
        if not user:
            user = User(
                id=composite_id, email=email, name=name,
                provider=provider, image=image,
            )
            self._users[composite_id] = user
            print(f"[Auth] OAuth signup: {email} via {provider}")
        return self._create_session(user)

    # ---- Token management ----

    def _create_session(self, user: User) -> Session:
        now = time.time()
        access_payload = {
            "sub": user.id,
            "email": user.email,
            "name": user.name,
            "roles": user.roles,
            "iat": int(now),
            "exp": int(now + ACCESS_TOKEN_TTL),
        }
        refresh_payload = {
            "sub": user.id,
            "type": "refresh",
            "iat": int(now),
            "exp": int(now + REFRESH_TOKEN_TTL),
        }
        access_token = jwt.encode(access_payload, SECRET_KEY, algorithm=ALGORITHM)
        refresh_token = jwt.encode(refresh_payload, SECRET_KEY, algorithm=ALGORITHM)
        self._refresh_tokens[refresh_token] = user.id
        return Session(
            user=user,
            access_token=access_token,
            refresh_token=refresh_token,
            expires_at=now + ACCESS_TOKEN_TTL,
        )

    def verify_token(self, token: str) -> Dict[str, Any]:
        """Verify a JWT access token and return the payload."""
        try:
            payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
            return payload
        except JWTError as e:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=f"Invalid token: {e}",
            )

    def refresh(self, refresh_token: str) -> Session:
        """Issue new access token from valid refresh token."""
        try:
            payload = jwt.decode(refresh_token, SECRET_KEY, algorithms=[ALGORITHM])
        except JWTError:
            raise HTTPException(status_code=401, detail="Invalid refresh token")
        user_id = payload.get("sub")
        user = self._users.get(user_id)
        if not user:
            raise HTTPException(status_code=401, detail="User not found")
        return self._create_session(user)

    def get_current_user(
        self,
        credentials: Optional[HTTPAuthorizationCredentials] = Depends(bearer_scheme),
    ) -> User:
        """FastAPI dependency: extract and verify user from Bearer token."""
        if not credentials:
            raise HTTPException(status_code=401, detail="Not authenticated")
        payload = self.verify_token(credentials.credentials)
        user_id = payload.get("sub")
        user = self._users.get(user_id)
        if not user:
            raise HTTPException(status_code=401, detail="User not found")
        return user
