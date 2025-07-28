# """
# Comprehensive authentication and authorization system for the analytics API.
# Implements JWT-based authentication with refresh tokens, role-based access control,
# API key authentication, OAuth2 integration, and comprehensive security features.
# """

# import asyncio
# import hashlib
# import secrets
# import time
# from datetime import datetime, timedelta
# from typing import Optional, Dict, List, Any, Union
# from functools import wraps
# from enum import Enum
# import re
# import json
# import uuid

# import jwt
# from passlib.context import CryptContext
# from fastapi import HTTPException, Security, Depends, Request, status
# from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials, APIKeyHeader
# from fastapi.middleware.cors import CORSMiddleware
# from fastapi.responses import JSONResponse
# from pydantic import BaseModel, Field, validator
# import redis
# from sqlalchemy import create_engine, Column, String, DateTime, Boolean, JSON, Integer, Text
# from sqlalchemy.ext.declarative import declarative_base
# from sqlalchemy.orm import sessionmaker, Session
# import aioredis
# # from authlib.integrations.fastapi_oauth2 import AuthorizationServer
# # from authlib.integrations.starlette_oauth2 import AuthorizationServer
# from authlib.jose import jwt
# from authlib.oauth2.rfc6749 import grants
# # from authlib.integrations.starlette_oauth2 import AuthorizationServer
# from authlib.oauth2.rfc6749 import grants
# import logging
# import os

# def verify_token(token: str, key: str, alg: str = "HS256"):
#     try:
#         claims = jwt.decode(token, key)
#         claims.validate()  # optional expiration and audience checks
#         return claims
#     except Exception as e:
#         raise HTTPException(status_code=401, detail="Invalid token")

# # Configuration
# JWT_SECRET_KEY = os.getenv("JWT_SECRET")  # Should be from environment
# JWT_ALGORITHM = "HS256"
# JWT_EXPIRATION_DELTA = timedelta(hours=1)
# JWT_REFRESH_EXPIRATION_DELTA = timedelta(days=30)
# API_KEY_PREFIX = "ak_"
# REDIS_URL = "redis://localhost:6379"
# DATABASE_URL = "sqlite:///./auth.db"

# # Security
# pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
# security = HTTPBearer()
# api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

# # Database setup
# Base = declarative_base()
# engine = create_engine(DATABASE_URL)
# SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# # Logger
# logger = logging.getLogger(__name__)

# # Enums
# class UserRole(str, Enum):
#     ADMIN = "admin"
#     OPERATOR = "operator"
#     VIEWER = "viewer"

# class Permission(str, Enum):
#     READ_ALERTS = "read_alerts"
#     WRITE_ALERTS = "write_alerts"
#     READ_ANALYTICS = "read_analytics"
#     WRITE_ANALYTICS = "write_analytics"
#     READ_CONFIG = "read_config"
#     WRITE_CONFIG = "write_config"
#     MANAGE_USERS = "manage_users"
#     MANAGE_API_KEYS = "manage_api_keys"

# # Role permissions mapping
# ROLE_PERMISSIONS = {
#     UserRole.ADMIN: [
#         Permission.READ_ALERTS, Permission.WRITE_ALERTS,
#         Permission.READ_ANALYTICS, Permission.WRITE_ANALYTICS,
#         Permission.READ_CONFIG, Permission.WRITE_CONFIG,
#         Permission.MANAGE_USERS, Permission.MANAGE_API_KEYS
#     ],
#     UserRole.OPERATOR: [
#         Permission.READ_ALERTS, Permission.WRITE_ALERTS,
#         Permission.READ_ANALYTICS, Permission.WRITE_ANALYTICS,
#         Permission.READ_CONFIG
#     ],
#     UserRole.VIEWER: [
#         Permission.READ_ALERTS, Permission.READ_ANALYTICS, Permission.READ_CONFIG
#     ]
# }

# # Database Models
# class User(Base):
#     __tablename__ = "users"
    
#     id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
#     username = Column(String, unique=True, index=True)
#     email = Column(String, unique=True, index=True)
#     hashed_password = Column(String)
#     role = Column(String, default=UserRole.VIEWER)
#     is_active = Column(Boolean, default=True)
#     tenant_id = Column(String, index=True)
#     created_at = Column(DateTime, default=datetime.utcnow)
#     last_login = Column(DateTime)
#     failed_login_attempts = Column(Integer, default=0)
#     locked_until = Column(DateTime)
#     password_changed_at = Column(DateTime, default=datetime.utcnow)
#     oauth_provider = Column(String)
#     oauth_id = Column(String)
#     metadata = Column(JSON)

# class APIKey(Base):
#     __tablename__ = "api_keys"
    
#     id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
#     key_hash = Column(String, unique=True, index=True)
#     name = Column(String)
#     user_id = Column(String, index=True)
#     tenant_id = Column(String, index=True)
#     permissions = Column(JSON)
#     is_active = Column(Boolean, default=True)
#     created_at = Column(DateTime, default=datetime.utcnow)
#     last_used = Column(DateTime)
#     expires_at = Column(DateTime)
#     rate_limit = Column(Integer, default=1000)  # requests per hour
#     metadata = Column(JSON)

# class RefreshToken(Base):
#     __tablename__ = "refresh_tokens"
    
#     id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
#     token_hash = Column(String, unique=True, index=True)
#     user_id = Column(String, index=True)
#     created_at = Column(DateTime, default=datetime.utcnow)
#     expires_at = Column(DateTime)
#     is_revoked = Column(Boolean, default=False)
#     tenant_id = Column(String, index=True)

# class AuditLog(Base):
#     __tablename__ = "audit_logs"
    
#     id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
#     user_id = Column(String, index=True)
#     action = Column(String)
#     resource = Column(String)
#     ip_address = Column(String)
#     user_agent = Column(String)
#     timestamp = Column(DateTime, default=datetime.utcnow)
#     tenant_id = Column(String, index=True)
#     metadata = Column(JSON)
#     success = Column(Boolean)

# # Pydantic Models
# class UserCreate(BaseModel):
#     username: str = Field(..., min_length=3, max_length=50)
#     email: str = Field(..., regex=r'^[^@]+@[^@]+\.[^@]+$')
#     password: str = Field(..., min_length=8)
#     role: UserRole = UserRole.VIEWER
#     tenant_id: str
    
#     @validator('password')
#     def validate_password(cls, v):
#         if not re.search(r'[A-Z]', v):
#             raise ValueError('Password must contain at least one uppercase letter')
#         if not re.search(r'[a-z]', v):
#             raise ValueError('Password must contain at least one lowercase letter')
#         if not re.search(r'\d', v):
#             raise ValueError('Password must contain at least one digit')
#         if not re.search(r'[!@#$%^&*(),.?":{}|<>]', v):
#             raise ValueError('Password must contain at least one special character')
#         return v

# class UserResponse(BaseModel):
#     id: str
#     username: str
#     email: str
#     role: UserRole
#     is_active: bool
#     tenant_id: str
#     created_at: datetime
#     last_login: Optional[datetime]

# class Token(BaseModel):
#     access_token: str
#     token_type: str = "bearer"
#     expires_in: int
#     refresh_token: str

# class TokenData(BaseModel):
#     user_id: str
#     username: str
#     role: UserRole
#     tenant_id: str
#     permissions: List[Permission]
#     exp: int

# class APIKeyCreate(BaseModel):
#     name: str
#     permissions: List[Permission]
#     expires_at: Optional[datetime] = None
#     rate_limit: int = 1000

# class APIKeyResponse(BaseModel):
#     id: str
#     name: str
#     key: str  # Only returned on creation
#     permissions: List[Permission]
#     created_at: datetime
#     expires_at: Optional[datetime]
#     rate_limit: int

# # Redis client for rate limiting and token blacklisting
# redis_client = None

# async def get_redis():
#     global redis_client
#     if redis_client is None:
#         redis_client = await aioredis.from_url(REDIS_URL)
#     return redis_client

# # Database dependency
# def get_db():
#     db = SessionLocal()
#     try:
#         yield db
#     finally:
#         db.close()

# # Authentication utilities
# class AuthManager:
#     def __init__(self):
#         self.pwd_context = pwd_context
    
#     def verify_password(self, plain_password: str, hashed_password: str) -> bool:
#         return self.pwd_context.verify(plain_password, hashed_password)
    
#     def get_password_hash(self, password: str) -> str:
#         return self.pwd_context.hash(password)
    
#     def create_access_token(self, data: dict, expires_delta: Optional[timedelta] = None) -> str:
#         to_encode = data.copy()
#         if expires_delta:
#             expire = datetime.utcnow() + expires_delta
#         else:
#             expire = datetime.utcnow() + JWT_EXPIRATION_DELTA
        
#         to_encode.update({"exp": expire, "type": "access"})
#         encoded_jwt = jwt.encode(to_encode, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)
#         return encoded_jwt
    
#     def create_refresh_token(self, user_id: str, tenant_id: str) -> str:
#         data = {
#             "user_id": user_id,
#             "tenant_id": tenant_id,
#             "type": "refresh",
#             "exp": datetime.utcnow() + JWT_REFRESH_EXPIRATION_DELTA
#         }
#         return jwt.encode(data, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)
    
#     def verify_token(self, token: str) -> Optional[TokenData]:
#         try:
#             payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
#             if payload.get("type") != "access":
#                 return None
            
#             user_id = payload.get("user_id")
#             username = payload.get("username")
#             role = payload.get("role")
#             tenant_id = payload.get("tenant_id")
#             permissions = payload.get("permissions", [])
#             exp = payload.get("exp")
            
#             if not all([user_id, username, role, tenant_id]):
#                 return None
            
#             return TokenData(
#                 user_id=user_id,
#                 username=username,
#                 role=UserRole(role),
#                 tenant_id=tenant_id,
#                 permissions=[Permission(p) for p in permissions],
#                 exp=exp
#             )
#         except jwt.ExpiredSignatureError:
#             return None
#         except jwt.JWTError:
#             return None
    
#     def generate_api_key(self) -> str:
#         return f"{API_KEY_PREFIX}{secrets.token_urlsafe(32)}"
    
#     def hash_api_key(self, api_key: str) -> str:
#         return hashlib.sha256(api_key.encode()).hexdigest()

# auth_manager = AuthManager()

# # Rate limiting
# class RateLimiter:
#     def __init__(self, redis_client):
#         self.redis = redis_client
    
#     async def is_allowed(self, key: str, limit: int, window: int = 3600) -> bool:
#         """Check if request is allowed based on rate limit"""
#         current_time = int(time.time())
#         window_start = current_time - window
        
#         # Clean old entries
#         await self.redis.zremrangebyscore(key, 0, window_start)
        
#         # Count current requests
#         current_requests = await self.redis.zcard(key)
        
#         if current_requests >= limit:
#             return False
        
#         # Add current request
#         await self.redis.zadd(key, {str(current_time): current_time})
#         await self.redis.expire(key, window)
        
#         return True

# # Token blacklisting
# class TokenBlacklist:
#     def __init__(self, redis_client):
#         self.redis = redis_client
    
#     async def add_token(self, token: str, exp: int):
#         """Add token to blacklist"""
#         ttl = exp - int(time.time())
#         if ttl > 0:
#             await self.redis.setex(f"blacklist:{token}", ttl, "1")
    
#     async def is_blacklisted(self, token: str) -> bool:
#         """Check if token is blacklisted"""
#         result = await self.redis.get(f"blacklist:{token}")
#         return result is not None

# # User management
# class UserManager:
#     def __init__(self, db: Session):
#         self.db = db
    
#     def create_user(self, user_data: UserCreate) -> User:
#         hashed_password = auth_manager.get_password_hash(user_data.password)
#         user = User(
#             username=user_data.username,
#             email=user_data.email,
#             hashed_password=hashed_password,
#             role=user_data.role,
#             tenant_id=user_data.tenant_id
#         )
#         self.db.add(user)
#         self.db.commit()
#         self.db.refresh(user)
#         return user
    
#     def get_user_by_username(self, username: str) -> Optional[User]:
#         return self.db.query(User).filter(User.username == username).first()
    
#     def get_user_by_email(self, email: str) -> Optional[User]:
#         return self.db.query(User).filter(User.email == email).first()
    
#     def get_user_by_id(self, user_id: str) -> Optional[User]:
#         return self.db.query(User).filter(User.id == user_id).first()
    
#     def authenticate_user(self, username: str, password: str) -> Optional[User]:
#         user = self.get_user_by_username(username)
#         if not user or not user.is_active:
#             return None
        
#         # Check if user is locked
#         if user.locked_until and user.locked_until > datetime.utcnow():
#             return None
        
#         if not auth_manager.verify_password(password, user.hashed_password):
#             # Increment failed attempts
#             user.failed_login_attempts += 1
#             if user.failed_login_attempts >= 5:
#                 user.locked_until = datetime.utcnow() + timedelta(minutes=30)
#             self.db.commit()
#             return None
        
#         # Reset failed attempts on successful login
#         user.failed_login_attempts = 0
#         user.locked_until = None
#         user.last_login = datetime.utcnow()
#         self.db.commit()
        
#         return user
    
#     def update_user_role(self, user_id: str, new_role: UserRole):
#         user = self.get_user_by_id(user_id)
#         if user:
#             user.role = new_role
#             self.db.commit()
    
#     def deactivate_user(self, user_id: str):
#         user = self.get_user_by_id(user_id)
#         if user:
#             user.is_active = False
#             self.db.commit()

# # API Key management
# class APIKeyManager:
#     def __init__(self, db: Session):
#         self.db = db
    
#     def create_api_key(self, user_id: str, tenant_id: str, key_data: APIKeyCreate) -> tuple[APIKey, str]:
#         api_key = auth_manager.generate_api_key()
#         key_hash = auth_manager.hash_api_key(api_key)
        
#         db_key = APIKey(
#             key_hash=key_hash,
#             name=key_data.name,
#             user_id=user_id,
#             tenant_id=tenant_id,
#             permissions=[p.value for p in key_data.permissions],
#             expires_at=key_data.expires_at,
#             rate_limit=key_data.rate_limit
#         )
#         self.db.add(db_key)
#         self.db.commit()
#         self.db.refresh(db_key)
        
#         return db_key, api_key
    
#     def get_api_key_by_hash(self, key_hash: str) -> Optional[APIKey]:
#         return self.db.query(APIKey).filter(APIKey.key_hash == key_hash).first()
    
#     def authenticate_api_key(self, api_key: str) -> Optional[APIKey]:
#         key_hash = auth_manager.hash_api_key(api_key)
#         db_key = self.get_api_key_by_hash(key_hash)
        
#         if not db_key or not db_key.is_active:
#             return None
        
#         if db_key.expires_at and db_key.expires_at < datetime.utcnow():
#             return None
        
#         # Update last used
#         db_key.last_used = datetime.utcnow()
#         self.db.commit()
        
#         return db_key
    
#     def revoke_api_key(self, key_id: str):
#         key = self.db.query(APIKey).filter(APIKey.id == key_id).first()
#         if key:
#             key.is_active = False
#             self.db.commit()

# # Audit logging
# class AuditLogger:
#     def __init__(self, db: Session):
#         self.db = db
    
#     def log_event(self, user_id: str, action: str, resource: str, 
#                   ip_address: str, user_agent: str, tenant_id: str, 
#                   success: bool = True, metadata: dict = None):
#         log_entry = AuditLog(
#             user_id=user_id,
#             action=action,
#             resource=resource,
#             ip_address=ip_address,
#             user_agent=user_agent,
#             tenant_id=tenant_id,
#             success=success,
#             metadata=metadata or {}
#         )
#         self.db.add(log_entry)
#         self.db.commit()

# # Authentication dependencies
# async def get_current_user(
#     credentials: HTTPAuthorizationCredentials = Security(security),
#     db: Session = Depends(get_db)
# ) -> User:
#     """Get current user from JWT token"""
#     redis_client = await get_redis()
#     blacklist = TokenBlacklist(redis_client)
    
#     # Check if token is blacklisted
#     if await blacklist.is_blacklisted(credentials.credentials):
#         raise HTTPException(
#             status_code=status.HTTP_401_UNAUTHORIZED,
#             detail="Token has been revoked"
#         )
    
#     token_data = auth_manager.verify_token(credentials.credentials)
#     if not token_data:
#         raise HTTPException(
#             status_code=status.HTTP_401_UNAUTHORIZED,
#             detail="Invalid token"
#         )
    
#     user_manager = UserManager(db)
#     user = user_manager.get_user_by_id(token_data.user_id)
#     if not user or not user.is_active:
#         raise HTTPException(
#             status_code=status.HTTP_401_UNAUTHORIZED,
#             detail="User not found or inactive"
#         )
    
#     return user

# async def get_current_user_from_api_key(
#     api_key: str = Security(api_key_header),
#     db: Session = Depends(get_db)
# ) -> Optional[User]:
#     """Get current user from API key"""
#     if not api_key:
#         return None
    
#     api_key_manager = APIKeyManager(db)
#     db_key = api_key_manager.authenticate_api_key(api_key)
    
#     if not db_key:
#         return None
    
#     # Check rate limit
#     redis_client = await get_redis()
#     rate_limiter = RateLimiter(redis_client)
    
#     if not await rate_limiter.is_allowed(f"api_key:{db_key.id}", db_key.rate_limit):
#         raise HTTPException(
#             status_code=status.HTTP_429_TOO_MANY_REQUESTS,
#             detail="Rate limit exceeded"
#         )
    
#     user_manager = UserManager(db)
#     user = user_manager.get_user_by_id(db_key.user_id)
    
#     if user:
#         # Add API key permissions to user context
#         user.api_key_permissions = [Permission(p) for p in db_key.permissions]
    
#     return user

# async def get_current_user_flexible(
#     credentials: HTTPAuthorizationCredentials = Security(security),
#     api_key: str = Security(api_key_header),
#     db: Session = Depends(get_db)
# ) -> User:
#     """Get current user from either JWT token or API key"""
#     user = None
    
#     # Try API key first
#     if api_key:
#         user = await get_current_user_from_api_key(api_key, db)
    
#     # If no API key or API key failed, try JWT
#     if not user and credentials:
#         user = await get_current_user(credentials, db)
    
#     if not user:
#         raise HTTPException(
#             status_code=status.HTTP_401_UNAUTHORIZED,
#             detail="Authentication required"
#         )
    
#     return user

# # Permission checking
# def require_permission(permission: Permission):
#     """Decorator to require specific permission"""
#     def decorator(func):
#         @wraps(func)
#         async def wrapper(*args, **kwargs):
#             # Get current user from function arguments
#             current_user = None
#             for arg in args:
#                 if isinstance(arg, User):
#                     current_user = arg
#                     break
            
#             if not current_user:
#                 raise HTTPException(
#                     status_code=status.HTTP_403_FORBIDDEN,
#                     detail="Permission denied"
#                 )
            
#             # Check user role permissions
#             user_permissions = ROLE_PERMISSIONS.get(UserRole(current_user.role), [])
            
#             # Check API key permissions if available
#             if hasattr(current_user, 'api_key_permissions'):
#                 user_permissions = current_user.api_key_permissions
            
#             if permission not in user_permissions:
#                 raise HTTPException(
#                     status_code=status.HTTP_403_FORBIDDEN,
#                     detail=f"Permission {permission.value} required"
#                 )
            
#             return await func(*args, **kwargs)
#         return wrapper
#     return decorator

# # Multi-tenant support
# def require_tenant_access(tenant_id: str, user: User):
#     """Check if user has access to specific tenant"""
#     if user.role == UserRole.ADMIN:
#         return True  # Admin can access all tenants
    
#     if user.tenant_id != tenant_id:
#         raise HTTPException(
#             status_code=status.HTTP_403_FORBIDDEN,
#             detail="Access denied to this tenant"
#         )
    
#     return True

# # Security headers middleware
# async def add_security_headers(request: Request, call_next):
#     response = await call_next(request)
    
#     response.headers["X-Content-Type-Options"] = "nosniff"
#     response.headers["X-Frame-Options"] = "DENY"
#     response.headers["X-XSS-Protection"] = "1; mode=block"
#     response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
#     response.headers["Content-Security-Policy"] = "default-src 'self'"
    
#     return response

# # Initialize database
# def init_db():
#     Base.metadata.create_all(bind=engine)

# # Testing utilities
# class AuthTestUtils:
#     @staticmethod
#     def create_test_user(db: Session, username: str = "testuser", 
#                         role: UserRole = UserRole.VIEWER, 
#                         tenant_id: str = "test_tenant") -> User:
#         """Create a test user for testing purposes"""
#         user_data = UserCreate(
#             username=username,
#             email=f"{username}@example.com",
#             password="TestPass123!",
#             role=role,
#             tenant_id=tenant_id
#         )
#         user_manager = UserManager(db)
#         return user_manager.create_user(user_data)
    
#     @staticmethod
#     def create_test_token(user: User) -> str:
#         """Create a test JWT token"""
#         data = {
#             "user_id": user.id,
#             "username": user.username,
#             "role": user.role,
#             "tenant_id": user.tenant_id,
#             "permissions": [p.value for p in ROLE_PERMISSIONS.get(UserRole(user.role), [])]
#         }
#         return auth_manager.create_access_token(data)
    
#     @staticmethod
#     def create_test_api_key(db: Session, user_id: str, 
#                            permissions: List[Permission] = None) -> tuple[APIKey, str]:
#         """Create a test API key"""
#         if permissions is None:
#             permissions = [Permission.READ_ALERTS]
        
#         key_data = APIKeyCreate(
#             name="Test Key",
#             permissions=permissions
#         )
#         api_key_manager = APIKeyManager(db)
#         return api_key_manager.create_api_key(user_id, "test_tenant", key_data)

# # Export main components
# __all__ = [
#     'AuthManager', 'UserManager', 'APIKeyManager', 'AuditLogger',
#     'get_current_user', 'get_current_user_from_api_key', 'get_current_user_flexible',
#     'require_permission', 'require_tenant_access', 'add_security_headers',
#     'UserRole', 'Permission', 'UserCreate', 'UserResponse', 'Token', 'APIKeyCreate',
#     'APIKeyResponse', 'init_db', 'AuthTestUtils'
# ]
# """
# Comprehensive authentication and authorization system for the analytics API.
# Implements JWT-based authentication with refresh tokens, role-based access control,
# API key authentication, OAuth2 integration, and comprehensive security features.
# """

# import asyncio
# import hashlib
# import secrets
# import time
# from datetime import datetime, timedelta
# from typing import Optional, Dict, List, Any, Union
# from functools import wraps
# from enum import Enum
# import re
# import json
# import uuid

# import jwt # This is the PyJWT library
# from passlib.context import CryptContext
# from fastapi import HTTPException, Security, Depends, Request, status
# from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials, APIKeyHeader
# from fastapi.middleware.cors import CORSMiddleware
# from fastapi.responses import JSONResponse
# from pydantic import BaseModel, Field, validator, ValidationError # Added ValidationError
# import redis.asyncio as aioredis # Use async Redis client consistently
# from sqlalchemy import create_engine, Column, String, DateTime, Boolean, JSON, Integer, Text
# from sqlalchemy.ext.declarative import declarative_base
# from sqlalchemy.orm import sessionmaker, Session
# # from authlib.integrations.starlette_oauth2 import AuthorizationServer # Keep commented as per original
# # from authlib.oauth2.rfc6749 import grants # Keep commented as per original
# import logging
# import os

# # Import your global settings
# from config.settings import Settings # Assuming your main settings class is named Settings

# # Logger
# logger = logging.getLogger(__name__)

# # Security
# pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
# security = HTTPBearer()
# api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

# # Database setup (engine and SessionLocal will be initialized by AuthManager for dependency injection)
# Base = declarative_base()
# # engine and SessionLocal are no longer global, they will be managed by AuthManager


# # Enums
# class UserRole(str, Enum):
#     ADMIN = "admin"
#     OPERATOR = "operator"
#     VIEWER = "viewer"

# class Permission(str, Enum):
#     READ_ALERTS = "read_alerts"
#     WRITE_ALERTS = "write_alerts"
#     READ_ANALYTICS = "read_analytics"
#     WRITE_ANALYTICS = "write_analytics"
#     READ_CONFIG = "read_config"
#     WRITE_CONFIG = "write_config"
#     MANAGE_USERS = "manage_users"
#     MANAGE_API_KEYS = "manage_api_keys"

# # Role permissions mapping
# ROLE_PERMISSIONS = {
#     UserRole.ADMIN: [
#         Permission.READ_ALERTS, Permission.WRITE_ALERTS,
#         Permission.READ_ANALYTICS, Permission.WRITE_ANALYTICS,
#         Permission.READ_CONFIG, Permission.WRITE_CONFIG,
#         Permission.MANAGE_USERS, Permission.MANAGE_API_KEYS
#     ],
#     UserRole.OPERATOR: [
#         Permission.READ_ALERTS, Permission.WRITE_ALERTS,
#         Permission.READ_ANALYTICS, Permission.WRITE_ANALYTICS,
#         Permission.READ_CONFIG
#     ],
#     UserRole.VIEWER: [
#         Permission.READ_ALERTS, Permission.READ_ANALYTICS, Permission.READ_CONFIG
#     ]
# }

# # Database Models (using the corrected 'metadata' column names)
# class User(Base):
#     __tablename__ = "users"
    
#     id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
#     username = Column(String, unique=True, index=True)
#     email = Column(String, unique=True, index=True)
#     hashed_password = Column(String)
#     role = Column(String, default=UserRole.VIEWER)
#     is_active = Column(Boolean, default=True)
#     tenant_id = Column(String, index=True)
#     created_at = Column(DateTime, default=datetime.utcnow)
#     last_login = Column(DateTime)
#     failed_login_attempts = Column(Integer, default=0)
#     locked_until = Column(DateTime)
#     password_changed_at = Column(DateTime, default=datetime.utcnow)
#     oauth_provider = Column(String)
#     oauth_id = Column(String)
#     user_metadata = Column(JSON) # Corrected name

# class APIKey(Base):
#     __tablename__ = "api_keys"
    
#     id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
#     key_hash = Column(String, unique=True, index=True)
#     name = Column(String)
#     user_id = Column(String, index=True)
#     tenant_id = Column(String, index=True)
#     permissions = Column(JSON)
#     is_active = Column(Boolean, default=True)
#     created_at = Column(DateTime, default=datetime.utcnow)
#     last_used = Column(DateTime)
#     expires_at = Column(DateTime)
#     rate_limit = Column(Integer, default=1000)  # requests per hour
#     key_metadata = Column(JSON) # Corrected name

# class RefreshToken(Base):
#     __tablename__ = "refresh_tokens"
    
#     id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
#     token_hash = Column(String, unique=True, index=True)
#     user_id = Column(String, index=True)
#     created_at = Column(DateTime, default=datetime.utcnow)
#     expires_at = Column(DateTime)
#     is_revoked = Column(Boolean, default=False)
#     tenant_id = Column(String, index=True)

# class AuditLog(Base):
#     __tablename__ = "audit_logs"
    
#     id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
#     user_id = Column(String, index=True)
#     action = Column(String)
#     resource = Column(String)
#     ip_address = Column(String)
#     user_agent = Column(String)
#     timestamp = Column(DateTime, default=datetime.utcnow)
#     tenant_id = Column(String, index=True)
#     log_metadata = Column(JSON) # Corrected name
#     success = Column(Boolean)

# # Pydantic Models
# class UserCreate(BaseModel):
#     username: str = Field(..., min_length=3, max_length=50)
#     email: str = Field(..., pattern=r'^[^@]+@[^@]+\.[^@]+$')
#     password: str = Field(..., min_length=8)
#     role: UserRole = UserRole.VIEWER
#     tenant_id: str
    
#     @validator('password')
#     def validate_password(cls, v):
#         if not re.search(r'[A-Z]', v):
#             raise ValueError('Password must contain at least one uppercase letter')
#         if not re.search(r'[a-z]', v):
#             raise ValueError('Password must contain at least one lowercase letter')
#         if not re.search(r'\d', v):
#             raise ValueError('Password must contain at least one digit')
#         if not re.search(r'[!@#$%^&*(),.?":{}|<>]', v):
#             raise ValueError('Password must contain at least one special character')
#         return v

# class UserResponse(BaseModel):
#     id: str
#     username: str
#     email: str
#     role: UserRole
#     is_active: bool
#     tenant_id: str
#     created_at: datetime
#     last_login: Optional[datetime]

# class Token(BaseModel):
#     access_token: str
#     token_type: str = "bearer"
#     expires_in: int
#     refresh_token: str

# class TokenData(BaseModel):
#     user_id: str
#     username: str
#     role: UserRole
#     tenant_id: str
#     permissions: List[Permission]
#     exp: int

# class APIKeyCreate(BaseModel):
#     name: str
#     permissions: List[Permission]
#     expires_at: Optional[datetime] = None
#     rate_limit: int = 1000

# class APIKeyResponse(BaseModel):
#     id: str
#     name: str
#     key: str  # Only returned on creation
#     permissions: List[Permission]
#     created_at: datetime
#     expires_at: Optional[datetime]
#     rate_limit: int

# # Redis client for rate limiting and token blacklisting
# # Global redis_client variable will be initialized by AuthManager
# _redis_client_instance: Optional[aioredis.Redis] = None

# # Database engine and session for global dependency
# _db_engine: Optional[Any] = None
# _SessionLocal: Optional[sessionmaker] = None

# async def get_redis_connection(settings: Settings) -> Optional[aioredis.Redis]:
#     """Get a Redis connection from settings."""
#     global _redis_client_instance
#     if _redis_client_instance is None:
#         if settings.redis.enabled:
#             redis_password_part = f":{settings.redis.password}" if settings.redis.password else ""
#             redis_url_str = f"redis://{redis_password_part}@{settings.redis.host}:{settings.redis.port}/{settings.redis.db}"
#             try:
#                 _redis_client_instance = await aioredis.from_url(
#                     redis_url_str,
#                     socket_timeout=settings.redis.socket_timeout,
#                     socket_connect_timeout=settings.redis.socket_connect_timeout
#                 )
#                 await _redis_client_instance.ping()
#                 logger.info("AuthManager: Connected to Redis successfully.")
#             except Exception as e:
#                 logger.warning(f"AuthManager: Could not connect to Redis at {redis_url_str}. Auth features requiring Redis might be limited. Error: {e}")
#                 _redis_client_instance = None
#         else:
#             logger.info("AuthManager: Redis is disabled in settings. Running without Redis support for auth.")
#             _redis_client_instance = None
#     return _redis_client_instance


# def get_db_session(db_url: str) -> Session:
#     """Get a SQLAlchemy database session."""
#     global _db_engine, _SessionLocal
#     if _db_engine is None:
#         _db_engine = create_engine(db_url, connect_args={"check_same_thread": False} if "sqlite" in db_url else {})
#         _SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=_db_engine)
#         logger.info(f"AuthManager: Database engine created for {db_url}")
    
#     if _SessionLocal:
#         db = _SessionLocal()
#         try:
#             yield db
#         finally:
#             db.close()
#     else:
#         logger.error("AuthManager: Database session not initialized.")
#         raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database not initialized")


# # Authentication utilities
# class AuthManager:
#     # Changed __init__ to accept settings
#     def __init__(self, settings: Settings):
#         self.settings = settings
#         self.pwd_context = pwd_context
#         self.jwt_secret_key = self.settings.jwt.secret
#         self.jwt_algorithm = self.settings.jwt.algorithm
#         self.jwt_expiration_delta = timedelta(minutes=self.settings.jwt.access_token_expire_minutes)
#         self.jwt_refresh_expiration_delta = timedelta(days=self.settings.jwt.refresh_token_expire_days)
        
#         # Initialize dependencies that rely on settings
#         self.redis_client: Optional[aioredis.Redis] = None
#         self.rate_limiter: Optional[RateLimiter] = None
#         self.token_blacklist: Optional[TokenBlacklist] = None

#         self.db_engine: Optional[Any] = None
#         self.SessionLocal: Optional[sessionmaker] = None
#         self.user_manager: Optional[UserManager] = None
#         self.api_key_manager: Optional[APIKeyManager] = None
#         self.audit_logger: Optional[AuditLogger] = None

#         # Set up a callable for get_db that uses the instance's SessionLocal
#         self._get_db_instance_callable = lambda: get_db_session(self.settings.api.database_url) # Assuming API config has database_url. Adjust if not.

#     async def initialize(self):
#         """Asynchronously initialize AuthManager dependencies."""
#         logger.info("AuthManager: Initializing...")
        
#         # Initialize Redis components
#         self.redis_client = await get_redis_connection(self.settings)
#         if self.redis_client:
#             self.rate_limiter = RateLimiter(self.redis_client)
#             self.token_blacklist = TokenBlacklist(self.redis_client)
#         else:
#             logger.warning("AuthManager: Redis is not available. Rate limiting and token blacklisting will be non-functional.")

#         # Initialize Database components
#         # Ensure your APIConfig or a new DBConfig in settings.py has a 'database_url'
#         db_url_for_auth = getattr(self.settings.api, 'database_url', None) # Assuming API config for now
#         if not db_url_for_auth:
#              logger.warning("AuthManager: No database_url found in settings for auth. User/APIKey management will be non-functional.")
#         else:
#             try:
#                 # Initialize local engine and session factory
#                 self.db_engine = create_engine(db_url_for_auth, connect_args={"check_same_thread": False} if "sqlite" in db_url_for_auth else {})
#                 Base.metadata.create_all(bind=self.db_engine) # Ensure tables are created
#                 self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.db_engine)
#                 logger.info(f"AuthManager: Database engine initialized for {db_url_for_auth}")

#                 self.user_manager = UserManager(db_session_factory=self.SessionLocal) # Pass factory
#                 self.api_key_manager = APIKeyManager(db_session_factory=self.SessionLocal) # Pass factory
#                 self.audit_logger = AuditLogger(db_session_factory=self.SessionLocal) # Pass factory
#             except Exception as e:
#                 logger.error(f"AuthManager: Failed to initialize database: {e}", exc_info=True)
#                 self.db_engine = None
#                 self.SessionLocal = None
#                 logger.warning("AuthManager: Database is not available. User/APIKey management will be non-functional.")


#         logger.info("AuthManager: Initialization complete.")

#     async def shutdown(self):
#         """Gracefully shut down AuthManager dependencies."""
#         logger.info("AuthManager: Shutting down...")
#         if self.redis_client:
#             await self.redis_client.close()
#             logger.info("AuthManager: Redis client closed.")
#         if self.db_engine:
#             # Dispose of the engine if it has a dispose method (e.g., SQLAlchemy async engines)
#             if hasattr(self.db_engine, 'dispose'):
#                 self.db_engine.dispose()
#                 logger.info("AuthManager: Database engine disposed.")
#         logger.info("AuthManager: Shutdown complete.")

#     def verify_password(self, plain_password: str, hashed_password: str) -> bool:
#         return self.pwd_context.verify(plain_password, hashed_password)
    
#     def get_password_hash(self, password: str) -> str:
#         return self.pwd_context.hash(password)
    
#     def create_access_token(self, data: dict, expires_delta: Optional[timedelta] = None) -> str:
#         to_encode = data.copy()
#         if expires_delta:
#             expire = datetime.utcnow() + expires_delta
#         else:
#             expire = datetime.utcnow() + self.jwt_expiration_delta
        
#         to_encode.update({"exp": expire.timestamp(), "type": "access"}) # Use timestamp for JWT exp
#         encoded_jwt = jwt.encode(to_encode, self.jwt_secret_key, algorithm=self.jwt_algorithm)
#         return encoded_jwt
    
#     def create_refresh_token(self, user_id: str, tenant_id: str) -> str:
#         data = {
#             "user_id": user_id,
#             "tenant_id": tenant_id,
#             "type": "refresh",
#             "exp": (datetime.utcnow() + self.jwt_refresh_expiration_delta).timestamp() # Use timestamp for JWT exp
#         }
#         return jwt.encode(data, self.jwt_secret_key, algorithm=self.jwt_algorithm)
    
#     def verify_token(self, token: str) -> Optional[TokenData]:
#         try:
#             payload = jwt.decode(token, self.jwt_secret_key, algorithms=[self.jwt_algorithm])
            
#             # Check if using PyJWT or authlib.jose.jwt
#             # If using authlib.jose.jwt:
#             # payload.validate() # This is if 'jwt' is from authlib.jose.jwt
            
#             if payload.get("type") != "access":
#                 logger.warning(f"Invalid token type: {payload.get('type')}")
#                 return None
            
#             user_id = payload.get("user_id")
#             username = payload.get("username")
#             role = payload.get("role")
#             tenant_id = payload.get("tenant_id")
#             permissions = payload.get("permissions", [])
#             exp = payload.get("exp")
            
#             if not all([user_id, username, role, tenant_id, exp]): # Ensure exp is present
#                 logger.warning("Missing essential fields in token payload.")
#                 return None
            
#             # Check expiration manually if PyJWT is used and validation is not implicit
#             if datetime.fromtimestamp(exp) < datetime.utcnow():
#                 logger.warning("Token has expired.")
#                 return None

#             return TokenData(
#                 user_id=user_id,
#                 username=username,
#                 role=UserRole(role),
#                 tenant_id=tenant_id,
#                 permissions=[Permission(p) for p in permissions],
#                 exp=exp
#             )
#         except jwt.ExpiredSignatureError:
#             logger.warning("Token expired signature error.")
#             return None
#         except jwt.InvalidTokenError as e: # Catch general JWT errors
#             logger.warning(f"Invalid JWT token: {e}")
#             return None
#         except ValidationError as e: # Catch Pydantic validation errors for TokenData
#             logger.warning(f"TokenData validation failed: {e}")
#             return None
#         except Exception as e:
#             logger.error(f"Unexpected error during token verification: {e}", exc_info=True)
#             return None
    
#     def generate_api_key(self) -> str:
#         return f"{self.settings.api.api_key_prefix}{secrets.token_urlsafe(32)}" # Use settings for prefix
    
#     def hash_api_key(self, api_key: str) -> str:
#         return hashlib.sha256(api_key.encode()).hexdigest()

# # Note: The global auth_manager = AuthManager() at the end of the previous structure
# # should be replaced by dependency injection using the initialized instance from main.py

# # Rate limiting (updated to accept Redis client directly)
# class RateLimiter:
#     def __init__(self, redis_client: Optional[aioredis.Redis]):
#         self.redis = redis_client
#         if not self.redis:
#             logger.warning("RateLimiter initialized without Redis client. Rate limiting will be non-functional.")
    
#     async def is_allowed(self, key: str, limit: int, window: int = 3600) -> bool:
#         """Check if request is allowed based on rate limit"""
#         if not self.redis:
#             return True # Fail open if Redis not available
        
#         current_time = int(time.time())
#         window_start = current_time - window
        
#         try:
#             async with self.redis.pipeline() as pipe:
#                 pipe.zremrangebyscore(key, 0, window_start)
#                 pipe.zcard(key)
#                 pipe.zadd(key, {str(current_time): current_time})
#                 pipe.expire(key, window)
#                 results = await pipe.execute()
            
#             return results[1] < limit
#         except Exception as e:
#             logger.error(f"RateLimiter Redis operation failed for key {key}: {e}", exc_info=True)
#             return True # Fail open

# # Token blacklisting (updated to accept Redis client directly)
# class TokenBlacklist:
#     def __init__(self, redis_client: Optional[aioredis.Redis]):
#         self.redis = redis_client
#         if not self.redis:
#             logger.warning("TokenBlacklist initialized without Redis client. Token blacklisting will be non-functional.")

#     async def add_token(self, token: str, exp: int):
#         """Add token to blacklist"""
#         if not self.redis:
#             return
#         ttl = exp - int(time.time())
#         if ttl > 0:
#             try:
#                 await self.redis.setex(f"blacklist:{token}", ttl, "1")
#             except Exception as e:
#                 logger.error(f"Failed to add token to blacklist: {e}", exc_info=True)
    
#     async def is_blacklisted(self, token: str) -> bool:
#         """Check if token is blacklisted"""
#         if not self.redis:
#             return False # Not blacklisted if service is down
#         try:
#             result = await self.redis.get(f"blacklist:{token}")
#             return result is not None
#         except Exception as e:
#             logger.error(f"Failed to check token blacklist: {e}", exc_info=True)
#             return False # Assume not blacklisted if service is down

# # User management (updated to accept session factory for proper async handling)
# class UserManager:
#     def __init__(self, db_session_factory: Any):
#         self.db_session_factory = db_session_factory
#         # No direct db session here, obtain it per-request via dependency

#     async def create_user(self, user_data: UserCreate) -> User:
#         async with self.db_session_factory() as db:
#             hashed_password = pwd_context.hash(user_data.password) # Use pwd_context directly
#             user = User(
#                 username=user_data.username,
#                 email=user_data.email,
#                 hashed_password=hashed_password,
#                 role=user_data.role,
#                 tenant_id=user_data.tenant_id
#             )
#             db.add(user)
#             await db.commit() # Use await for async SQLAlchemy methods
#             await db.refresh(user) # Use await
#             return user
    
#     async def get_user_by_username(self, username: str) -> Optional[User]:
#         async with self.db_session_factory() as db:
#             # Use async query methods if db is async session
#             return await db.query(User).filter(User.username == username).first()
    
#     async def get_user_by_email(self, email: str) -> Optional[User]:
#         async with self.db_session_factory() as db:
#             return await db.query(User).filter(User.email == email).first()
    
#     async def get_user_by_id(self, user_id: str) -> Optional[User]:
#         async with self.db_session_factory() as db:
#             return await db.query(User).filter(User.id == user_id).first()
    
#     async def authenticate_user(self, username: str, password: str) -> Optional[User]:
#         async with self.db_session_factory() as db:
#             user = await self.get_user_by_username(username)
#             if not user or not user.is_active:
#                 return None
            
#             # Check if user is locked
#             if user.locked_until and user.locked_until > datetime.utcnow():
#                 return None
            
#             if not pwd_context.verify(password, user.hashed_password): # Use pwd_context directly
#                 # Increment failed attempts
#                 user.failed_login_attempts += 1
#                 if user.failed_login_attempts >= 5:
#                     user.locked_until = datetime.utcnow() + timedelta(minutes=30)
#                 await db.commit()
#                 return None
            
#             # Reset failed attempts on successful login
#             user.failed_login_attempts = 0
#             user.locked_until = None
#             user.last_login = datetime.utcnow()
#             await db.commit()
            
#             return user
    
#     async def update_user_role(self, user_id: str, new_role: UserRole):
#         async with self.db_session_factory() as db:
#             user = await self.get_user_by_id(user_id)
#             if user:
#                 user.role = new_role
#                 await db.commit()
    
#     async def deactivate_user(self, user_id: str):
#         async with self.db_session_factory() as db:
#             user = await self.get_user_by_id(user_id)
#             if user:
#                 user.is_active = False
#                 await db.commit()

# # API Key management (updated to accept session factory)
# class APIKeyManager:
#     def __init__(self, db_session_factory: Any):
#         self.db_session_factory = db_session_factory
    
#     async def create_api_key(self, user_id: str, tenant_id: str, key_data: APIKeyCreate) -> tuple[APIKey, str]:
#         async with self.db_session_factory() as db:
#             # Need an instance of AuthManager here to call generate_api_key/hash_api_key
#             # For simplicity, passing global auth_manager as part of this class or a separate dependency.
#             # Assuming for now, this will be accessed by the AuthManager instance, which holds its own auth_manager callable.
#             # A more robust solution might inject AuthManager's instance methods or a dedicated APIKeyAuth service.
            
#             api_key = secrets.token_urlsafe(32) # Generate raw key here
#             key_hash = hashlib.sha256(api_key.encode()).hexdigest() # Hash it

#             # Assuming API_KEY_PREFIX will be fetched from settings inside AuthManager instance
#             # For now, manually prefixing based on your old global.
#             api_key_with_prefix = f"ak_{api_key}" # This is problematic as prefix should come from settings
#             # Will need to refactor how AuthManager's `generate_api_key` is called if it should use settings.

#             db_key = APIKey(
#                 key_hash=key_hash,
#                 name=key_data.name,
#                 user_id=user_id,
#                 tenant_id=tenant_id,
#                 permissions=[p.value for p in key_data.permissions],
#                 expires_at=key_data.expires_at,
#                 rate_limit=key_data.rate_limit
#             )
#             db.add(db_key)
#             await db.commit()
#             await db.refresh(db_key)
            
#             return db_key, api_key_with_prefix # Return the actual key to the user

#     async def get_api_key_by_hash(self, key_hash: str) -> Optional[APIKey]:
#         async with self.db_session_factory() as db:
#             return await db.query(APIKey).filter(APIKey.key_hash == key_hash).first()
    
#     async def authenticate_api_key(self, api_key: str) -> Optional[APIKey]:
#         key_hash = hashlib.sha256(api_key.encode()).hexdigest() # Hash the provided key
#         db_key = await self.get_api_key_by_hash(key_hash)
        
#         if not db_key or not db_key.is_active:
#             return None
        
#         if db_key.expires_at and db_key.expires_at < datetime.utcnow():
#             return None
        
#         # Update last used
#         db_key.last_used = datetime.utcnow()
#         await db.commit()
        
#         return db_key
    
#     async def revoke_api_key(self, key_id: str):
#         async with self.db_session_factory() as db:
#             key = await db.query(APIKey).filter(APIKey.id == key_id).first()
#             if key:
#                 key.is_active = False
#                 await db.commit()

# # Audit logging (updated to accept session factory)
# class AuditLogger:
#     def __init__(self, db_session_factory: Any):
#         self.db_session_factory = db_session_factory
    
#     async def log_event(self, user_id: str, action: str, resource: str, 
#                         ip_address: str, user_agent: str, tenant_id: str, 
#                         success: bool = True, metadata: dict = None):
#         async with self.db_session_factory() as db:
#             log_entry = AuditLog(
#                 user_id=user_id,
#                 action=action,
#                 resource=resource,
#                 ip_address=ip_address,
#                 user_agent=user_agent,
#                 tenant_id=tenant_id,
#                 success=success,
#                 log_metadata=metadata or {}
#             )
#             db.add(log_entry)
#             await db.commit()


# # Authentication dependencies (will use the injected AuthManager instance)
# # These functions will be methods on the AuthManager class or helpers that receive AuthManager
# # For now, adapting them to receive AuthManager as a dependency.

# async def get_current_user_from_auth_manager(
#     auth_manager_instance: AuthManager = Depends(lambda: _auth_manager_instance), # Global instance
#     credentials: HTTPAuthorizationCredentials = Security(security),
#     db: Session = Depends(lambda: auth_manager_instance._get_db_instance_callable()) # Use injected factory
# ) -> User:
#     """Get current user from JWT token"""
#     if not auth_manager_instance.token_blacklist:
#         raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Token blacklisting service unavailable.")

#     # Check if token is blacklisted
#     if await auth_manager_instance.token_blacklist.is_blacklisted(credentials.credentials):
#         raise HTTPException(
#             status_code=status.HTTP_401_UNAUTHORIZED,
#             detail="Token has been revoked"
#         )
    
#     token_data = auth_manager_instance.verify_token(credentials.credentials)
#     if not token_data:
#         raise HTTPException(
#             status_code=status.HTTP_401_UNAUTHORIZED,
#             detail="Invalid token"
#         )
    
#     if not auth_manager_instance.user_manager:
#         raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="User management service unavailable.")

#     user = await auth_manager_instance.user_manager.get_user_by_id(token_data.user_id)
#     if not user or not user.is_active:
#         raise HTTPException(
#             status_code=status.HTTP_401_UNAUTHORIZED,
#             detail="User not found or inactive"
#         )
    
#     return user

# async def get_current_user_from_api_key_from_auth_manager(
#     auth_manager_instance: AuthManager = Depends(lambda: _auth_manager_instance), # Global instance
#     api_key_str: str = Security(api_key_header),
#     db: Session = Depends(lambda: auth_manager_instance._get_db_instance_callable()) # Use injected factory
# ) -> Optional[User]:
#     """Get current user from API key"""
#     if not api_key_str:
#         return None
    
#     if not auth_manager_instance.api_key_manager:
#         raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="API Key management service unavailable.")
    
#     db_key = await auth_manager_instance.api_key_manager.authenticate_api_key(api_key_str)
    
#     if not db_key:
#         return None
    
#     if not auth_manager_instance.rate_limiter:
#         logger.warning("Rate limiter service unavailable for API key, bypassing rate limit check.")
#     elif not await auth_manager_instance.rate_limiter.is_allowed(f"api_key:{db_key.id}", db_key.rate_limit):
#         raise HTTPException(
#             status_code=status.HTTP_429_TOO_MANY_REQUESTS,
#             detail="Rate limit exceeded"
#         )
    
#     if not auth_manager_instance.user_manager:
#         raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="User management service unavailable.")

#     user = await auth_manager_instance.user_manager.get_user_by_id(db_key.user_id)
    
#     if user:
#         # Add API key permissions to user context
#         user.api_key_permissions = [Permission(p) for p in db_key.permissions]
    
#     return user

# async def get_current_user_flexible_from_auth_manager(
#     credentials: Optional[HTTPAuthorizationCredentials] = Security(security, auto_error=False),
#     api_key_str: Optional[str] = Security(api_key_header, auto_error=False),
#     auth_manager_instance: AuthManager = Depends(lambda: _auth_manager_instance), # Global instance
#     db: Session = Depends(lambda: auth_manager_instance._get_db_instance_callable()) # Use injected factory
# ) -> User:
#     """Get current user from either JWT token or API key"""
#     user = None
    
#     # Try API key first
#     if api_key_str:
#         user = await get_current_user_from_api_key_from_auth_manager(
#             auth_manager_instance=auth_manager_instance,
#             api_key_str=api_key_str,
#             db=db
#         )
    
#     # If no API key or API key failed, try JWT
#     if not user and credentials:
#         user = await get_current_user_from_auth_manager(
#             auth_manager_instance=auth_manager_instance,
#             credentials=credentials,
#             db=db
#         )
    
#     if not user:
#         raise HTTPException(
#             status_code=status.HTTP_401_UNAUTHORIZED,
#             detail="Authentication required"
#         )
    
#     return user

# # Permission checking
# def require_permission(permission: Permission):
#     """Decorator to require specific permission"""
#     def decorator(func):
#         @wraps(func)
#         async def wrapper(*args, **kwargs):
#             # Get current user from function arguments (FastAPI will inject it)
#             current_user: Optional[User] = None
#             for arg in args:
#                 if isinstance(arg, User): # Find the User object passed by FastAPI
#                     current_user = arg
#                     break
            
#             if not current_user:
#                 # Fallback if user is not directly in args (e.g., if using custom dependency)
#                 # In FastAPI, this typically works by having the dependency return the User object.
#                 # If you use Depends(get_current_user_flexible_from_auth_manager) directly,
#                 # the returned User object should be in kwargs.
#                 if 'current_user' in kwargs and isinstance(kwargs['current_user'], User):
#                     current_user = kwargs['current_user']

#             if not current_user:
#                 raise HTTPException(
#                     status_code=status.HTTP_403_FORBIDDEN,
#                     detail="Permission denied: User object not found in request context."
#                 )
            
#             # Check user role permissions
#             user_permissions = set(ROLE_PERMISSIONS.get(UserRole(current_user.role), []))
            
#             # Check API key permissions if available and override/supplement
#             if hasattr(current_user, 'api_key_permissions') and current_user.api_key_permissions:
#                 # If an API key is used, its permissions override role permissions
#                 # or are combined based on your security model.
#                 # Here, assuming API key permissions are stricter or define allowed scope.
#                 user_permissions = set(current_user.api_key_permissions) 
            
#             if permission not in user_permissions:
#                 raise HTTPException(
#                     status_code=status.HTTP_403_FORBIDDEN,
#                     detail=f"Permission {permission.value} required"
#                 )
            
#             return await func(*args, **kwargs)
#         return wrapper
#     return decorator

# # Multi-tenant support
# def require_tenant_access(tenant_id_param: str, user: User): # Renamed tenant_id to avoid conflict with method param
#     """Check if user has access to specific tenant"""
#     if user.role == UserRole.ADMIN:
#         return True  # Admin can access all tenants
    
#     if user.tenant_id != tenant_id_param:
#         raise HTTPException(
#             status_code=status.HTTP_403_FORBIDDEN,
#             detail="Access denied to this tenant"
#         )
    
#     return True

# # Security headers middleware
# async def add_security_headers(request: Request, call_next):
#     response = await call_next(request)
    
#     response.headers["X-Content-Type-Options"] = "nosniff"
#     response.headers["X-Frame-Options"] = "DENY"
#     response.headers["X-XSS-Protection"] = "1; mode=block"
#     response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
#     response.headers["Content-Security-Policy"] = "default-src 'self'"
    
#     return response

# # Initialize database (called once at startup by main.py's AuthManager)
# def init_db(engine: Any): # Accepts engine now
#     Base.metadata.create_all(bind=engine)
#     logger.info("Database tables ensured.")


# # Testing utilities
# class AuthTestUtils:
#     @staticmethod
#     async def create_test_user(db_session: Session, username: str = "testuser", # Changed db to db_session
#                         role: UserRole = UserRole.VIEWER, 
#                         tenant_id: str = "test_tenant") -> User:
#         """Create a test user for testing purposes"""
#         # Assume a global auth_manager_instance is available or passed
#         # For test utils, it's common to instantiate needed managers directly or mock them.
#         # This function should ideally take `UserManager` instance.
#         user_manager_for_test = UserManager(db_session_factory=lambda: db_session) # Temporary factory for test
#         user_data = UserCreate(
#             username=username,
#             email=f"{username}@example.com",
#             password="TestPass123!",
#             role=role,
#             tenant_id=tenant_id
#         )
#         return await user_manager_for_test.create_user(user_data) # Await

#     @staticmethod
#     async def create_test_token(user: User, auth_manager_instance: 'AuthManager') -> str: # Takes AuthManager instance
#         """Create a test JWT token"""
#         data = {
#             "user_id": user.id,
#             "username": user.username,
#             "role": user.role.value, # Use .value for enum
#             "tenant_id": user.tenant_id,
#             "permissions": [p.value for p in ROLE_PERMISSIONS.get(UserRole(user.role), [])]
#         }
#         return auth_manager_instance.create_access_token(data)
    
#     @staticmethod
#     async def create_test_api_key(db_session: Session, user_id: str, # Changed db to db_session
#                            permissions: Optional[List[Permission]] = None) -> tuple[APIKey, str]: # Added Optional
#         """Create a test API key"""
#         if permissions is None:
#             permissions = [Permission.READ_ALERTS]
        
#         key_data = APIKeyCreate(
#             name="Test Key",
#             permissions=permissions
#         )
#         api_key_manager_for_test = APIKeyManager(db_session_factory=lambda: db_session) # Temporary factory for test
#         return await api_key_manager_for_test.create_api_key(user_id, "test_tenant", key_data) # Await


# # Global instance of AuthManager, initialized in main.py's startup sequence
# _auth_manager_instance: Optional[AuthManager] = None

# def get_auth_manager_instance() -> AuthManager:
#     """Provides the singleton AuthManager instance."""
#     if _auth_manager_instance is None:
#         logger.error("AuthManager instance not initialized globally!")
#         raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Authentication system not initialized.")
#     return _auth_manager_instance

# # Export main components
# __all__ = [
#     'AuthManager', 'UserManager', 'APIKeyManager', 'AuditLogger',
#     'get_current_user_from_auth_manager', # Renamed dependency function
#     'get_current_user_from_api_key_from_auth_manager', # Renamed dependency function
#     'get_current_user_flexible_from_auth_manager', # Renamed dependency function
#     'get_auth_manager_instance', # New function to get the initialized instance
#     'require_permission', 'require_tenant_access', 'add_security_headers',
#     'UserRole', 'Permission', 'UserCreate', 'UserResponse', 'Token', 'APIKeyCreate',
#     'APIKeyResponse', 'init_db', 'AuthTestUtils'
# ]

# """
# Comprehensive authentication and authorization system for the analytics API.
# Implements JWT-based authentication with refresh tokens, role-based access control,
# API key authentication, OAuth2 integration, and comprehensive security features.
# """

# import asyncio
# import hashlib
# import secrets
# import time
# from datetime import datetime, timedelta
# from typing import Optional, Dict, List, Any, Union
# from functools import wraps
# from enum import Enum
# import re
# import json
# import uuid

# import jwt # This is the PyJWT library, make sure it's installed (pip install PyJWT)
# from passlib.context import CryptContext
# from fastapi import HTTPException, Security, Depends, Request, status
# from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials, APIKeyHeader
# from fastapi.middleware.cors import CORSMiddleware
# from fastapi.responses import JSONResponse
# from pydantic import BaseModel, Field, validator, ValidationError
# import redis.asyncio as aioredis # Use async Redis client consistently
# from sqlalchemy import create_engine, Column, String, DateTime, Boolean, JSON, Integer, Text
# from sqlalchemy.ext.declarative import declarative_base
# from sqlalchemy.orm import sessionmaker, Session
# from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession # For async DB operations, assuming you plan to use it
# import logging
# import os

# # Import your global settings
# from config.settings import Settings # Assuming your main settings class is named Settings

# # Logger
# logger = logging.getLogger(__name__)

# # Security
# pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
# security = HTTPBearer()
# api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

# # Database setup (engine and SessionLocal will be initialized by AuthManager for dependency injection)
# Base = declarative_base()


# # Enums
# class UserRole(str, Enum):
#     ADMIN = "admin"
#     OPERATOR = "operator"
#     VIEWER = "viewer"

# class Permission(str, Enum):
#     READ_ALERTS = "read_alerts"
#     WRITE_ALERTS = "write_alerts"
#     READ_ANALYTICS = "read_analytics"
#     WRITE_ANALYTICS = "write_analytics"
#     READ_CONFIG = "read_config"
#     WRITE_CONFIG = "write_config"
#     MANAGE_USERS = "manage_users"
#     MANAGE_API_KEYS = "manage_api_keys"

# # Role permissions mapping
# ROLE_PERMISSIONS = {
#     UserRole.ADMIN: [
#         Permission.READ_ALERTS, Permission.WRITE_ALERTS,
#         Permission.READ_ANALYTICS, Permission.WRITE_ANALYTICS,
#         Permission.READ_CONFIG, Permission.WRITE_CONFIG,
#         Permission.MANAGE_USERS, Permission.MANAGE_API_KEYS
#     ],
#     UserRole.OPERATOR: [
#         Permission.READ_ALERTS, Permission.WRITE_ALERTS,
#         Permission.READ_ANALYTICS, Permission.WRITE_ANALYTICS,
#         Permission.READ_CONFIG
#     ],
#     UserRole.VIEWER: [
#         Permission.READ_ALERTS, Permission.READ_ANALYTICS, Permission.READ_CONFIG
#     ]
# }

# # Database Models (using the corrected 'metadata' column names)
# class User(Base):
#     __tablename__ = "users"
    
#     id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
#     username = Column(String, unique=True, index=True)
#     email = Column(String, unique=True, index=True)
#     hashed_password = Column(String)
#     role = Column(String, default=UserRole.VIEWER)
#     is_active = Column(Boolean, default=True)
#     tenant_id = Column(String, index=True)
#     created_at = Column(DateTime, default=datetime.utcnow)
#     last_login = Column(DateTime)
#     failed_login_attempts = Column(Integer, default=0)
#     locked_until = Column(DateTime)
#     password_changed_at = Column(DateTime, default=datetime.utcnow)
#     oauth_provider = Column(String)
#     oauth_id = Column(String)
#     user_metadata = Column(JSON) # Corrected name

# class APIKey(Base):
#     __tablename__ = "api_keys"
    
#     id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
#     key_hash = Column(String, unique=True, index=True)
#     name = Column(String)
#     user_id = Column(String, index=True)
#     tenant_id = Column(String, index=True)
#     permissions = Column(JSON)
#     is_active = Column(Boolean, default=True)
#     created_at = Column(DateTime, default=datetime.utcnow)
#     last_used = Column(DateTime)
#     expires_at = Column(DateTime)
#     rate_limit = Column(Integer, default=1000)  # requests per hour
#     key_metadata = Column(JSON) # Corrected name

# class RefreshToken(Base):
#     __tablename__ = "refresh_tokens"
    
#     id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
#     token_hash = Column(String, unique=True, index=True)
#     user_id = Column(String, index=True)
#     created_at = Column(DateTime, default=datetime.utcnow)
#     expires_at = Column(DateTime)
#     is_revoked = Column(Boolean, default=False)
#     tenant_id = Column(String, index=True)

# class AuditLog(Base):
#     __tablename__ = "audit_logs"
    
#     id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
#     user_id = Column(String, index=True)
#     action = Column(String)
#     resource = Column(String)
#     ip_address = Column(String)
#     user_agent = Column(String)
#     timestamp = Column(DateTime, default=datetime.utcnow)
#     tenant_id = Column(String, index=True)
#     log_metadata = Column(JSON) # Corrected name
#     success = Column(Boolean)

# # Pydantic Models
# class UserCreate(BaseModel):
#     username: str = Field(..., min_length=3, max_length=50)
#     email: str = Field(..., pattern=r'^[^@]+@[^@]+\.[^@]+$')
#     password: str = Field(..., min_length=8)
#     role: UserRole = UserRole.VIEWER
#     tenant_id: str
    
#     @validator('password')
#     def validate_password(cls, v):
#         if not re.search(r'[A-Z]', v):
#             raise ValueError('Password must contain at least one uppercase letter')
#         if not re.search(r'[a-z]', v):
#             raise ValueError('Password must contain at least one lowercase letter')
#         if not re.search(r'\d', v):
#             raise ValueError('Password must contain at least one digit')
#         if not re.search(r'[!@#$%^&*(),.?":{}|<>]', v):
#             raise ValueError('Password must contain at least one special character')
#         return v

# class UserResponse(BaseModel):
#     id: str
#     username: str
#     email: str
#     role: UserRole
#     is_active: bool
#     tenant_id: str
#     created_at: datetime
#     last_login: Optional[datetime]

# class Token(BaseModel):
#     access_token: str
#     token_type: str = "bearer"
#     expires_in: int
#     refresh_token: str

# class TokenData(BaseModel):
#     user_id: str
#     username: str
#     role: UserRole
#     tenant_id: str
#     permissions: List[Permission]
#     exp: int

# class APIKeyCreate(BaseModel):
#     name: str
#     permissions: List[Permission]
#     expires_at: Optional[datetime] = None
#     rate_limit: int = 1000

# class APIKeyResponse(BaseModel):
#     id: str
#     name: str
#     key: str  # Only returned on creation
#     permissions: List[Permission]
#     created_at: datetime
#     expires_at: Optional[datetime]
#     rate_limit: int

# # Global redis_client variable will be initialized by AuthManager
# _redis_client_instance: Optional[aioredis.Redis] = None

# # Database engine and session for global dependency - no longer needed as AuthManager manages its own
# # _db_engine: Optional[Any] = None
# # _SessionLocal: Optional[sessionmaker] = None

# async def get_redis_connection_from_settings(settings: Settings) -> Optional[aioredis.Redis]:
#     """Get a Redis connection from settings."""
#     global _redis_client_instance # This global should ideally be managed by the AuthManager instance
#     # For now, keeping it here for simple dependency injection in FastAPI `Depends`
    
#     # If the instance is already created and connected, return it
#     if _redis_client_instance is not None and await _redis_client_instance.ping():
#         return _redis_client_instance

#     if settings.redis.enabled:
#         # Construct Redis URL based on settings
#         redis_password_part = f":{settings.redis.password}" if settings.redis.password else ""
#         redis_url_str = f"redis://{redis_password_part}@{settings.redis.host}:{settings.redis.port}/{settings.redis.db}"
#         try:
#             _redis_client_instance = aioredis.from_url(
#                 redis_url_str,
#                 socket_timeout=settings.redis.socket_timeout,
#                 socket_connect_timeout=settings.redis.socket_connect_timeout
#             )
#             await _redis_client_instance.ping()
#             logger.info("AuthModule: Connected to Redis successfully.")
#             return _redis_client_instance
#         except Exception as e:
#             logger.warning(f"AuthModule: Could not connect to Redis at {redis_url_str}. Auth features requiring Redis might be limited. Error: {e}")
#             _redis_client_instance = None # Ensure it's None if connection fails
#             return None
#     else:
#         logger.info("AuthModule: Redis is disabled in settings. Running without Redis support for auth.")
#         _redis_client_instance = None
#         return None


# # Authentication utilities
# class AuthManager:
#     def __init__(self, settings: Settings):
#         self.settings = settings
#         self.pwd_context = pwd_context
#         self.jwt_secret_key = self.settings.jwt.secret
#         self.jwt_algorithm = self.settings.jwt.algorithm
#         self.jwt_expiration_delta = timedelta(minutes=self.settings.jwt.access_token_expire_minutes)
#         self.jwt_refresh_expiration_delta = timedelta(days=self.settings.jwt.refresh_token_expire_days)
        
#         self.redis_client: Optional[aioredis.Redis] = None
#         self.rate_limiter: Optional['RateLimiter'] = None # Forward reference
#         self.token_blacklist: Optional['TokenBlacklist'] = None # Forward reference

#         self.db_engine: Optional[Any] = None
#         self.AsyncSessionLocal: Optional[sessionmaker] = None # Using AsyncSessionLocal

#         self.user_manager: Optional['UserManager'] = None
#         self.api_key_manager: Optional['APIKeyManager'] = None
#         self.audit_logger: Optional['AuditLogger'] = None

#     async def initialize(self):
#         """Asynchronously initialize AuthManager dependencies."""
#         logger.info("AuthManager: Initializing...")
        
#         # Initialize Redis components
#         self.redis_client = await get_redis_connection_from_settings(self.settings)
#         if self.redis_client:
#             self.rate_limiter = RateLimiter(self.redis_client)
#             self.token_blacklist = TokenBlacklist(self.redis_client)
#         else:
#             logger.warning("AuthManager: Redis is not available. Rate limiting and token blacklisting will be non-functional.")

#         # Initialize Database components
#         db_url_for_auth = self.settings.api.database_url
#         if not db_url_for_auth:
#              logger.warning("AuthManager: No database_url found in API settings. User/APIKey management will be non-functional.")
#         else:
#             try:
#                 # Use create_async_engine for async SQLAlchemy
#                 self.db_engine = create_async_engine(db_url_for_auth, echo=False) # echo=True for SQL logs
#                 self.AsyncSessionLocal = sessionmaker(
#                     autocommit=False, autoflush=False, bind=self.db_engine, class_=AsyncSession
#                 )
                
#                 # Create tables (requires async context for metadata.create_all)
#                 async with self.db_engine.begin() as conn:
#                     await conn.run_sync(Base.metadata.create_all)

#                 logger.info(f"AuthManager: Database engine initialized for {db_url_for_auth}")

#                 # Pass async session factory to managers
#                 self.user_manager = UserManager(db_session_factory=self.AsyncSessionLocal)
#                 self.api_key_manager = APIKeyManager(db_session_factory=self.AsyncSessionLocal, settings=self.settings) # Pass settings
#                 self.audit_logger = AuditLogger(db_session_factory=self.AsyncSessionLocal)
#             except Exception as e:
#                 logger.error(f"AuthManager: Failed to initialize database: {e}", exc_info=True)
#                 self.db_engine = None
#                 self.AsyncSessionLocal = None
#                 logger.warning("AuthManager: Database is not available. User/APIKey management will be non-functional.")

#         logger.info("AuthManager: Initialization complete.")

#     async def shutdown(self):
#         """Gracefully shut down AuthManager dependencies."""
#         logger.info("AuthManager: Shutting down...")
#         if self.redis_client:
#             await self.redis_client.close()
#             logger.info("AuthManager: Redis client closed.")
#         if self.db_engine:
#             await self.db_engine.dispose() # Async dispose
#             logger.info("AuthManager: Database engine disposed.")
#         logger.info("AuthManager: Shutdown complete.")

#     def verify_password(self, plain_password: str, hashed_password: str) -> bool:
#         return self.pwd_context.verify(plain_password, hashed_password)
    
#     def get_password_hash(self, password: str) -> str:
#         return self.pwd_context.hash(password)
    
#     def create_access_token(self, data: dict, expires_delta: Optional[timedelta] = None) -> str:
#         to_encode = data.copy()
#         if expires_delta:
#             expire = datetime.utcnow() + expires_delta
#         else:
#             expire = datetime.utcnow() + self.jwt_expiration_delta
        
#         to_encode.update({"exp": int(expire.timestamp()), "type": "access"}) # Use int timestamp for JWT exp
#         encoded_jwt = jwt.encode(to_encode, self.jwt_secret_key, algorithm=self.jwt_algorithm)
#         return encoded_jwt
    
#     def create_refresh_token(self, user_id: str, tenant_id: str) -> str:
#         data = {
#             "user_id": user_id,
#             "tenant_id": tenant_id,
#             "type": "refresh",
#             "exp": int((datetime.utcnow() + self.jwt_refresh_expiration_delta).timestamp()) # Use int timestamp for JWT exp
#         }
#         return jwt.encode(data, self.jwt_secret_key, algorithm=self.jwt_algorithm)
    
#     def verify_token(self, token: str) -> Optional[TokenData]:
#         try:
#             payload = jwt.decode(token, self.jwt_secret_key, algorithms=[self.jwt_algorithm])
            
#             if payload.get("type") != "access":
#                 logger.warning(f"Invalid token type: {payload.get('type')}")
#                 return None
            
#             user_id = payload.get("user_id")
#             username = payload.get("username")
#             role = payload.get("role")
#             tenant_id = payload.get("tenant_id")
#             permissions = payload.get("permissions", [])
#             exp = payload.get("exp")
            
#             if not all([user_id, username, role, tenant_id, exp]):
#                 logger.warning("Missing essential fields in token payload.")
#                 return None
            
#             # Check expiration manually if PyJWT is used and validation is not implicit
#             # PyJWT decode can raise ExpiredSignatureError, but explicit check for robustness
#             if datetime.fromtimestamp(exp) < datetime.utcnow():
#                 logger.warning("Token has expired.")
#                 return None

#             return TokenData(
#                 user_id=user_id,
#                 username=username,
#                 role=UserRole(role),
#                 tenant_id=tenant_id,
#                 permissions=[Permission(p) for p in permissions],
#                 exp=exp
#             )
#         except jwt.ExpiredSignatureError:
#             logger.warning("Token expired signature error.")
#             return None
#         except jwt.InvalidTokenError as e:
#             logger.warning(f"Invalid JWT token: {e}")
#             return None
#         except ValidationError as e:
#             logger.warning(f"TokenData validation failed: {e}")
#             return None
#         except Exception as e:
#             logger.error(f"Unexpected error during token verification: {e}", exc_info=True)
#             return None
    
#     def generate_api_key(self) -> str:
#         return f"{self.settings.api.api_key_prefix}{secrets.token_urlsafe(32)}"
    
#     def hash_api_key(self, api_key: str) -> str:
#         return hashlib.sha256(api_key.encode()).hexdigest()

# # Rate limiting (updated to accept Redis client directly)
# class RateLimiter:
#     def __init__(self, redis_client: Optional[aioredis.Redis]):
#         self.redis = redis_client
#         if not self.redis:
#             logger.warning("RateLimiter initialized without Redis client. Rate limiting will be non-functional.")
    
#     async def is_allowed(self, key: str, limit: int, window: int = 3600) -> bool:
#         """Check if request is allowed based on rate limit"""
#         if not self.redis:
#             return True # Fail open if Redis not available
        
#         current_time = int(time.time())
#         window_start = current_time - window
        
#         try:
#             async with self.redis.pipeline() as pipe:
#                 pipe.zremrangebyscore(key, 0, window_start)
#                 pipe.zcard(key)
#                 pipe.zadd(key, {str(current_time): current_time})
#                 pipe.expire(key, window)
#                 results = await pipe.execute()
            
#             return results[1] < limit
#         except Exception as e:
#             logger.error(f"RateLimiter Redis operation failed for key {key}: {e}", exc_info=True)
#             return True # Fail open

# # Token blacklisting (updated to accept Redis client directly)
# class TokenBlacklist:
#     def __init__(self, redis_client: Optional[aioredis.Redis]):
#         self.redis = redis_client
#         if not self.redis:
#             logger.warning("TokenBlacklist initialized without Redis client. Token blacklisting will be non-functional.")

#     async def add_token(self, token: str, exp: int):
#         """Add token to blacklist"""
#         if not self.redis:
#             return
#         ttl = exp - int(time.time())
#         if ttl > 0:
#             try:
#                 await self.redis.setex(f"blacklist:{token}", ttl, "1")
#             except Exception as e:
#                 logger.error(f"Failed to add token to blacklist: {e}", exc_info=True)
    
#     async def is_blacklisted(self, token: str) -> bool:
#         """Check if token is blacklisted"""
#         if not self.redis:
#             return False # Not blacklisted if service is down
#         try:
#             result = await self.redis.get(f"blacklist:{token}")
#             return result is not None
#         except Exception as e:
#             logger.error(f"Failed to check token blacklist: {e}", exc_info=True)
#             return False # Assume not blacklisted if service is down

# # User management (updated to accept session factory for proper async handling)
# class UserManager:
#     def __init__(self, db_session_factory: Any): # SessionLocal or AsyncSessionLocal
#         self.db_session_factory = db_session_factory

#     async def _get_db(self) -> AsyncSession:
#         """Helper to get an async session."""
#         async with self.db_session_factory() as session:
#             yield session

#     async def create_user(self, user_data: UserCreate) -> User:
#         async for db in self._get_db(): # Use async for
#             hashed_password = pwd_context.hash(user_data.password)
#             user = User(
#                 username=user_data.username,
#                 email=user_data.email,
#                 hashed_password=hashed_password,
#                 role=user_data.role,
#                 tenant_id=user_data.tenant_id
#             )
#             db.add(user)
#             await db.commit()
#             await db.refresh(user)
#             return user
    
#     async def get_user_by_username(self, username: str) -> Optional[User]:
#         async for db in self._get_db():
#             return await db.query(User).filter(User.username == username).first()
    
#     async def get_user_by_email(self, email: str) -> Optional[User]:
#         async for db in self._get_db():
#             return await db.query(User).filter(User.email == email).first()
    
#     async def get_user_by_id(self, user_id: str) -> Optional[User]:
#         async for db in self._get_db():
#             return await db.query(User).filter(User.id == user_id).first()
    
#     async def authenticate_user(self, username: str, password: str) -> Optional[User]:
#         async for db in self._get_db():
#             user = await self.get_user_by_username(username) # Call async method
#             if not user or not user.is_active:
#                 return None
            
#             if user.locked_until and user.locked_until > datetime.utcnow():
#                 return None
            
#             if not pwd_context.verify(password, user.hashed_password):
#                 user.failed_login_attempts += 1
#                 if user.failed_login_attempts >= 5:
#                     user.locked_until = datetime.utcnow() + timedelta(minutes=30)
#                 await db.commit()
#                 return None
            
#             user.failed_login_attempts = 0
#             user.locked_until = None
#             user.last_login = datetime.utcnow()
#             await db.commit()
            
#             return user
    
#     async def update_user_role(self, user_id: str, new_role: UserRole):
#         async for db in self._get_db():
#             user = await self.get_user_by_id(user_id) # Call async method
#             if user:
#                 user.role = new_role
#                 await db.commit()
    
#     async def deactivate_user(self, user_id: str):
#         async for db in self._get_db():
#             user = await self.get_user_by_id(user_id) # Call async method
#             if user:
#                 user.is_active = False
#                 await db.commit()

# # API Key management (updated to accept session factory and settings)
# class APIKeyManager:
#     def __init__(self, db_session_factory: Any, settings: Settings):
#         self.db_session_factory = db_session_factory
#         self.settings = settings
    
#     async def _get_db(self) -> AsyncSession:
#         """Helper to get an async session."""
#         async with self.db_session_factory() as session:
#             yield session

#     async def create_api_key(self, user_id: str, tenant_id: str, key_data: APIKeyCreate) -> tuple[APIKey, str]:
#         async for db in self._get_db():
#             raw_api_key = secrets.token_urlsafe(32)
#             key_with_prefix = f"{self.settings.api.api_key_prefix}{raw_api_key}" # Use prefix from settings
#             key_hash = hashlib.sha256(key_with_prefix.encode()).hexdigest() # Hash the prefixed key
            
#             db_key = APIKey(
#                 key_hash=key_hash,
#                 name=key_data.name,
#                 user_id=user_id,
#                 tenant_id=tenant_id,
#                 permissions=[p.value for p in key_data.permissions],
#                 expires_at=key_data.expires_at,
#                 rate_limit=key_data.rate_limit
#             )
#             db.add(db_key)
#             await db.commit()
#             await db.refresh(db_key)
            
#             return db_key, key_with_prefix

#     async def get_api_key_by_hash(self, key_hash: str) -> Optional[APIKey]:
#         async for db in self._get_db():
#             return await db.query(APIKey).filter(APIKey.key_hash == key_hash).first()
    
#     async def authenticate_api_key(self, api_key_str: str) -> Optional[APIKey]:
#         key_hash = hashlib.sha256(api_key_str.encode()).hexdigest()
#         db_key = await self.get_api_key_by_hash(key_hash)
        
#         if not db_key or not db_key.is_active:
#             return None
        
#         if db_key.expires_at and db_key.expires_at < datetime.utcnow():
#             return None
        
#         db_key.last_used = datetime.utcnow()
#         async for db in self._get_db(): # Commit in context
#             await db.commit()
        
#         return db_key
    
#     async def revoke_api_key(self, key_id: str):
#         async for db in self._get_db():
#             key = await db.query(APIKey).filter(APIKey.id == key_id).first()
#             if key:
#                 key.is_active = False
#                 await db.commit()

# # Audit logging (updated to accept session factory)
# class AuditLogger:
#     def __init__(self, db_session_factory: Any):
#         self.db_session_factory = db_session_factory
    
#     async def _get_db(self) -> AsyncSession:
#         """Helper to get an async session."""
#         async with self.db_session_factory() as session:
#             yield session

#     async def log_event(self, user_id: str, action: str, resource: str, 
#                         ip_address: str, user_agent: str, tenant_id: str, 
#                         success: bool = True, metadata: dict = None):
#         async for db in self._get_db():
#             log_entry = AuditLog(
#                 user_id=user_id,
#                 action=action,
#                 resource=resource,
#                 ip_address=ip_address,
#                 user_agent=user_agent,
#                 tenant_id=tenant_id,
#                 success=success,
#                 log_metadata=metadata or {}
#             )
#             db.add(log_entry)
#             await db.commit()


# # Global instance of AuthManager, initialized in main.py's startup sequence
# _auth_manager_instance: Optional[AuthManager] = None

# def get_auth_manager_instance() -> AuthManager:
#     """Provides the singleton AuthManager instance."""
#     if _auth_manager_instance is None:
#         logger.error("AuthManager instance not initialized globally!")
#         raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Authentication system not initialized.")
#     return _auth_manager_instance

# # Authentication dependencies (now take `AuthManager` instance as a direct dependency)
# # These helper functions simplify the FastAPI `Depends` usage
# async def get_current_user(
#     auth_manager_instance: AuthManager = Depends(get_auth_manager_instance),
#     credentials: HTTPAuthorizationCredentials = Security(security)
# ) -> User:
#     """Get current user from JWT token"""
#     if not auth_manager_instance.token_blacklist:
#         raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Token blacklisting service unavailable.")

#     if await auth_manager_instance.token_blacklist.is_blacklisted(credentials.credentials):
#         raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token has been revoked")
    
#     token_data = auth_manager_instance.verify_token(credentials.credentials)
#     if not token_data:
#         raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")
    
#     if not auth_manager_instance.user_manager:
#         raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="User management service unavailable.")

#     user = await auth_manager_instance.user_manager.get_user_by_id(token_data.user_id)
#     if not user or not user.is_active:
#         raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found or inactive")
    
#     return user

# async def get_current_user_from_api_key(
#     auth_manager_instance: AuthManager = Depends(get_auth_manager_instance),
#     api_key_str: str = Security(api_key_header)
# ) -> Optional[User]:
#     """Get current user from API key"""
#     if not api_key_str:
#         return None
    
#     if not auth_manager_instance.api_key_manager:
#         raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="API Key management service unavailable.")
    
#     db_key = await auth_manager_instance.api_key_manager.authenticate_api_key(api_key_str)
    
#     if not db_key:
#         return None
    
#     if not auth_manager_instance.rate_limiter:
#         logger.warning("Rate limiter service unavailable for API key, bypassing rate limit check.")
#     elif not await auth_manager_instance.rate_limiter.is_allowed(f"api_key:{db_key.id}", db_key.rate_limit):
#         raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail="Rate limit exceeded")
    
#     if not auth_manager_instance.user_manager:
#         raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="User management service unavailable.")

#     user = await auth_manager_instance.user_manager.get_user_by_id(db_key.user_id)
    
#     if user:
#         user.api_key_permissions = [Permission(p) for p in db_key.permissions]
    
#     return user

# async def get_current_user_flexible(
#     credentials: Optional[HTTPAuthorizationCredentials] = Security(security, auto_error=False),
#     api_key_str: Optional[str] = Security(api_key_header, auto_error=False),
#     auth_manager_instance: AuthManager = Depends(get_auth_manager_instance)
# ) -> User:
#     """Get current user from either JWT token or API key"""
#     user = None
    
#     if api_key_str:
#         user = await get_current_user_from_api_key(auth_manager_instance=auth_manager_instance, api_key_str=api_key_str)
    
#     if not user and credentials:
#         user = await get_current_user(auth_manager_instance=auth_manager_instance, credentials=credentials)
    
#     if not user:
#         raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authentication required")
    
#     return user

# # Permission checking
# def require_permission(permission: Permission):
#     """Decorator to require specific permission"""
#     def decorator(func):
#         @wraps(func)
#         async def wrapper(*args, **kwargs):
#             current_user: Optional[User] = None
#             for arg in args:
#                 if isinstance(arg, User):
#                     current_user = arg
#                     break
            
#             if not current_user:
#                 if 'current_user' in kwargs and isinstance(kwargs['current_user'], User):
#                     current_user = kwargs['current_user']

#             if not current_user:
#                 raise HTTPException(
#                     status_code=status.HTTP_403_FORBIDDEN,
#                     detail="Permission denied: User object not found in request context."
#                 )
            
#             user_permissions = set(ROLE_PERMISSIONS.get(UserRole(current_user.role), []))
            
#             if hasattr(current_user, 'api_key_permissions') and current_user.api_key_permissions:
#                 user_permissions.update(current_user.api_key_permissions) # Combine permissions
            
#             if permission not in user_permissions:
#                 raise HTTPException(
#                     status_code=status.HTTP_403_FORBIDDEN,
#                     detail=f"Permission {permission.value} required"
#                 )
            
#             return await func(*args, **kwargs)
#         return wrapper
#     return decorator

# # Multi-tenant support
# def require_tenant_access(tenant_id_param: str, user: User):
#     """Check if user has access to specific tenant"""
#     if user.role == UserRole.ADMIN:
#         return True
    
#     if user.tenant_id != tenant_id_param:
#         raise HTTPException(
#             status_code=status.HTTP_403_FORBIDDEN,
#             detail="Access denied to this tenant"
#         )
    
#     return True

# # Security headers middleware
# async def add_security_headers(request: Request, call_next):
#     response = await call_next(request)
    
#     response.headers["X-Content-Type-Options"] = "nosniff"
#     response.headers["X-Frame-Options"] = "DENY"
#     response.headers["X-XSS-Protection"] = "1; mode=block"
#     response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
#     response.headers["Content-Security-Policy"] = "default-src 'self'"
    
#     return response

# # Initialize database (called once at startup by main.py's AuthManager)
# def init_db(engine: Any): # Accepts engine now
#     # This function is now mostly redundant as AuthManager's initialize() handles this with asyncpg
#     # but keeping it for backward compatibility or if there's another path that calls it.
#     Base.metadata.create_all(bind=engine)
#     logger.info("Database tables ensured by sync init_db (consider using async for primary init).")


# # Testing utilities
# class AuthTestUtils:
#     @staticmethod
#     async def create_test_user(db_session: AsyncSession, username: str = "testuser", 
#                         role: UserRole = UserRole.VIEWER, 
#                         tenant_id: str = "test_tenant") -> User:
#         """Create a test user for testing purposes"""
#         user_manager_for_test = UserManager(db_session_factory=lambda: db_session)
#         user_data = UserCreate(
#             username=username,
#             email=f"{username}@example.com",
#             password="TestPass123!",
#             role=role,
#             tenant_id=tenant_id
#         )
#         return await user_manager_for_test.create_user(user_data)

#     @staticmethod
#     async def create_test_token(user: User, auth_manager_instance: 'AuthManager') -> str:
#         """Create a test JWT token"""
#         data = {
#             "user_id": user.id,
#             "username": user.username,
#             "role": user.role.value,
#             "tenant_id": user.tenant_id,
#             "permissions": [p.value for p in ROLE_PERMISSIONS.get(UserRole(user.role), [])]
#         }
#         return auth_manager_instance.create_access_token(data)
    
#     @staticmethod
#     async def create_test_api_key(db_session: AsyncSession, user_id: str, 
#                            permissions: Optional[List[Permission]] = None, settings: Settings = Depends(get_settings)) -> tuple[APIKey, str]:
#         """Create a test API key"""
#         if permissions is None:
#             permissions = [Permission.READ_ALERTS]
        
#         key_data = APIKeyCreate(
#             name="Test Key",
#             permissions=permissions
#         )
#         api_key_manager_for_test = APIKeyManager(db_session_factory=lambda: db_session, settings=settings)
#         return await api_key_manager_for_test.create_api_key(user_id, "test_tenant", key_data)


# # Export main components
# __all__ = [
#     'AuthManager', 'UserManager', 'APIKeyManager', 'AuditLogger',
#     'get_current_user', # Simplified dependency name for use in API routes
#     'get_current_user_from_api_key', # Simplified dependency name
#     'get_current_user_flexible', # Simplified dependency name
#     'get_auth_manager_instance', # Function to get the initialized instance
#     'require_permission', 'require_tenant_access', 'add_security_headers',
#     'UserRole', 'Permission', 'UserCreate', 'UserResponse', 'Token', 'APIKeyCreate',
#     'APIKeyResponse', 'init_db', 'AuthTestUtils'
# ]

"""
Comprehensive authentication and authorization system for the analytics API.
Implements JWT-based authentication with refresh tokens, role-based access control,
API key authentication, OAuth2 integration, and comprehensive security features.
"""

import asyncio
import hashlib
import secrets
import time
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any, Union
from functools import wraps
from enum import Enum
import re
import json
import uuid

import jwt # This is the PyJWT library, make sure it's installed (pip install PyJWT)
from passlib.context import CryptContext
from fastapi import HTTPException, Security, Depends, Request, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials, APIKeyHeader
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, validator, ValidationError
import redis.asyncio as aioredis # Use async Redis client consistently
from sqlalchemy import create_engine, Column, String, DateTime, Boolean, JSON, Integer, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession # For async DB operations, assuming you plan to use it
import logging
import os

# Import your global settings
from config.settings import Settings, get_settings # ADDED: Import get_settings here

# Logger
logger = logging.getLogger(__name__)

# Security
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
security = HTTPBearer() # Keep this as is for default auto_error=True behavior
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False) # Keep this as is


# Database setup (engine and SessionLocal will be initialized by AuthManager for dependency injection)
Base = declarative_base()


# Enums
class UserRole(str, Enum):
    ADMIN = "admin"
    OPERATOR = "operator"
    VIEWER = "viewer"

class Permission(str, Enum):
    READ_ALERTS = "read_alerts"
    WRITE_ALERTS = "write_alerts"
    READ_ANALYTICS = "read_analytics"
    WRITE_ANALYTICS = "write_analytics"
    READ_CONFIG = "read_config"
    WRITE_CONFIG = "write_config"
    MANAGE_USERS = "manage_users"
    MANAGE_API_KEYS = "manage_api_keys"

# Role permissions mapping
ROLE_PERMISSIONS = {
    UserRole.ADMIN: [
        Permission.READ_ALERTS, Permission.WRITE_ALERTS,
        Permission.READ_ANALYTICS, Permission.WRITE_ANALYTICS,
        Permission.READ_CONFIG, Permission.WRITE_CONFIG,
        Permission.MANAGE_USERS, Permission.MANAGE_API_KEYS
    ],
    UserRole.OPERATOR: [
        Permission.READ_ALERTS, Permission.WRITE_ALERTS,
        Permission.READ_ANALYTICS, Permission.WRITE_ANALYTICS,
        Permission.READ_CONFIG
    ],
    UserRole.VIEWER: [
        Permission.READ_ALERTS, Permission.READ_ANALYTICS, Permission.READ_CONFIG
    ]
}

# Database Models (using the corrected 'metadata' column names)
class User(Base):
    __tablename__ = "users"
    
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    username = Column(String, unique=True, index=True)
    email = Column(String, unique=True, index=True)
    hashed_password = Column(String)
    role = Column(String, default=UserRole.VIEWER)
    is_active = Column(Boolean, default=True)
    tenant_id = Column(String, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    last_login = Column(DateTime)
    failed_login_attempts = Column(Integer, default=0)
    locked_until = Column(DateTime)
    password_changed_at = Column(DateTime, default=datetime.utcnow)
    oauth_provider = Column(String)
    oauth_id = Column(String)
    user_metadata = Column(JSON) # Corrected name

class APIKey(Base):
    __tablename__ = "api_keys"
    
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    key_hash = Column(String, unique=True, index=True)
    name = Column(String)
    user_id = Column(String, index=True)
    tenant_id = Column(String, index=True)
    permissions = Column(JSON)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    last_used = Column(DateTime)
    expires_at = Column(DateTime)
    rate_limit = Column(Integer, default=1000)  # requests per hour
    key_metadata = Column(JSON) # Corrected name

class RefreshToken(Base):
    __tablename__ = "refresh_tokens"
    
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    token_hash = Column(String, unique=True, index=True)
    user_id = Column(String, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    expires_at = Column(DateTime)
    is_revoked = Column(Boolean, default=False)
    tenant_id = Column(String, index=True)

class AuditLog(Base):
    __tablename__ = "audit_logs"
    
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id = Column(String, index=True)
    action = Column(String)
    resource = Column(String)
    ip_address = Column(String)
    user_agent = Column(String)
    timestamp = Column(DateTime, default=datetime.utcnow)
    tenant_id = Column(String, index=True)
    log_metadata = Column(JSON) # Corrected name
    success = Column(Boolean)

# Pydantic Models
class UserCreate(BaseModel):
    username: str = Field(..., min_length=3, max_length=50)
    email: str = Field(..., pattern=r'^[^@]+@[^@]+\.[^@]+$')
    password: str = Field(..., min_length=8)
    role: UserRole = UserRole.VIEWER
    tenant_id: str
    
    @validator('password')
    def validate_password(cls, v):
        if not re.search(r'[A-Z]', v):
            raise ValueError('Password must contain at least one uppercase letter')
        if not re.search(r'[a-z]', v):
            raise ValueError('Password must contain at least one lowercase letter')
        if not re.search(r'\d', v):
            raise ValueError('Password must contain at least one digit')
        if not re.search(r'[!@#$%^&*(),.?":{}|<>]', v):
            raise ValueError('Password must contain at least one special character')
        return v

class UserResponse(BaseModel):
    id: str
    username: str
    email: str
    role: UserRole
    is_active: bool
    tenant_id: str
    created_at: datetime
    last_login: Optional[datetime]

class Token(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int
    refresh_token: str

class TokenData(BaseModel):
    user_id: str
    username: str
    role: UserRole
    tenant_id: str
    permissions: List[Permission]
    exp: int

class APIKeyCreate(BaseModel):
    name: str
    permissions: List[Permission]
    expires_at: Optional[datetime] = None
    rate_limit: int = 1000

class APIKeyResponse(BaseModel):
    id: str
    name: str
    key: str  # Only returned on creation
    permissions: List[Permission]
    created_at: datetime
    expires_at: Optional[datetime]
    rate_limit: int

# Global redis_client variable will be initialized by AuthManager
_redis_client_instance: Optional[aioredis.Redis] = None


async def get_redis_connection_from_settings(settings: Settings) -> Optional[aioredis.Redis]:
    """Get a Redis connection from settings."""
    global _redis_client_instance # This global should ideally be managed by the AuthManager instance
    
    # If the instance is already created and connected, return it
    if _redis_client_instance is not None: # Changed from 'and await _redis_client_instance.ping()'
        try: # Added try-except to handle ping failing for already created instance
            await _redis_client_instance.ping()
            return _redis_client_instance
        except Exception:
            _redis_client_instance = None # Reset if ping fails
            logger.warning("AuthModule: Existing Redis connection is stale, attempting reconnection.")


    if settings.redis.enabled:
        # Construct Redis URL based on settings
        redis_password_part = f":{settings.redis.password}" if settings.redis.password else ""
        redis_url_str = f"redis://{redis_password_part}@{settings.redis.host}:{settings.redis.port}/{settings.redis.db}"
        try:
            _redis_client_instance = aioredis.from_url(
                redis_url_str,
                socket_timeout=settings.redis.socket_timeout,
                socket_connect_timeout=settings.redis.socket_connect_timeout
            )
            await _redis_client_instance.ping()
            logger.info("AuthModule: Connected to Redis successfully.")
            return _redis_client_instance
        except Exception as e:
            logger.warning(f"AuthModule: Could not connect to Redis at {redis_url_str}. Auth features requiring Redis might be limited. Error: {e}")
            _redis_client_instance = None # Ensure it's None if connection fails
            return None
    else:
        logger.info("AuthModule: Redis is disabled in settings. Running without Redis support for auth.")
        _redis_client_instance = None
        return None


# Authentication utilities
class AuthManager:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.pwd_context = pwd_context
        self.jwt_secret_key = self.settings.jwt.secret
        self.jwt_algorithm = self.settings.jwt.algorithm
        self.jwt_expiration_delta = timedelta(minutes=self.settings.jwt.access_token_expire_minutes)
        self.jwt_refresh_expiration_delta = timedelta(days=self.settings.jwt.refresh_token_expire_days)
        
        self.redis_client: Optional[aioredis.Redis] = None
        self.rate_limiter: Optional['RateLimiter'] = None # Forward reference
        self.token_blacklist: Optional['TokenBlacklist'] = None # Forward reference

        self.db_engine: Optional[Any] = None
        self.AsyncSessionLocal: Optional[sessionmaker] = None # Using AsyncSessionLocal

        self.user_manager: Optional['UserManager'] = None
        self.api_key_manager: Optional['APIKeyManager'] = None
        self.audit_logger: Optional['AuditLogger'] = None

    async def initialize(self):
        """Asynchronously initialize AuthManager dependencies."""
        logger.info("AuthManager: Initializing...")
        
        # Initialize Redis components
        self.redis_client = await get_redis_connection_from_settings(self.settings)
        if self.redis_client:
            self.rate_limiter = RateLimiter(self.redis_client)
            self.token_blacklist = TokenBlacklist(self.redis_client)
        else:
            logger.warning("AuthManager: Redis is not available. Rate limiting and token blacklisting will be non-functional.")

        # Initialize Database components
        db_url_for_auth = self.settings.api.database_url
        if not db_url_for_auth:
             logger.warning("AuthManager: No database_url found in API settings. User/APIKey management will be non-functional.")
        else:
            try:
                # Use create_async_engine for async SQLAlchemy
                self.db_engine = create_async_engine(db_url_for_auth, echo=False) # echo=True for SQL logs
                self.AsyncSessionLocal = sessionmaker(
                    autocommit=False, autoflush=False, bind=self.db_engine, class_=AsyncSession
                )
                
                # Create tables (requires async context for metadata.create_all)
                async with self.db_engine.begin() as conn:
                    await conn.run_sync(Base.metadata.create_all)

                logger.info(f"AuthManager: Database engine initialized for {db_url_for_auth}")

                # Pass async session factory to managers
                self.user_manager = UserManager(db_session_factory=self.AsyncSessionLocal)
                self.api_key_manager = APIKeyManager(db_session_factory=self.AsyncSessionLocal, settings=self.settings) # Pass settings
                self.audit_logger = AuditLogger(db_session_factory=self.AsyncSessionLocal)
            except Exception as e:
                logger.error(f"AuthManager: Failed to initialize database: {e}", exc_info=True)
                self.db_engine = None
                self.AsyncSessionLocal = None
                logger.warning("AuthManager: Database is not available. User/APIKey management will be non-functional.")

        logger.info("AuthManager: Initialization complete.")

    async def shutdown(self):
        """Gracefully shut down AuthManager dependencies."""
        logger.info("AuthManager: Shutting down...")
        if self.redis_client:
            await self.redis_client.close()
            logger.info("AuthManager: Redis client closed.")
        if self.db_engine:
            await self.db_engine.dispose() # Async dispose
            logger.info("AuthManager: Database engine disposed.")
        logger.info("AuthManager: Shutdown complete.")

    def verify_password(self, plain_password: str, hashed_password: str) -> bool:
        return self.pwd_context.verify(plain_password, hashed_password)
    
    def get_password_hash(self, password: str) -> str:
        return self.pwd_context.hash(password)
    
    def create_access_token(self, data: dict, expires_delta: Optional[timedelta] = None) -> str:
        to_encode = data.copy()
        if expires_delta:
            expire = datetime.utcnow() + expires_delta
        else:
            expire = datetime.utcnow() + self.jwt_expiration_delta
        
        to_encode.update({"exp": int(expire.timestamp()), "type": "access"}) # Use int timestamp for JWT exp
        encoded_jwt = jwt.encode(to_encode, self.jwt_secret_key, algorithm=self.jwt_algorithm)
        return encoded_jwt
    
    def create_refresh_token(self, user_id: str, tenant_id: str) -> str:
        data = {
            "user_id": user_id,
            "tenant_id": tenant_id,
            "type": "refresh",
            "exp": int((datetime.utcnow() + self.jwt_refresh_expiration_delta).timestamp()) # Use int timestamp for JWT exp
        }
        return jwt.encode(data, self.jwt_secret_key, algorithm=self.jwt_algorithm)
    
    def verify_token(self, token: str) -> Optional[TokenData]:
        try:
            payload = jwt.decode(token, self.jwt_secret_key, algorithms=[self.jwt_algorithm])
            
            if payload.get("type") != "access":
                logger.warning(f"Invalid token type: {payload.get('type')}")
                return None
            
            user_id = payload.get("user_id")
            username = payload.get("username")
            role = payload.get("role")
            tenant_id = payload.get("tenant_id")
            permissions = payload.get("permissions", [])
            exp = payload.get("exp")
            
            if not all([user_id, username, role, tenant_id, exp]):
                logger.warning("Missing essential fields in token payload.")
                return None
            
            # Check expiration manually if PyJWT is used and validation is not implicit
            # PyJWT decode can raise ExpiredSignatureError, but explicit check for robustness
            if datetime.fromtimestamp(exp) < datetime.utcnow():
                logger.warning("Token has expired.")
                return None

            return TokenData(
                user_id=user_id,
                username=username,
                role=UserRole(role),
                tenant_id=tenant_id,
                permissions=[Permission(p) for p in permissions],
                exp=exp
            )
        except jwt.ExpiredSignatureError:
            logger.warning("Token expired signature error.")
            return None
        except jwt.InvalidTokenError as e:
            logger.warning(f"Invalid JWT token: {e}")
            return None
        except ValidationError as e:
            logger.warning(f"TokenData validation failed: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error during token verification: {e}", exc_info=True)
            return None
    
    def generate_api_key(self) -> str:
        return f"{self.settings.api.api_key_prefix}{secrets.token_urlsafe(32)}"
    
    def hash_api_key(self, api_key: str) -> str:
        return hashlib.sha256(api_key.encode()).hexdigest()

# Rate limiting (updated to accept Redis client directly)
class RateLimiter:
    def __init__(self, redis_client: Optional[aioredis.Redis]):
        self.redis = redis_client
        if not self.redis:
            logger.warning("RateLimiter initialized without Redis client. Rate limiting will be non-functional.")
    
    async def is_allowed(self, key: str, limit: int, window: int = 3600) -> bool:
        """Check if request is allowed based on rate limit"""
        if not self.redis:
            return True # Fail open if Redis not available
        
        current_time = int(time.time())
        window_start = current_time - window
        
        try:
            async with self.redis.pipeline() as pipe:
                pipe.zremrangebyscore(key, 0, window_start)
                pipe.zcard(key)
                pipe.zadd(key, {str(current_time): current_time})
                pipe.expire(key, window)
                results = await pipe.execute()
            
            return results[1] < limit
        except Exception as e:
            logger.error(f"RateLimiter Redis operation failed for key {key}: {e}", exc_info=True)
            return True # Fail open

# Token blacklisting (updated to accept Redis client directly)
class TokenBlacklist:
    def __init__(self, redis_client: Optional[aioredis.Redis]):
        self.redis = redis_client
        if not self.redis:
            logger.warning("TokenBlacklist initialized without Redis client. Token blacklisting will be non-functional.")

    async def add_token(self, token: str, exp: int):
        """Add token to blacklist"""
        if not self.redis:
            return
        ttl = exp - int(time.time())
        if ttl > 0:
            try:
                await self.redis.setex(f"blacklist:{token}", ttl, "1")
            except Exception as e:
                logger.error(f"Failed to add token to blacklist: {e}", exc_info=True)
    
    async def is_blacklisted(self, token: str) -> bool:
        """Check if token is blacklisted"""
        if not self.redis:
            return False # Not blacklisted if service is down
        try:
            result = await self.redis.get(f"blacklist:{token}")
            return result is not None
        except Exception as e:
            logger.error(f"Failed to check token blacklist: {e}", exc_info=True)
            return False # Assume not blacklisted if service is down

# User management (updated to accept session factory for proper async handling)
class UserManager:
    def __init__(self, db_session_factory: Any): # SessionLocal or AsyncSessionLocal
        self.db_session_factory = db_session_factory

    async def _get_db(self) -> AsyncSession:
        """Helper to get an async session."""
        async with self.db_session_factory() as session:
            yield session

    async def create_user(self, user_data: UserCreate) -> User:
        async for db in self._get_db(): # Use async for
            hashed_password = pwd_context.hash(user_data.password)
            user = User(
                username=user_data.username,
                email=user_data.email,
                hashed_password=hashed_password,
                role=user_data.role,
                tenant_id=user_data.tenant_id
            )
            db.add(user)
            await db.commit()
            await db.refresh(user)
            return user
    
    async def get_user_by_username(self, username: str) -> Optional[User]:
        async for db in self._get_db():
            return await db.query(User).filter(User.username == username).first()
    
    async def get_user_by_email(self, email: str) -> Optional[User]:
        async for db in self._get_db():
            return await db.query(User).filter(User.email == email).first()
    
    async def get_user_by_id(self, user_id: str) -> Optional[User]:
        async for db in self._get_db():
            return await db.query(User).filter(User.id == user_id).first()
    
    async def authenticate_user(self, username: str, password: str) -> Optional[User]:
        async for db in self._get_db():
            user = await self.get_user_by_username(username) # Call async method
            if not user or not user.is_active:
                return None
            
            if user.locked_until and user.locked_until > datetime.utcnow():
                return None
            
            if not pwd_context.verify(password, user.hashed_password):
                user.failed_login_attempts += 1
                if user.failed_login_attempts >= 5:
                    user.locked_until = datetime.utcnow() + timedelta(minutes=30)
                await db.commit()
                return None
            
            user.failed_login_attempts = 0
            user.locked_until = None
            user.last_login = datetime.utcnow()
            await db.commit()
            
            return user
    
    async def update_user_role(self, user_id: str, new_role: UserRole):
        async for db in self._get_db():
            user = await self.get_user_by_id(user_id) # Call async method
            if user:
                user.role = new_role
                await db.commit()
    
    async def deactivate_user(self, user_id: str):
        async for db in self._get_db():
            user = await self.get_user_by_id(user_id) # Call async method
            if user:
                user.is_active = False
                await db.commit()

# API Key management (updated to accept session factory and settings)
class APIKeyManager:
    def __init__(self, db_session_factory: Any, settings: Settings):
        self.db_session_factory = db_session_factory
        self.settings = settings
    
    async def _get_db(self) -> AsyncSession:
        """Helper to get an async session."""
        async with self.db_session_factory() as session:
            yield session

    async def create_api_key(self, user_id: str, tenant_id: str, key_data: APIKeyCreate) -> tuple[APIKey, str]:
        async for db in self._get_db():
            raw_api_key = secrets.token_urlsafe(32)
            key_with_prefix = f"{self.settings.api.api_key_prefix}{raw_api_key}" # Use prefix from settings
            key_hash = hashlib.sha256(key_with_prefix.encode()).hexdigest() # Hash the prefixed key
            
            db_key = APIKey(
                key_hash=key_hash,
                name=key_data.name,
                user_id=user_id,
                tenant_id=tenant_id,
                permissions=[p.value for p in key_data.permissions],
                expires_at=key_data.expires_at,
                rate_limit=key_data.rate_limit
            )
            db.add(db_key)
            await db.commit()
            await db.refresh(db_key)
            
            return db_key, key_with_prefix

    async def get_api_key_by_hash(self, key_hash: str) -> Optional[APIKey]:
        async for db in self._get_db():
            return await db.query(APIKey).filter(APIKey.key_hash == key_hash).first()
    
    async def authenticate_api_key(self, api_key_str: str) -> Optional[APIKey]:
        key_hash = hashlib.sha256(api_key_str.encode()).hexdigest()
        db_key = await self.get_api_key_by_hash(key_hash)
        
        if not db_key or not db_key.is_active:
            return None
        
        if db_key.expires_at and db_key.expires_at < datetime.utcnow():
            return None
        
        db_key.last_used = datetime.utcnow()
        async for db in self._get_db(): # Commit in context
            await db.commit()
        
        return db_key
    
    async def revoke_api_key(self, key_id: str):
        async for db in self._get_db():
            key = await db.query(APIKey).filter(APIKey.id == key_id).first()
            if key:
                key.is_active = False
                await db.commit()

# Audit logging (updated to accept session factory)
class AuditLogger:
    def __init__(self, db_session_factory: Any):
        self.db_session_factory = db_session_factory
    
    async def _get_db(self) -> AsyncSession:
        """Helper to get an async session."""
        async with self.db_session_factory() as session:
            yield session

    async def log_event(self, user_id: str, action: str, resource: str, 
                        ip_address: str, user_agent: str, tenant_id: str, 
                        success: bool = True, metadata: dict = None):
        async for db in self._get_db():
            log_entry = AuditLog(
                user_id=user_id,
                action=action,
                resource=resource,
                ip_address=ip_address,
                user_agent=user_agent,
                tenant_id=tenant_id,
                success=success,
                log_metadata=metadata or {}
            )
            db.add(log_entry)
            await db.commit()


# Global instance of AuthManager, initialized in main.py's startup sequence
_auth_manager_instance: Optional[AuthManager] = None

def get_auth_manager_instance() -> AuthManager:
    """Provides the singleton AuthManager instance."""
    if _auth_manager_instance is None:
        logger.error("AuthManager instance not initialized globally!")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Authentication system not initialized.")
    return _auth_manager_instance

# Authentication dependencies (now take `AuthManager` instance as a direct dependency)
# These helper functions simplify the FastAPI `Depends` usage
async def get_current_user(
    auth_manager_instance: AuthManager = Depends(get_auth_manager_instance),
    credentials: HTTPAuthorizationCredentials = Security(security) # No auto_error here, it's on HTTPBearer()
) -> User:
    """Get current user from JWT token"""
    if not auth_manager_instance.token_blacklist:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Token blacklisting service unavailable.")

    if await auth_manager_instance.token_blacklist.is_blacklisted(credentials.credentials):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token has been revoked")
    
    token_data = auth_manager_instance.verify_token(credentials.credentials)
    if not token_data:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")
    
    if not auth_manager_instance.user_manager:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="User management service unavailable.")

    user = await auth_manager_instance.user_manager.get_user_by_id(token_data.user_id)
    if not user or not user.is_active:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found or inactive")
    
    return user

async def get_current_user_from_api_key(
    auth_manager_instance: AuthManager = Depends(get_auth_manager_instance),
    api_key_str: str = Security(api_key_header) # No auto_error here, it's on APIKeyHeader()
) -> Optional[User]:
    """Get current user from API key"""
    if not api_key_str:
        return None
    
    if not auth_manager_instance.api_key_manager:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="API Key management service unavailable.")
    
    db_key = await auth_manager_instance.api_key_manager.authenticate_api_key(api_key_str)
    
    if not db_key:
        return None
    
    if not auth_manager_instance.rate_limiter:
        logger.warning("Rate limiter service unavailable for API key, bypassing rate limit check.")
    elif not await auth_manager_instance.rate_limiter.is_allowed(f"api_key:{db_key.id}", db_key.rate_limit):
        raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail="Rate limit exceeded")
    
    if not auth_manager_instance.user_manager:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="User management service unavailable.")

    user = await auth_manager_instance.user_manager.get_user_by_id(db_key.user_id)
    
    if user:
        user.api_key_permissions = [Permission(p) for p in db_key.permissions]
    
    return user

async def get_current_user_flexible(
    # These security schemes are instantiated with auto_error=False where needed
    credentials: Optional[HTTPAuthorizationCredentials] = Security(HTTPBearer(auto_error=False)),
    api_key_str: Optional[str] = Security(APIKeyHeader(name="X-API-Key", auto_error=False)),
    auth_manager_instance: AuthManager = Depends(get_auth_manager_instance)
) -> User:
    """Get current user from either JWT token or API key"""
    user = None
    
    if api_key_str:
        user = await get_current_user_from_api_key(auth_manager_instance=auth_manager_instance, api_key_str=api_key_str)
    
    if not user and credentials:
        user = await get_current_user(auth_manager_instance=auth_manager_instance, credentials=credentials)
    
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authentication required")
    
    return user

# Permission checking
def require_permission(permission: Permission):
    """Decorator to require specific permission"""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            current_user: Optional[User] = None
            for arg in args:
                if isinstance(arg, User):
                    current_user = arg
                    break
            
            if not current_user:
                if 'current_user' in kwargs and isinstance(kwargs['current_user'], User):
                    current_user = kwargs['current_user']

            if not current_user:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Permission denied: User object not found in request context."
                )
            
            user_permissions = set(ROLE_PERMISSIONS.get(UserRole(current_user.role), []))
            
            if hasattr(current_user, 'api_key_permissions') and current_user.api_key_permissions:
                user_permissions.update(current_user.api_key_permissions) # Combine permissions
            
            if permission not in user_permissions:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=f"Permission {permission.value} required"
                )
            
            return await func(*args, **kwargs)
        return wrapper
    return decorator

# Multi-tenant support
def require_tenant_access(tenant_id_param: str, user: User):
    """Check if user has access to specific tenant"""
    if user.role == UserRole.ADMIN:
        return True
    
    if user.tenant_id != tenant_id_param:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access denied to this tenant"
        )
    
    return True

# Security headers middleware
async def add_security_headers(request: Request, call_next):
    response = await call_next(request)
    
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    response.headers["Content-Security-Policy"] = "default-src 'self'"
    
    return response

# Initialize database (called once at startup by main.py's AuthManager)
def init_db(engine: Any): # Accepts engine now
    # This function is now mostly redundant as AuthManager's initialize() handles this with asyncpg
    # but keeping it for backward compatibility or if there's another path that calls it.
    Base.metadata.create_all(bind=engine)
    logger.info("Database tables ensured by sync init_db (consider using async for primary init).")


# Testing utilities
class AuthTestUtils:
    @staticmethod
    async def create_test_user(db_session: AsyncSession, username: str = "testuser", 
                        role: UserRole = UserRole.VIEWER, 
                        tenant_id: str = "test_tenant") -> User:
        """Create a test user for testing purposes"""
        user_manager_for_test = UserManager(db_session_factory=lambda: db_session)
        user_data = UserCreate(
            username=username,
            email=f"{username}@example.com",
            password="TestPass123!",
            role=role,
            tenant_id=tenant_id
        )
        return await user_manager_for_test.create_user(user_data)

    @staticmethod
    async def create_test_token(user: User, auth_manager_instance: 'AuthManager') -> str:
        """Create a test JWT token"""
        data = {
            "user_id": user.id,
            "username": user.username,
            "role": user.role.value,
            "tenant_id": user.tenant_id,
            "permissions": [p.value for p in ROLE_PERMISSIONS.get(UserRole(user.role), [])]
        }
        return auth_manager_instance.create_access_token(data)
    
    @staticmethod
    async def create_test_api_key(db_session: AsyncSession, user_id: str, 
                           permissions: Optional[List[Permission]] = None, settings: Settings = Depends(get_settings)) -> tuple[APIKey, str]:
        """Create a test API key"""
        if permissions is None:
            permissions = [Permission.READ_ALERTS]
        
        key_data = APIKeyCreate(
            name="Test Key",
            permissions=permissions
        )
        api_key_manager_for_test = APIKeyManager(db_session_factory=lambda: db_session, settings=settings)
        return await api_key_manager_for_test.create_api_key(user_id, "test_tenant", key_data)


# Export main components
__all__ = [
    'AuthManager', 'UserManager', 'APIKeyManager', 'AuditLogger',
    'get_current_user', # Simplified dependency name for use in API routes
    'get_current_user_from_api_key', # Simplified dependency name
    'get_current_user_flexible', # Simplified dependency name
    'get_auth_manager_instance', # Function to get the initialized instance
    'require_permission', 'require_tenant_access', 'add_security_headers',
    'UserRole', 'Permission', 'UserCreate', 'UserResponse', 'Token', 'APIKeyCreate',
    'APIKeyResponse', 'init_db', 'AuthTestUtils'
]