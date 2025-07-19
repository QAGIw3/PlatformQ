"""
Auth Service with Vault & Consul Integration
"""

from fastapi import FastAPI, Depends, HTTPException, Header, Request
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from contextlib import asynccontextmanager
import asyncio
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
import logging
import os

from platformq_shared.vault.vault_client import VaultClient
from platformq_shared.consul.consul_client import ConsulClient
from platformq_shared.middleware.security_middleware import SecurityMiddleware

from .vault_consul_integration import (
    AuthServiceVaultIntegration,
    AuthServiceConsulIntegration,
    AuthConfig
)
from .models import UserCreate, UserLogin, TokenResponse, User
from .database import get_db, Database
from .utils import verify_password, get_password_hash

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# OAuth2 scheme
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


class AuthService:
    """Enhanced Auth Service with Vault & Consul Integration"""
    
    def __init__(self):
        self.app = FastAPI(title="PlatformQ Auth Service", version="2.0.0")
        self.vault_integration: Optional[AuthServiceVaultIntegration] = None
        self.consul_integration: Optional[AuthServiceConsulIntegration] = None
        self.config: Optional[AuthConfig] = None
        
    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        """Application lifespan management"""
        # Startup
        await self.startup()
        yield
        # Shutdown
        await self.shutdown()
        
    async def startup(self):
        """Service startup procedure"""
        logger.info("Starting Auth Service with Vault & Consul integration")
        
        try:
            # Initialize Vault client
            vault_client = VaultClient(
                vault_addr=os.getenv("VAULT_ADDR", "http://vault:8200"),
                role_id=os.getenv("VAULT_ROLE_ID"),
                secret_id=os.getenv("VAULT_SECRET_ID")
            )
            await vault_client.initialize()
            
            # Initialize Consul client
            consul_client = ConsulClient(
                host=os.getenv("CONSUL_HOST", "consul"),
                port=int(os.getenv("CONSUL_PORT", "8500"))
            )
            
            # Initialize integrations
            self.vault_integration = AuthServiceVaultIntegration(vault_client)
            await self.vault_integration.initialize()
            
            self.consul_integration = AuthServiceConsulIntegration(consul_client)
            await self.consul_integration.initialize()
            
            # Load initial configuration
            self.config = await self.consul_integration.get_config()
            
            # Set up routes
            self._setup_routes()
            
            # Add security middleware
            security_middleware = SecurityMiddleware(
                vault_client=vault_client,
                consul_client=consul_client,
                service_name="auth-service"
            )
            self.app.add_middleware(security_middleware)
            
            # Start background tasks
            asyncio.create_task(self._health_check_loop())
            asyncio.create_task(self._config_refresh_loop())
            asyncio.create_task(self._key_rotation_monitor())
            
            logger.info("Auth Service started successfully")
            
        except Exception as e:
            logger.error(f"Failed to start Auth Service: {e}")
            raise
            
    async def shutdown(self):
        """Service shutdown procedure"""
        logger.info("Shutting down Auth Service")
        
        # Cancel background tasks
        for task in asyncio.all_tasks():
            if task.get_name() in ["health_check", "config_refresh", "key_rotation"]:
                task.cancel()
                
        # Deregister from Consul
        if self.consul_integration:
            await self.consul_integration.consul.deregister_service()
            
        logger.info("Auth Service shutdown complete")
        
    def _setup_routes(self):
        """Set up API routes"""
        
        @self.app.get("/health")
        async def health_check():
            """Health check endpoint"""
            try:
                # Check Vault connectivity
                vault_healthy = await self._check_vault_health()
                
                # Check Consul connectivity
                consul_healthy = await self._check_consul_health()
                
                # Check database
                db_healthy = await self._check_database_health()
                
                overall_status = "healthy"
                if not all([vault_healthy, consul_healthy, db_healthy]):
                    overall_status = "degraded" if any([vault_healthy, consul_healthy, db_healthy]) else "unhealthy"
                    
                health_data = {
                    "status": overall_status,
                    "service": "auth-service",
                    "checks": {
                        "vault": "healthy" if vault_healthy else "unhealthy",
                        "consul": "healthy" if consul_healthy else "unhealthy",
                        "database": "healthy" if db_healthy else "unhealthy"
                    },
                    "timestamp": datetime.utcnow().isoformat()
                }
                
                # Update Consul health status
                await self.consul_integration.update_health_status(
                    "overall",
                    "passing" if overall_status == "healthy" else "critical",
                    str(health_data)
                )
                
                if overall_status == "unhealthy":
                    raise HTTPException(status_code=503, detail=health_data)
                    
                return health_data
                
            except Exception as e:
                logger.error(f"Health check failed: {e}")
                raise HTTPException(status_code=503, detail="Service unhealthy")
                
        @self.app.post("/api/v1/register", response_model=User)
        async def register(
            user_data: UserCreate,
            request: Request,
            db: Database = Depends(get_db)
        ):
            """Register new user"""
            # Check rate limit
            client_ip = request.client.host
            if not await self.consul_integration.check_rate_limit(
                f"register:{client_ip}",
                self.config.api_request_limit
            ):
                raise HTTPException(429, "Too many registration attempts")
                
            # Validate password against policy
            if not self._validate_password_policy(user_data.password):
                raise HTTPException(
                    400,
                    f"Password does not meet requirements: minimum {self.config.password_min_length} characters"
                )
                
            # Check if user exists
            existing_user = await db.get_user_by_email(user_data.email)
            if existing_user:
                raise HTTPException(400, "Email already registered")
                
            # Get password pepper from Vault
            pepper = await self.vault_integration.get_password_pepper()
            
            # Hash password with pepper
            hashed_password = get_password_hash(user_data.password + pepper)
            
            # Encrypt PII data
            encrypted_email = await self.vault_integration.encrypt_pii(user_data.email)
            
            # Create user
            user = await db.create_user(
                username=user_data.username,
                email=user_data.email,
                encrypted_email=encrypted_email,
                hashed_password=hashed_password
            )
            
            logger.info(f"User registered: {user.username}")
            return user
            
        @self.app.post("/api/v1/login", response_model=TokenResponse)
        async def login(
            form_data: OAuth2PasswordRequestForm = Depends(),
            request: Request = None,
            db: Database = Depends(get_db)
        ):
            """User login"""
            # Check rate limit
            if not await self.consul_integration.check_rate_limit(
                f"login:{form_data.username}",
                self.config.login_attempts_limit
            ):
                raise HTTPException(429, "Too many login attempts")
                
            # Check if passwordless is enabled for user
            if await self.consul_integration.get_feature_flag("passwordless", form_data.username):
                # Handle passwordless login
                return await self._handle_passwordless_login(form_data.username, db)
                
            # Get user
            user = await db.get_user_by_username(form_data.username)
            if not user:
                raise HTTPException(401, "Invalid credentials")
                
            # Get password pepper
            pepper = await self.vault_integration.get_password_pepper()
            
            # Verify password
            if not verify_password(form_data.password + pepper, user.hashed_password):
                raise HTTPException(401, "Invalid credentials")
                
            # Check concurrent sessions
            active_sessions = await self.consul_integration.count_active_sessions(user.id)
            if active_sessions >= self.config.max_concurrent_sessions:
                raise HTTPException(400, f"Maximum {self.config.max_concurrent_sessions} concurrent sessions reached")
                
            # Generate tokens
            access_token = await self._create_access_token(user)
            refresh_token = await self._create_refresh_token(user)
            
            # Register session
            session_id = f"session-{user.id}-{datetime.utcnow().timestamp()}"
            await self.consul_integration.register_active_session(
                user.id,
                session_id,
                self.config.session_timeout_minutes
            )
            
            return TokenResponse(
                access_token=access_token,
                refresh_token=refresh_token,
                token_type="bearer",
                expires_in=self.config.session_timeout_minutes * 60
            )
            
        @self.app.post("/api/v1/logout")
        async def logout(
            token: str = Depends(oauth2_scheme),
            db: Database = Depends(get_db)
        ):
            """User logout"""
            # Verify token
            payload = await self.vault_integration.verify_jwt_with_rotation(token)
            user_id = payload.get("sub")
            session_id = payload.get("session_id")
            
            if session_id:
                # Remove session
                await self.consul_integration.remove_active_session(user_id, session_id)
                
            return {"message": "Logged out successfully"}
            
        @self.app.get("/api/v1/oauth/{provider}/login")
        async def oauth_login(provider: str):
            """OAuth login redirect"""
            # Check if OAuth is enabled
            if not self.config.oauth_enabled:
                raise HTTPException(400, "OAuth login is disabled")
                
            # Get OAuth credentials from Vault
            try:
                oauth_creds = await self.vault_integration.get_oauth_credentials(provider)
            except Exception as e:
                logger.error(f"Failed to get OAuth credentials for {provider}: {e}")
                raise HTTPException(400, f"OAuth provider {provider} not configured")
                
            # Build OAuth URL
            oauth_url = self._build_oauth_url(provider, oauth_creds)
            
            return {"redirect_url": oauth_url}
            
        @self.app.post("/api/v1/oauth/{provider}/callback")
        async def oauth_callback(
            provider: str,
            code: str,
            state: str,
            db: Database = Depends(get_db)
        ):
            """OAuth callback handler"""
            # Verify state parameter
            if not await self._verify_oauth_state(state):
                raise HTTPException(400, "Invalid OAuth state")
                
            # Exchange code for tokens
            oauth_creds = await self.vault_integration.get_oauth_credentials(provider)
            user_info = await self._exchange_oauth_code(provider, code, oauth_creds)
            
            # Create or update user
            user = await self._handle_oauth_user(user_info, provider, db)
            
            # Generate tokens
            access_token = await self._create_access_token(user)
            refresh_token = await self._create_refresh_token(user)
            
            return TokenResponse(
                access_token=access_token,
                refresh_token=refresh_token,
                token_type="bearer"
            )
            
        @self.app.get("/api/v1/me", response_model=User)
        async def get_current_user(
            token: str = Depends(oauth2_scheme),
            db: Database = Depends(get_db)
        ):
            """Get current user info"""
            # Verify token
            payload = await self.vault_integration.verify_jwt_with_rotation(token)
            user_id = payload.get("sub")
            
            user = await db.get_user_by_id(user_id)
            if not user:
                raise HTTPException(401, "User not found")
                
            return user
            
    async def _create_access_token(self, user: User) -> str:
        """Create JWT access token"""
        # Get signing key from Vault
        signing_key = await self.vault_integration.get_jwt_signing_key("access")
        
        # Create token payload
        payload = {
            "sub": str(user.id),
            "username": user.username,
            "email": user.email,
            "exp": datetime.utcnow() + timedelta(minutes=15),
            "iat": datetime.utcnow(),
            "type": "access",
            "session_id": f"session-{user.id}-{datetime.utcnow().timestamp()}"
        }
        
        # Sign token
        from jose import jwt
        return jwt.encode(payload, signing_key, algorithm="HS256")
        
    async def _create_refresh_token(self, user: User) -> str:
        """Create JWT refresh token"""
        # Get signing key from Vault
        signing_key = await self.vault_integration.get_jwt_signing_key("refresh")
        
        # Create token payload
        payload = {
            "sub": str(user.id),
            "exp": datetime.utcnow() + timedelta(days=30),
            "iat": datetime.utcnow(),
            "type": "refresh"
        }
        
        # Sign token
        from jose import jwt
        return jwt.encode(payload, signing_key, algorithm="HS256")
        
    def _validate_password_policy(self, password: str) -> bool:
        """Validate password against policy"""
        if len(password) < self.config.password_min_length:
            return False
            
        if self.config.password_require_uppercase and not any(c.isupper() for c in password):
            return False
            
        if self.config.password_require_numbers and not any(c.isdigit() for c in password):
            return False
            
        if self.config.password_require_special and not any(c in "!@#$%^&*" for c in password):
            return False
            
        return True
        
    async def _check_vault_health(self) -> bool:
        """Check Vault connectivity"""
        try:
            # Try to read a health check secret
            await self.vault_integration.vault.get_secret("auth-service/health-check")
            return True
        except:
            return False
            
    async def _check_consul_health(self) -> bool:
        """Check Consul connectivity"""
        try:
            # Try to read a health check key
            await self.consul_integration.consul.kv_get("services/auth-service/health/status")
            return True
        except:
            return False
            
    async def _check_database_health(self) -> bool:
        """Check database connectivity"""
        try:
            db = await get_db()
            await db.execute("SELECT 1")
            return True
        except:
            return False
            
    async def _health_check_loop(self):
        """Periodic health check"""
        while True:
            try:
                await asyncio.sleep(30)  # Every 30 seconds
                
                # Run health checks
                vault_healthy = await self._check_vault_health()
                consul_healthy = await self._check_consul_health()
                db_healthy = await self._check_database_health()
                
                # Update individual health statuses
                await self.consul_integration.update_health_status(
                    "vault",
                    "passing" if vault_healthy else "critical"
                )
                await self.consul_integration.update_health_status(
                    "consul",
                    "passing" if consul_healthy else "critical"
                )
                await self.consul_integration.update_health_status(
                    "database",
                    "passing" if db_healthy else "critical"
                )
                
            except Exception as e:
                logger.error(f"Health check loop error: {e}")
                
    async def _config_refresh_loop(self):
        """Periodic configuration refresh"""
        while True:
            try:
                await asyncio.sleep(300)  # Every 5 minutes
                self.config = await self.consul_integration.reload_config()
                logger.info("Configuration refreshed from Consul")
            except Exception as e:
                logger.error(f"Config refresh loop error: {e}")
                
    async def _key_rotation_monitor(self):
        """Monitor for upcoming key rotations"""
        while True:
            try:
                await asyncio.sleep(86400)  # Daily check
                
                # Check JWT key age
                for key_type in ["access", "refresh", "id"]:
                    key_path = f"auth-service/jwt/{key_type}-token-key"
                    secret = await self.vault_integration.vault.get_secret(key_path)
                    
                    if "rotated_at" in secret:
                        rotated_at = datetime.fromisoformat(secret["rotated_at"])
                        age_days = (datetime.utcnow() - rotated_at).days
                        
                        if age_days > 150:  # Alert 30 days before rotation
                            logger.warning(f"JWT {key_type} key is {age_days} days old, rotation due soon")
                            
            except Exception as e:
                logger.error(f"Key rotation monitor error: {e}")


# Create app instance
auth_service = AuthService()
app = auth_service.app

# Set up lifespan
app.router.lifespan_context = auth_service.lifespan

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
