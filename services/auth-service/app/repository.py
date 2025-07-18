"""
Enhanced Repository Classes for Auth Service

Uses the new repository patterns from platformq-shared.
"""

import datetime
import secrets
from datetime import timedelta
from uuid import UUID, uuid4
from typing import Optional, List, Tuple, Dict, Any

from cassandra.cluster import Session
from cassandra.query import SimpleStatement, ConsistencyLevel

from platformq_shared import CassandraRepository
from .schemas.user import UserCreate, UserUpdate
from .schemas.api_key import ApiKey, ApiKeyCreate
from .schemas.invitation import InvitationCreate
from .schemas.role import RoleAssignmentRequest
from .schemas.subscription import Subscription, SubscriptionCreate, SubscriptionUpdate
from .schemas.tenant import Tenant, TenantCreate
from .core.security import get_password_hash


class UserRepository(CassandraRepository[UserUpdate]):
    """Enhanced user repository with query capabilities"""
    
    def __init__(self, db_session: Session):
        super().__init__(db_session, UserUpdate, "auth_keyspace")
        self.table_name = "users"
    
    def get_by_email(self, email: str) -> Optional[UserUpdate]:
        """Get user by email address"""
        query = "SELECT * FROM users WHERE email = %s ALLOW FILTERING"
        prepared = self.session.prepare(query)
        prepared.consistency_level = ConsistencyLevel.QUORUM
        
        rows = self.session.execute(prepared, [email])
        row = rows.one()
        return UserUpdate(**row) if row else None

    def get_by_wallet_address(self, wallet_address: str) -> Optional[UserUpdate]:
        """Get user by blockchain wallet address"""
        query = "SELECT * FROM users WHERE wallet_address = %s ALLOW FILTERING"
        prepared = self.session.prepare(query)
        prepared.consistency_level = ConsistencyLevel.QUORUM
        
        rows = self.session.execute(prepared, [wallet_address])
        row = rows.one()
        return UserUpdate(**row) if row else None
        
    def get_by_tenant(self, tenant_id: UUID, limit: int = 100) -> List[UserUpdate]:
        """Get all users for a tenant"""
        query = "SELECT * FROM users WHERE tenant_id = %s LIMIT %s"
        prepared = self.session.prepare(query)
        
        rows = self.session.execute(prepared, [tenant_id, limit])
        return [UserUpdate(**row) for row in rows]
        
    def search_by_name(self, name_pattern: str, tenant_id: Optional[UUID] = None) -> List[UserUpdate]:
        """Search users by name pattern"""
        if tenant_id:
            query = "SELECT * FROM users WHERE tenant_id = %s AND full_name LIKE %s ALLOW FILTERING"
            params = [tenant_id, f"%{name_pattern}%"]
        else:
            query = "SELECT * FROM users WHERE full_name LIKE %s ALLOW FILTERING"
            params = [f"%{name_pattern}%"]
            
        prepared = self.session.prepare(query)
        rows = self.session.execute(prepared, params)
        return [UserUpdate(**row) for row in rows]

    def add(self, user: UserCreate, tenant_id: UUID) -> UserUpdate:
        """Create a new user"""
        user_id = uuid4()
        created_time = datetime.datetime.now(datetime.timezone.utc)

        query = """
        INSERT INTO users (
            tenant_id, id, email, full_name, status, created_at, updated_at, 
            did, wallet_address, storage_backend, storage_config
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        prepared = self.session.prepare(query)
        prepared.consistency_level = ConsistencyLevel.QUORUM
        
        self.session.execute(
            prepared,
            [
                tenant_id, user_id, user.email, user.full_name, "active", 
                created_time, created_time, user.did, user.wallet_address,
                user.storage_backend, user.storage_config
            ],
        )
        return self.get(user_id=user_id)
    
    def get(self, user_id: UUID) -> Optional[UserUpdate]:
        """Get user by ID"""
        query = "SELECT * FROM users WHERE id = %s ALLOW FILTERING"
        prepared = self.session.prepare(query)
        prepared.consistency_level = ConsistencyLevel.ONE
        
        rows = self.session.execute(prepared, [user_id])
        row = rows.one()
        return UserUpdate(**row) if row else None
        
    def list(self, skip: int = 0, limit: int = 100) -> List[UserUpdate]:
        """List all users with pagination"""
        # Note: Cassandra doesn't support OFFSET, so we use token-based pagination in production
        query = "SELECT * FROM users LIMIT %s"
        rows = self.session.execute(query, [limit])
        return [UserUpdate(**row) for row in rows]

    def update(self, user_id: UUID, user_update: UserUpdate) -> Optional[UserUpdate]:
        """Update user information"""
        user = self.get(user_id)
        if not user:
            return None
        
        # Build dynamic update query based on provided fields
        update_fields = []
        params = []
        
        if user_update.full_name is not None:
            update_fields.append("full_name = %s")
            params.append(user_update.full_name)
            
        if user_update.did is not None:
            update_fields.append("did = %s")
            params.append(user_update.did)
            
        if user_update.wallet_address is not None:
            update_fields.append("wallet_address = %s")
            params.append(user_update.wallet_address)
            
        if user_update.storage_backend is not None:
            update_fields.append("storage_backend = %s")
            params.append(user_update.storage_backend)
            
        if user_update.storage_config is not None:
            update_fields.append("storage_config = %s")
            params.append(user_update.storage_config)
            
        # Always update timestamp
        update_fields.append("updated_at = %s")
        params.append(datetime.datetime.now(datetime.timezone.utc))
        
        # Add WHERE clause parameters
        params.extend([user_id, user.tenant_id])
        
        query = f"UPDATE users SET {', '.join(update_fields)} WHERE id = %s AND tenant_id = %s"
        prepared = self.session.prepare(query)
        prepared.consistency_level = ConsistencyLevel.QUORUM
        
        self.session.execute(prepared, params)
        return self.get(user_id)
        
    def delete(self, user_id: UUID):
        """Soft delete user by anonymizing data"""
        user = self.get(user_id)
        if not user:
            return

        anonymized_email = f"deleted_{user_id}@platformq.com"
        anonymized_name = "Deleted User"

        query = """
        UPDATE users 
        SET email = %s, full_name = %s, status = 'deleted', updated_at = %s 
        WHERE id = %s AND tenant_id = %s
        """
        
        prepared = self.session.prepare(query)
        prepared.consistency_level = ConsistencyLevel.QUORUM
        
        self.session.execute(
            prepared,
            [
                anonymized_email,
                anonymized_name,
                datetime.datetime.now(datetime.timezone.utc),
                user_id,
                user.tenant_id
            ],
        )
        
    def bulk_create(self, users: List[Tuple[UserCreate, UUID]]) -> List[UserUpdate]:
        """Bulk create users"""
        created_users = []
        batch_query = "BEGIN BATCH "
        
        for user, tenant_id in users:
            user_id = uuid4()
            created_time = datetime.datetime.now(datetime.timezone.utc)
            
            batch_query += f"""
            INSERT INTO users (
                tenant_id, id, email, full_name, status, created_at, updated_at,
                did, wallet_address
            ) VALUES (
                {tenant_id}, {user_id}, '{user.email}', '{user.full_name}', 
                'active', '{created_time}', '{created_time}', 
                '{user.did or ''}', '{user.wallet_address or ''}'
            );
            """
            
            created_users.append(user_id)
            
        batch_query += " APPLY BATCH;"
        
        self.session.execute(batch_query)
        
        # Return created users
        return [self.get(uid) for uid in created_users if self.get(uid)]
        
    def count_by_tenant(self, tenant_id: UUID) -> int:
        """Count users in a tenant"""
        query = "SELECT COUNT(*) FROM users WHERE tenant_id = %s"
        prepared = self.session.prepare(query)
        
        result = self.session.execute(prepared, [tenant_id])
        return result.one()[0]
        
    def get_active_users(self, since: datetime.datetime, limit: int = 100) -> List[UserUpdate]:
        """Get recently active users"""
        query = """
        SELECT * FROM users 
        WHERE status = 'active' AND updated_at >= %s 
        ALLOW FILTERING 
        LIMIT %s
        """
        
        prepared = self.session.prepare(query)
        rows = self.session.execute(prepared, [since, limit])
        
        return [UserUpdate(**row) for row in rows]


class APIKeyRepository(CassandraRepository[ApiKey]):
    """Enhanced API key repository"""
    
    def __init__(self, db_session: Session):
        super().__init__(db_session, ApiKey, "auth_keyspace")
        self.table_name = "api_keys"
    
    def create(self, user_id: UUID, name: str, tenant_id: UUID) -> ApiKey:
        """Create a new API key"""
        key_id = uuid4()
        key_secret = secrets.token_urlsafe(32)
        hashed_key = get_password_hash(key_secret)
        created_at = datetime.datetime.now(datetime.timezone.utc)
        
        query = """
        INSERT INTO api_keys (
            id, user_id, tenant_id, name, hashed_key, created_at, last_used_at, active
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        prepared = self.session.prepare(query)
        prepared.consistency_level = ConsistencyLevel.QUORUM
        
        self.session.execute(
            prepared,
            [key_id, user_id, tenant_id, name, hashed_key, created_at, None, True]
        )
        
        # Return the key with the unhashed secret (only time it's visible)
        return ApiKey(
            id=key_id,
            user_id=user_id,
            tenant_id=tenant_id,
            name=name,
            key=f"sk_{key_id}_{key_secret}",  # Prefix for easy identification
            created_at=created_at,
            last_used_at=None,
            active=True
        )
        
    def get_by_key_id(self, key_id: UUID) -> Optional[ApiKey]:
        """Get API key by ID (returns hashed version)"""
        query = "SELECT * FROM api_keys WHERE id = %s"
        prepared = self.session.prepare(query)
        
        rows = self.session.execute(prepared, [key_id])
        row = rows.one()
        
        if row:
            # Don't return the hashed key
            return ApiKey(
                id=row.id,
                user_id=row.user_id,
                tenant_id=row.tenant_id,
                name=row.name,
                key="[REDACTED]",
                created_at=row.created_at,
                last_used_at=row.last_used_at,
                active=row.active
            )
        return None
        
    def validate_key(self, key_string: str, hashed_key: str) -> bool:
        """Validate an API key against its hash"""
        # In production, use proper password hashing verification
        return get_password_hash(key_string) == hashed_key
        
    def update_last_used(self, key_id: UUID):
        """Update last used timestamp"""
        query = "UPDATE api_keys SET last_used_at = %s WHERE id = %s"
        prepared = self.session.prepare(query)
        
        self.session.execute(
            prepared,
            [datetime.datetime.now(datetime.timezone.utc), key_id]
        )
        
    def list_by_user(self, user_id: UUID, include_inactive: bool = False) -> List[ApiKey]:
        """List API keys for a user"""
        if include_inactive:
            query = "SELECT * FROM api_keys WHERE user_id = %s ALLOW FILTERING"
        else:
            query = "SELECT * FROM api_keys WHERE user_id = %s AND active = true ALLOW FILTERING"
            
        prepared = self.session.prepare(query)
        rows = self.session.execute(prepared, [user_id])
        
        return [
            ApiKey(
                id=row.id,
                user_id=row.user_id,
                tenant_id=row.tenant_id,
                name=row.name,
                key="[REDACTED]",
                created_at=row.created_at,
                last_used_at=row.last_used_at,
                active=row.active
            )
            for row in rows
        ]
        
    def revoke(self, key_id: UUID) -> bool:
        """Revoke an API key"""
        query = "UPDATE api_keys SET active = false WHERE id = %s"
        prepared = self.session.prepare(query)
        
        self.session.execute(prepared, [key_id])
        return True


class TenantRepository(CassandraRepository[Tenant]):
    """Enhanced tenant repository"""
    
    def __init__(self, db_session: Session):
        super().__init__(db_session, Tenant, "auth_keyspace")
        self.table_name = "tenants"
        
    def create(self, tenant: TenantCreate) -> Tenant:
        """Create a new tenant"""
        tenant_id = uuid4()
        created_at = datetime.datetime.now(datetime.timezone.utc)
        
        query = """
        INSERT INTO tenants (
            id, name, status, tier, created_at, updated_at, 
            settings, resource_limits, features_enabled
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        prepared = self.session.prepare(query)
        prepared.consistency_level = ConsistencyLevel.QUORUM
        
        default_limits = {
            "max_users": 100,
            "max_storage_gb": 100,
            "max_api_calls_per_day": 10000
        }
        
        default_features = ["basic_auth", "api_access", "file_storage"]
        
        self.session.execute(
            prepared,
            [
                tenant_id, tenant.name, "pending", tenant.tier or "starter",
                created_at, created_at, {}, default_limits, default_features
            ]
        )
        
        return self.get(tenant_id)
        
    def get(self, tenant_id: UUID) -> Optional[Tenant]:
        """Get tenant by ID"""
        query = "SELECT * FROM tenants WHERE id = %s"
        prepared = self.session.prepare(query)
        
        rows = self.session.execute(prepared, [tenant_id])
        row = rows.one()
        
        return Tenant(**row) if row else None
        
    def update_status(self, tenant_id: UUID, status: str) -> Optional[Tenant]:
        """Update tenant status"""
        query = "UPDATE tenants SET status = %s, updated_at = %s WHERE id = %s"
        prepared = self.session.prepare(query)
        
        self.session.execute(
            prepared,
            [status, datetime.datetime.now(datetime.timezone.utc), tenant_id]
        )
        
        return self.get(tenant_id)
        
    def update_resource_limits(self, tenant_id: UUID, limits: Dict[str, Any]) -> Optional[Tenant]:
        """Update tenant resource limits"""
        query = "UPDATE tenants SET resource_limits = %s, updated_at = %s WHERE id = %s"
        prepared = self.session.prepare(query)
        
        self.session.execute(
            prepared,
            [limits, datetime.datetime.now(datetime.timezone.utc), tenant_id]
        )
        
        return self.get(tenant_id)
        
    def list_active(self, limit: int = 100) -> List[Tenant]:
        """List active tenants"""
        query = "SELECT * FROM tenants WHERE status = 'active' ALLOW FILTERING LIMIT %s"
        rows = self.session.execute(query, [limit])
        
        return [Tenant(**row) for row in rows]


# Keep existing repositories for now - they can be migrated incrementally
class InvitationRepository(CassandraRepository[InvitationCreate]):
    """Repository for user invitations"""
    def __init__(self, db_session: Session):
        super().__init__(db_session, InvitationCreate, "auth_keyspace")
        self.table_name = "invitations"

    def add(self, invite_in: InvitationCreate, invited_by: UUID, tenant_id: UUID) -> str:
        token = secrets.token_urlsafe(32)
        expires = datetime.datetime.now(datetime.timezone.utc) + timedelta(days=7)
        query = "INSERT INTO invitations (token, email_to_invite, invited_by_user_id, tenant_id, expires_at) VALUES (%s, %s, %s, %s, %s)"
        prepared = self.session.prepare(query)
        prepared.consistency_level = ConsistencyLevel.QUORUM
        self.session.execute(prepared, [token, invite_in.email_to_invite, invited_by, tenant_id, expires])
        return token

    def get(self, token: str) -> Optional[dict]:
        query = "SELECT * FROM invitations WHERE token = %s"
        prepared = self.session.prepare(query)
        prepared.consistency_level = ConsistencyLevel.ONE
        row = self.session.execute(prepared, [token]).one()
        return dict(row._asdict()) if row else None

    def mark_as_accepted(self, token: str):
        query = "UPDATE invitations SET is_accepted = true WHERE token = %s"
        prepared = self.session.prepare(query)
        prepared.consistency_level = ConsistencyLevel.QUORUM
        self.session.execute(prepared, [token])

class RoleRepository(CassandraRepository[RoleAssignmentRequest]):
    """Repository for user roles"""
    def __init__(self, db_session: Session):
        super().__init__(db_session, RoleAssignmentRequest, "auth_keyspace")
        self.table_name = "user_roles"

    def assign_role_to_user(self, user_id: UUID, role: str, tenant_id: UUID):
        query = "INSERT INTO user_roles (user_id, role, tenant_id) VALUES (%s, %s, %s)"
        prepared = self.session.prepare(query)
        prepared.consistency_level = ConsistencyLevel.QUORUM
        self.session.execute(prepared, [user_id, role, tenant_id])

    def remove_role_from_user(self, user_id: UUID, role: str):
        query = "DELETE FROM user_roles WHERE user_id = %s AND role = %s"
        prepared = self.session.prepare(query)
        prepared.consistency_level = ConsistencyLevel.QUORUM
        self.session.execute(prepared, [user_id, role])

    def get_roles_for_user(self, user_id: UUID) -> List[str]:
        query = "SELECT role FROM user_roles WHERE user_id = %s"
        prepared = self.session.prepare(query)
        rows = self.session.execute(prepared, [user_id])
        return [row.role for row in rows]

class SubscriptionRepository(CassandraRepository[SubscriptionUpdate]):
    """Repository for tenant subscriptions"""
    def __init__(self, db_session: Session):
        super().__init__(db_session, SubscriptionUpdate, "auth_keyspace")
        self.table_name = "subscriptions"

    def create_subscription_for_user(self, subscription: SubscriptionCreate):
        query = "INSERT INTO subscriptions (id, user_id, tier, status, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s)"
        prepared = self.session.prepare(query)
        prepared.consistency_level = ConsistencyLevel.QUORUM
        now = datetime.datetime.now(datetime.timezone.utc)
        self.session.execute(prepared, [uuid4(), subscription.user_id, subscription.tier, "active", now, now])

    def get_subscription_by_user_id(self, user_id: UUID) -> Optional[Subscription]:
        query = "SELECT * FROM subscriptions WHERE user_id = %s"
        prepared = self.session.prepare(query)
        prepared.consistency_level = ConsistencyLevel.ONE
        row = self.session.execute(prepared, [user_id]).one()
        return Subscription(**row) if row else None

    def update_subscription_for_user(self, user_id: UUID, sub_update: SubscriptionUpdate) -> Optional[Subscription]:
        sub = self.get_subscription_by_user_id(user_id)
        if not sub:
            return None
        
        updated_sub = sub.copy(update=sub_update.dict(exclude_unset=True))
        
        query = "UPDATE subscriptions SET tier = %s, status = %s, updated_at = %s WHERE id = %s"
        prepared = self.session.prepare(query)
        prepared.consistency_level = ConsistencyLevel.QUORUM
        self.session.execute(prepared, [updated_sub.tier, updated_sub.status, datetime.datetime.now(datetime.timezone.utc), sub.id])
        
        return self.get_subscription_by_user_id(user_id)

class OIDCRepository(CassandraRepository):
    pass

class SIWERepository(CassandraRepository):
    def __init__(self, db_session: Session):
        super().__init__(db_session, None, "auth_keyspace") # No schema model for SIWE
        self.table_name = "siwe_nonces"
        
    def use_nonce(self, nonce: str) -> bool:
        query = "SELECT nonce FROM siwe_nonces WHERE nonce = %s"
        prepared = self.session.prepare(query)
        prepared.consistency_level = ConsistencyLevel.ONE
        row = self.session.execute(prepared, [nonce]).one()
        if not row:
            return False
        
        delete_query = "DELETE FROM siwe_nonces WHERE nonce = %s"
        delete_prepared = self.session.prepare(delete_query)
        delete_prepared.consistency_level = ConsistencyLevel.QUORUM
        self.session.execute(delete_prepared, [nonce])
        return True

class AuditRepository(CassandraRepository):
    def __init__(self, db_session: Session):
        super().__init__(db_session, None, "auth_keyspace") # No schema model for AuditLog
        self.table_name = "audit_logs"
        
    def create_audit_log(self, user_id: UUID, event_type: str, details: str):
        query = "INSERT INTO audit_logs (id, user_id, event_type, details, timestamp) VALUES (%s, %s, %s, %s, %s)"
        prepared = self.session.prepare(query)
        prepared.consistency_level = ConsistencyLevel.QUORUM
        self.session.execute(prepared, [uuid4(), user_id, event_type, details, datetime.datetime.now(datetime.timezone.utc)])

class RefreshTokenRepository(CassandraRepository):
    def __init__(self, db_session: Session):
        super().__init__(db_session, None, "auth_keyspace") # No schema model for RefreshToken
        self.table_name = "refresh_tokens"
        
    def create_refresh_token(self, user_id: UUID) -> str:
        token = secrets.token_urlsafe(64)
        hashed_token = get_password_hash(token)
        expires = datetime.datetime.now(datetime.timezone.utc) + timedelta(days=30)
        
        query = "INSERT INTO refresh_tokens (token_hash, user_id, expires_at) VALUES (%s, %s, %s)"
        prepared = self.session.prepare(query)
        prepared.consistency_level = ConsistencyLevel.QUORUM
        self.session.execute(prepared, [hashed_token, user_id, expires])
        
        return token
        
    def validate_refresh_token(self, token: str) -> Optional[dict]:
        # This is inefficient and not secure. In a real app, you would not store the raw token.
        # This is a placeholder for a more secure implementation.
        hashed_token = get_password_hash(token)
        query = "SELECT * FROM refresh_tokens WHERE token_hash = %s"
        prepared = self.session.prepare(query)
        prepared.consistency_level = ConsistencyLevel.ONE
        row = self.session.execute(prepared, [hashed_token]).one()
        
        if not row:
            return None
            
        if row.expires_at < datetime.datetime.now(datetime.timezone.utc):
            return None
            
        return dict(row._asdict())

    def revoke_refresh_token(self, token: str):
        hashed_token = get_password_hash(token)
        query = "DELETE FROM refresh_tokens WHERE token_hash = %s"
        prepared = self.session.prepare(query)
        prepared.consistency_level = ConsistencyLevel.QUORUM
        self.session.execute(prepared, [hashed_token]) 