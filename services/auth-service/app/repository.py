import datetime
import secrets
from datetime import timedelta
from uuid import UUID, uuid4
from typing import Optional, List, Tuple

from cassandra.cluster import Session

from .schemas.user import UserCreate, UserUpdate
from .schemas.api_key import ApiKey, ApiKeyCreate
from .schemas.invitation import InvitationCreate
from .schemas.role import RoleAssignmentRequest
from .schemas.subscription import Subscription, SubscriptionCreate, SubscriptionUpdate
from .schemas.tenant import Tenant, TenantCreate
from platformq.shared.repository import AbstractRepository
from .core.security import get_password_hash


class UserRepository(AbstractRepository[UserUpdate]):
    def __init__(self, db_session: Session):
        self.db_session = db_session
    
    def get_by_email(self, email: str) -> Optional[UserUpdate]:
        query = "SELECT * FROM users WHERE email = %s ALLOW FILTERING"
        prepared = self.db_session.prepare(query)
        rows = self.db_session.execute(prepared, [email])
        row = rows.one()
        return UserUpdate(**row) if row else None

    def get_by_wallet_address(self, wallet_address: str) -> Optional[UserUpdate]:
        query = "SELECT * FROM users WHERE wallet_address = %s ALLOW FILTERING"
        prepared = self.db_session.prepare(query)
        rows = self.db_session.execute(prepared, [wallet_address])
        row = rows.one()
        return UserUpdate(**row) if row else None

    def add(self, user: UserCreate, tenant_id: UUID) -> UserUpdate:
        user_id = uuid4()
        created_time = datetime.datetime.now(datetime.timezone.utc)

        query = """
        INSERT INTO users (tenant_id, id, email, full_name, status, created_at, updated_at, did, wallet_address)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        prepared_statement = self.db_session.prepare(query)
        self.db_session.execute(
            prepared_statement,
            [tenant_id, user_id, user.email, user.full_name, "active", created_time, created_time, user.did, user.wallet_address],
        )
        return self.get(user_id=user_id)
    
    def get(self, user_id: UUID) -> Optional[UserUpdate]:
        query = "SELECT * FROM users WHERE id = %s ALLOW FILTERING"
        prepared_statement = self.db_session.prepare(query)
        rows = self.db_session.execute(prepared_statement, [user_id])
        row = rows.one()
        return UserUpdate(**row) if row else None
        
    def list(self) -> list[UserUpdate]:
        query = "SELECT * FROM users"
        rows = self.db_session.execute(query)
        return [UserUpdate(**row) for row in rows]

    def update(self, user_id: UUID, user_update: UserUpdate) -> Optional[UserUpdate]:
        #This is a simplified update method. In a real application, you would
        #want to be more specific about which fields can be updated.
        user = self.get(user_id)
        if not user:
            return None
        
        updated_user = user.copy(update=user_update.dict(exclude_unset=True))
        
        query = "UPDATE users SET full_name = %s, did = %s, wallet_address = %s, storage_backend = %s, storage_config = %s, updated_at = %s WHERE id = %s and tenant_id = %s"
        prepared = self.db_session.prepare(query)
        self.db_session.execute(prepared, [updated_user.full_name, updated_user.did, updated_user.wallet_address, updated_user.storage_backend, updated_user.storage_config, datetime.datetime.now(datetime.timezone.utc), user_id, user.tenant_id])
        
        return self.get(user_id)
        
    def delete(self, user_id: UUID):
        user = self.get(user_id)
        if not user:
            return

        anonymized_email = f"deleted_{user_id}@platformq.com"
        anonymized_name = "Deleted User"

        query = "UPDATE users SET email = %s, full_name = %s, status = 'deleted', updated_at = %s WHERE id = %s and tenant_id = %s"
        prepared = self.db_session.prepare(query)
        self.db_session.execute(
            prepared,
            [
                anonymized_email,
                anonymized_name,
                datetime.datetime.now(datetime.timezone.utc),
                user_id,
                user.tenant_id
            ],
        )


class APIKeyRepository(AbstractRepository[ApiKey]):
    def __init__(self, db_session: Session):
        self.db_session = db_session
        
    def add(self, key_in: ApiKeyCreate, user_id: UUID) -> ApiKey:
        full_key, secret, prefix = self._generate_api_key()
        hashed_key = get_password_hash(secret)

        now = datetime.datetime.now(datetime.timezone.utc)

        query = """
        INSERT INTO api_keys (key_prefix, hashed_key, user_id, created_at, expires_at, is_active)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        prepared = self.db_session.prepare(query)
        self.db_session.execute(prepared, [prefix, hashed_key, user_id, now, key_in.expires_at, True])

        key_data = self.get(prefix=prefix)
        key_data.full_key = full_key
        return key_data

    def get(self, prefix: str) -> Optional[ApiKey]:
        query = "SELECT * FROM api_keys WHERE key_prefix = %s"
        prepared = self.db_session.prepare(query)
        row = self.db_session.execute(prepared, [prefix]).one()
        return ApiKey(**row) if row else None
        
    def list(self, user_id: UUID) -> List[ApiKey]:
        query = "SELECT * FROM api_keys WHERE user_id = %s"
        prepared = self.db_session.prepare(query)
        rows = self.db_session.execute(prepared, [user_id])
        return [ApiKey(**row) for row in rows]
        
    def delete(self, prefix: str):
        key_info = self.get(prefix)
        if not key_info:
            return

        query = "UPDATE api_keys SET is_active = false WHERE key_prefix = %s"
        prepared = self.db_session.prepare(query)
        self.db_session.execute(prepared, [prefix])

    def _generate_api_key(self) -> Tuple[str, str, str]:
        prefix = f"pk_test_{secrets.token_hex(8)}"
        secret = secrets.token_urlsafe(32)
        full_key = f"{prefix}_{secret}"
        return full_key, secret, prefix 

class InvitationRepository(AbstractRepository[InvitationCreate]):
    def __init__(self, db_session: Session):
        self.db_session = db_session

    def add(self, invite_in: InvitationCreate, invited_by: UUID, tenant_id: UUID) -> str:
        token = secrets.token_urlsafe(32)
        expires = datetime.datetime.now(datetime.timezone.utc) + timedelta(days=7)
        query = "INSERT INTO invitations (token, email_to_invite, invited_by_user_id, tenant_id, expires_at) VALUES (%s, %s, %s, %s, %s)"
        prepared = self.db_session.prepare(query)
        self.db_session.execute(prepared, [token, invite_in.email_to_invite, invited_by, tenant_id, expires])
        return token

    def get(self, token: str) -> Optional[dict]:
        query = "SELECT * FROM invitations WHERE token = %s"
        prepared = self.db_session.prepare(query)
        row = self.db_session.execute(prepared, [token]).one()
        return dict(row._asdict()) if row else None

    def mark_as_accepted(self, token: str):
        query = "UPDATE invitations SET is_accepted = true WHERE token = %s"
        prepared = self.db_session.prepare(query)
        self.db_session.execute(prepared, [token])

class RoleRepository(AbstractRepository[RoleAssignmentRequest]):
    def __init__(self, db_session: Session):
        self.db_session = db_session

    def assign_role_to_user(self, user_id: UUID, role: str, tenant_id: UUID):
        query = "INSERT INTO user_roles (user_id, role, tenant_id) VALUES (%s, %s, %s)"
        prepared = self.db_session.prepare(query)
        self.db_session.execute(prepared, [user_id, role, tenant_id])

    def remove_role_from_user(self, user_id: UUID, role: str):
        query = "DELETE FROM user_roles WHERE user_id = %s AND role = %s"
        prepared = self.db_session.prepare(query)
        self.db_session.execute(prepared, [user_id, role])

    def get_roles_for_user(self, user_id: UUID) -> List[str]:
        query = "SELECT role FROM user_roles WHERE user_id = %s"
        prepared = self.db_session.prepare(query)
        rows = self.db_session.execute(prepared, [user_id])
        return [row.role for row in rows]

class SubscriptionRepository(AbstractRepository[SubscriptionUpdate]):
    def __init__(self, db_session: Session):
        self.db_session = db_session

    def create_subscription_for_user(self, subscription: SubscriptionCreate):
        query = "INSERT INTO subscriptions (id, user_id, tier, status, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s)"
        prepared = self.db_session.prepare(query)
        now = datetime.datetime.now(datetime.timezone.utc)
        self.db_session.execute(prepared, [uuid4(), subscription.user_id, subscription.tier, "active", now, now])

    def get_subscription_by_user_id(self, user_id: UUID) -> Optional[Subscription]:
        query = "SELECT * FROM subscriptions WHERE user_id = %s"
        prepared = self.db_session.prepare(query)
        row = self.db_session.execute(prepared, [user_id]).one()
        return Subscription(**row) if row else None

    def update_subscription_for_user(self, user_id: UUID, sub_update: SubscriptionUpdate) -> Optional[Subscription]:
        sub = self.get_subscription_by_user_id(user_id)
        if not sub:
            return None
        
        updated_sub = sub.copy(update=sub_update.dict(exclude_unset=True))
        
        query = "UPDATE subscriptions SET tier = %s, status = %s, updated_at = %s WHERE id = %s"
        prepared = self.db_session.prepare(query)
        self.db_session.execute(prepared, [updated_sub.tier, updated_sub.status, datetime.datetime.now(datetime.timezone.utc), sub.id])
        
        return self.get_subscription_by_user_id(user_id)

class TenantRepository(AbstractRepository[TenantCreate]):
    def __init__(self, db_session: Session):
        self.db_session = db_session

    def create_tenant(self, name: str) -> dict:
        tenant_id = uuid4()
        query = "INSERT INTO tenants (id, name, created_at) VALUES (%s, %s, %s)"
        prepared = self.db_session.prepare(query)
        self.db_session.execute(prepared, [tenant_id, name, datetime.datetime.now(datetime.timezone.utc)])
        return {"id": tenant_id, "name": name}

class OIDCRepository(AbstractRepository):
    pass

class SIWERepository(AbstractRepository):
    def __init__(self, db_session: Session):
        self.db_session = db_session
        
    def use_nonce(self, nonce: str) -> bool:
        query = "SELECT nonce FROM siwe_nonces WHERE nonce = %s"
        prepared = self.db_session.prepare(query)
        row = self.db_session.execute(prepared, [nonce]).one()
        if not row:
            return False
        
        delete_query = "DELETE FROM siwe_nonces WHERE nonce = %s"
        delete_prepared = self.db_session.prepare(delete_query)
        self.db_session.execute(delete_prepared, [nonce])
        return True

class AuditRepository(AbstractRepository):
    def __init__(self, db_session: Session):
        self.db_session = db_session
        
    def create_audit_log(self, user_id: UUID, event_type: str, details: str):
        query = "INSERT INTO audit_logs (id, user_id, event_type, details, timestamp) VALUES (%s, %s, %s, %s, %s)"
        prepared = self.db_session.prepare(query)
        self.db_session.execute(prepared, [uuid4(), user_id, event_type, details, datetime.datetime.now(datetime.timezone.utc)])

class RefreshTokenRepository(AbstractRepository):
    def __init__(self, db_session: Session):
        self.db_session = db_session
        
    def create_refresh_token(self, user_id: UUID) -> str:
        token = secrets.token_urlsafe(64)
        hashed_token = get_password_hash(token)
        expires = datetime.datetime.now(datetime.timezone.utc) + timedelta(days=30)
        
        query = "INSERT INTO refresh_tokens (token_hash, user_id, expires_at) VALUES (%s, %s, %s)"
        prepared = self.db_session.prepare(query)
        self.db_session.execute(prepared, [hashed_token, user_id, expires])
        
        return token
        
    def validate_refresh_token(self, token: str) -> Optional[dict]:
        # This is inefficient and not secure. In a real app, you would not store the raw token.
        # This is a placeholder for a more secure implementation.
        hashed_token = get_password_hash(token)
        query = "SELECT * FROM refresh_tokens WHERE token_hash = %s"
        prepared = self.db_session.prepare(query)
        row = self.db_session.execute(prepared, [hashed_token]).one()
        
        if not row:
            return None
            
        if row.expires_at < datetime.datetime.now(datetime.timezone.utc):
            return None
            
        return dict(row._asdict())

    def revoke_refresh_token(self, token: str):
        hashed_token = get_password_hash(token)
        query = "DELETE FROM refresh_tokens WHERE token_hash = %s"
        prepared = self.db_session.prepare(query)
        self.db_session.execute(prepared, [hashed_token]) 