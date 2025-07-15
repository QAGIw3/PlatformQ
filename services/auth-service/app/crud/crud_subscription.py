import datetime
from uuid import UUID, uuid4

from cassandra.cluster import Session

from ..schemas.subscription import (
    SubscriptionCreate,
    SubscriptionUpdate,
    UsageQuotaCreate,
)


def get_quotas_for_subscription(db: Session, *, tenant_id: UUID, subscription_id: UUID):
    """
    Retrieves all usage quotas for a given subscription.
    """
    query = "SELECT subscription_id, feature, quota, used FROM usage_quotas WHERE tenant_id = %s AND subscription_id = %s"
    prepared = db.prepare(query)
    return db.execute(prepared, [tenant_id, subscription_id]).all()


def create_or_update_quota(db: Session, *, tenant_id: UUID, quota: UsageQuotaCreate):
    """
    Creates or updates a usage quota for a subscription.
    """
    query = "INSERT INTO usage_quotas (tenant_id, subscription_id, feature, quota, used) VALUES (%s, %s, %s, %s, %s)"
    prepared = db.prepare(query)
    db.execute(
        prepared, [tenant_id, quota.subscription_id, quota.feature, quota.quota, quota.used or 0]
    )


def get_subscription_by_user_id(db: Session, *, tenant_id: UUID, user_id: UUID):
    """
    Retrieves a user's subscription details, including their quotas.
    """
    query = "SELECT id, user_id, tier, status, billing_cycle_start, billing_cycle_end FROM subscriptions WHERE tenant_id = %s AND user_id = %s ALLOW FILTERING"
    prepared = db.prepare(query)
    sub_row = db.execute(prepared, [tenant_id, user_id]).one()

    if not sub_row:
        return None

    # Fetch associated quotas
    quotas = get_quotas_for_subscription(db, tenant_id=tenant_id, subscription_id=sub_row.id)

    subscription_data = dict(sub_row._asdict())
    subscription_data["quotas"] = [dict(q._asdict()) for q in quotas]

    return subscription_data


def update_subscription_for_user(
    db: Session, *, tenant_id: UUID, user_id: UUID, sub_update: SubscriptionUpdate
):
    """
    Updates a user's subscription tier or status.
    """
    # We need the current subscription to know its ID
    current_sub = get_subscription_by_user_id(db, tenant_id=tenant_id, user_id=user_id)
    if not current_sub:
        return None

    # For simplicity, we'll update fields if they are provided.
    # A more complex system might recalculate billing cycles, etc.
    if sub_update.tier:
        query = "UPDATE subscriptions SET tier = %s WHERE tenant_id = %s AND id = %s"
        prepared = db.prepare(query)
        db.execute(prepared, [sub_update.tier, tenant_id, current_sub["id"]])

    if sub_update.status:
        query = "UPDATE subscriptions SET status = %s WHERE tenant_id = %s AND id = %s"
        prepared = db.prepare(query)
        db.execute(prepared, [sub_update.status, tenant_id, current_sub["id"]])

    return get_subscription_by_user_id(db, tenant_id=tenant_id, user_id=user_id)


def create_subscription_for_user(db: Session, *, tenant_id: UUID, subscription: SubscriptionCreate):
    """
    Creates a new subscription for a user and sets default quotas based on tier.
    """
    sub_id = uuid4()
    now = datetime.datetime.now(datetime.timezone.utc)
    # Default billing cycle to 30 days from now
    end_date = now + datetime.timedelta(days=30)

    query = """
    INSERT INTO subscriptions (tenant_id, id, user_id, tier, status, billing_cycle_start, billing_cycle_end)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    prepared = db.prepare(query)
    db.execute(
        prepared,
        (
            tenant_id,
            sub_id,
            subscription.user_id,
            subscription.tier,
            subscription.status,
            now,
            end_date,
        ),
    )

    # For a new "free" tier subscription, add some default quotas
    if subscription.tier == 'free':
        create_or_update_quota(
            db,
            tenant_id=tenant_id,
            quota=UsageQuotaCreate(
                subscription_id=sub_id, feature="api_calls", quota=1000, used=0
            ),
        )
        create_or_update_quota(
            db,
            tenant_id=tenant_id,
            quota=UsageQuotaCreate(
                subscription_id=sub_id, feature="storage_gb", quota=1, used=0
            ),
        )

    return get_subscription_by_user_id(db, tenant_id=tenant_id, user_id=subscription.user_id)
