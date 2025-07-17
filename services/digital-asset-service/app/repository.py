from cassandra.cluster import Session
from typing import List, Optional
import uuid
import requests
import os
from datetime import datetime

from .db.cassandra import get_cassandra_session
from .schemas import asset as schemas
from platformq.shared.repository import AbstractRepository
from .messaging.pulsar import pulsar_service
import time

VC_SERVICE_URL = os.getenv("VC_SERVICE_URL", "http://verifiable-credential-service:80")
HIGH_REP_THRESHOLD = 1000
HIGH_REP_REVIEWS_REQUIRED = 1
DEFAULT_REVIEWS_REQUIRED = 2

def get_user_reputation(user_id: str) -> int:
    """Fetches reputation for a user from the VC service."""
    try:
        headers = {"Authorization": "Bearer mock-service-token-for-tenant-default"}
        response = requests.get(f"{VC_SERVICE_URL}/api/v1/reputation/{user_id}", headers=headers, timeout=2)
        if response.status_code == 200:
            return response.json().get("reputation_score", 0)
    except requests.exceptions.RequestException:
        return 0 # Default to 0 if service is unavailable
    return 0

class DigitalAssetRepository(AbstractRepository[schemas.DigitalAsset]):
    def __init__(self, db_session: Session):
        self.db_session = db_session

    def add(self, asset: schemas.DigitalAssetCreate, owner_id: str) -> schemas.DigitalAsset:
        asset_id = uuid.uuid4()
        self.db_session.execute(
            """
            INSERT INTO digital_assets (id, name, description, s3_url, owner_id)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (asset_id, asset.name, asset.description, asset.s3_url, owner_id),
        )
        return self.get(asset_id)

    def get(self, id: uuid.UUID) -> Optional[schemas.DigitalAsset]:
        row = self.db_session.execute("SELECT * FROM digital_assets WHERE id = %s", (id,)).one()
        if row:
            asset = schemas.DigitalAsset(**row)
            # Fetch reviews
            review_rows = self.db_session.execute("SELECT * FROM peer_reviews WHERE asset_id = %s", (id,)).all()
            asset.reviews = [schemas.PeerReview(**review_row) for review_row in review_rows]
            return asset
        return None

    def list(self) -> List[schemas.DigitalAsset]:
        rows = self.db_session.execute("SELECT * FROM digital_assets")
        return [self.get(row['id']) for row in rows]

    def add_review(self, asset_id: uuid.UUID, reviewer_id: str, review_content: str) -> Optional[schemas.PeerReview]:
        asset = self.get(asset_id)
        if asset:
            review_id = uuid.uuid4()
            created_at = datetime.utcnow()
            self.db_session.execute(
                """
                INSERT INTO peer_reviews (id, asset_id, reviewer_id, review_content, created_at)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (review_id, asset_id, reviewer_id, review_content, created_at)
            )
            
            review = schemas.PeerReview(
                id=review_id,
                asset_id=asset_id,
                reviewer_id=reviewer_id,
                review_content=review_content,
                created_at=created_at
            )

            event_data = {
                "event_id": str(uuid.uuid4()),
                "asset_id": str(asset_id),
                "reviewer_id": reviewer_id,
                "review_id": str(review.id),
                "timestamp": int(time.time() * 1000)
            }
            pulsar_service.publish_event(event_data)

            return review
        return None

    def approve_asset(self, asset_id: uuid.UUID) -> Optional[schemas.DigitalAsset]:
        asset = self.get(asset_id)
        if not asset or asset.status == "approved":
            return None

        author_reputation = get_user_reputation(asset.owner_id)
        
        required_reviews = HIGH_REP_REVIEWS_REQUIRED if author_reputation >= HIGH_REP_THRESHOLD else DEFAULT_REVIEWS_REQUIRED

        if len(asset.reviews) >= required_reviews:
            self.db_session.execute(
                "UPDATE digital_assets SET status = 'approved', version = version + 1 WHERE id = %s",
                (asset_id,)
            )
            return self.get(asset_id)
        
        return asset 