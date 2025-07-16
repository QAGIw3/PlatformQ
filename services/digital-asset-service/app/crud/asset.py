import uuid
import time
from uuid import uuid4, UUID
from typing import List, Optional
from datetime import datetime
import requests
import os

from cassandra.cluster import Session
from ..db.cassandra import get_cassandra_session
from ..schemas.asset import DigitalAsset, DigitalAssetCreate, PeerReview, PeerReviewCreate
from ..messaging.pulsar import pulsar_service

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

class CRUDAsset:
    def get_session(self) -> Session:
        return get_cassandra_session()

    def create(self, *, obj_in: DigitalAssetCreate, owner_id: str) -> DigitalAsset:
        session = self.get_session()
        asset_id = uuid4()
        session.execute(
            """
            INSERT INTO digital_assets (id, name, description, s3_url, owner_id)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (asset_id, obj_in.name, obj_in.description, obj_in.s3_url, owner_id),
        )
        return self.get(asset_id)

    def get(self, id: UUID) -> Optional[DigitalAsset]:
        session = self.get_session()
        row = session.execute("SELECT * FROM digital_assets WHERE id = %s", (id,)).one()
        if row:
            asset = DigitalAsset(**row)
            # Fetch reviews
            review_rows = session.execute("SELECT * FROM peer_reviews WHERE asset_id = %s", (id,)).all()
            asset.reviews = [PeerReview(**review_row) for review_row in review_rows]
            return asset
        return None

    def get_multi(self) -> List[DigitalAsset]:
        session = self.get_session()
        rows = session.execute("SELECT * FROM digital_assets")
        # N+1 query problem here. In a real app, you might denormalize
        # or use a different data model.
        return [self.get(row['id']) for row in rows]

    def add_review(self, asset_id: UUID, reviewer_id: str, review_content: str) -> Optional[PeerReview]:
        session = self.get_session()
        # Check if asset exists
        asset = self.get(asset_id)
        if asset:
            review_id = uuid4()
            created_at = datetime.utcnow()
            session.execute(
                """
                INSERT INTO peer_reviews (id, asset_id, reviewer_id, review_content, created_at)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (review_id, asset_id, reviewer_id, review_content, created_at)
            )
            
            review = PeerReview(
                id=review_id,
                asset_id=asset_id,
                reviewer_id=reviewer_id,
                review_content=review_content,
                created_at=created_at
            )

            # Publish "AssetPeerReviewed" event to Pulsar
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

    def approve_asset(self, asset_id: UUID) -> Optional[DigitalAsset]:
        """
        Approves an asset if it meets the required number of peer reviews,
        based on the author's reputation score.
        """
        session = self.get_session()
        asset = self.get(asset_id)
        if not asset or asset.status == "approved":
            return None

        author_reputation = get_user_reputation(asset.owner_id)
        
        required_reviews = HIGH_REP_REVIEWS_REQUIRED if author_reputation >= HIGH_REP_THRESHOLD else DEFAULT_REVIEWS_REQUIRED

        if len(asset.reviews) >= required_reviews:
            session.execute(
                "UPDATE digital_assets SET status = 'approved', version = version + 1 WHERE id = %s",
                (asset_id,)
            )
            # In a real app with optimistic locking, you would check the IF version = ? clause
            
            updated_asset = self.get(asset_id)
            # Here you might want to publish an "AssetApproved" event
            return updated_asset
        
        return asset # Return the unchanged asset if criteria not met

asset = CRUDAsset() 