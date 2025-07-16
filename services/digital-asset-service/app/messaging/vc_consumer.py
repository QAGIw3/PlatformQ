"""
Consumer for handling VC-related events in the digital asset service
"""

import asyncio
import json
import logging
import threading
import pulsar
from pulsar.schema import AvroSchema, Record, String
from sqlalchemy.orm import Session
from typing import Optional
from uuid import UUID

from ..db.database import SessionLocal
from ..crud import crud_digital_asset

logger = logging.getLogger(__name__)

PULSAR_URL = "pulsar://localhost:6650"


class AssetVCCreated(Record):
    tenant_id = String()
    asset_id = String()
    vc_id = String()
    vc_type = String()


class AssetVCConsumer:
    def __init__(self, pulsar_url: str = PULSAR_URL):
        self.pulsar_url = pulsar_url
        self.running = False
        self.consumer = None
        self.client = None
        self.thread = None

    def start(self):
        """Start the consumer in a background thread"""
        if self.running:
            logger.warning("Consumer is already running")
            return
            
        self.running = True
        self.thread = threading.Thread(target=self._run_consumer, daemon=True)
        self.thread.start()
        logger.info("Asset VC consumer started")

    def stop(self):
        """Stop the consumer"""
        self.running = False
        if self.thread:
            self.thread.join(timeout=5)
        logger.info("Asset VC consumer stopped")

    def _run_consumer(self):
        """Main consumer loop"""
        self.client = pulsar.Client(self.pulsar_url)
        
        try:
            self.consumer = self.client.subscribe(
                topic_pattern="persistent://platformq/.*/asset-vc-created-events",
                subscription_name="digital-asset-service-vc-sub",
                consumer_type=pulsar.ConsumerType.Shared,
                schema=AvroSchema(AssetVCCreated)
            )
            
            while self.running:
                try:
                    msg = self.consumer.receive(timeout_millis=1000)
                    if msg:
                        self._process_message(msg)
                        self.consumer.acknowledge(msg)
                except pulsar.Timeout:
                    continue
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    
        except Exception as e:
            logger.error(f"Failed to start consumer: {e}")
        finally:
            if self.consumer:
                self.consumer.close()
            if self.client:
                self.client.close()

    def _process_message(self, msg):
        """Process a single message"""
        try:
            event = msg.value()
            logger.info(f"Received VC creation event for asset {event.asset_id}")
            
            # Update the asset with the VC ID
            db = SessionLocal()
            try:
                asset_id = UUID(event.asset_id)
                
                if event.vc_type == "creation":
                    # Update creation VC ID
                    updated = crud_digital_asset.update_asset_metadata(
                        db=db,
                        asset_id=asset_id,
                        new_metadata={"creation_vc_id": event.vc_id}
                    )
                    
                    if updated:
                        # Also update the dedicated field if it exists
                        asset = crud_digital_asset.get_asset(db, asset_id)
                        if asset:
                            asset.creation_vc_id = event.vc_id
                            db.commit()
                            logger.info(f"Updated asset {asset_id} with creation VC {event.vc_id}")
                    
                elif event.vc_type == "processing":
                    # Add to lineage VCs
                    asset = crud_digital_asset.get_asset(db, asset_id)
                    if asset:
                        lineage_vcs = asset.lineage_vc_ids or []
                        if event.vc_id not in lineage_vcs:
                            lineage_vcs.append(event.vc_id)
                            asset.lineage_vc_ids = lineage_vcs
                            asset.latest_processing_vc_id = event.vc_id
                            db.commit()
                            logger.info(f"Added processing VC {event.vc_id} to asset {asset_id} lineage")
                
            finally:
                db.close()
                
        except Exception as e:
            logger.error(f"Failed to process VC event: {e}")


# Global consumer instance
asset_vc_consumer = AssetVCConsumer() 