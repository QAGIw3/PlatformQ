from elasticsearch import AsyncElasticsearch
from platformq.events import IndexableEntityEvent
import logging

from ..core.config import settings

logger = logging.getLogger(__name__)

class Indexer:
    def __init__(self, es_client: AsyncElasticsearch):
        self.es_client = es_client
        self.index_name = settings.ES_INDEX_NAME

    async def handle_event(self, event: IndexableEntityEvent):
        if event.event_type == "DELETED":
            await self.delete_document(event.entity_id)
        else:
            document = self._transform_event_to_document(event)
            await self.index_document(document)

    async def index_document(self, document: dict):
        entity_id = document.get("entity_id")
        if not entity_id:
            logger.error("Document is missing entity_id")
            return

        try:
            await self.es_client.index(
                index=self.index_name,
                id=entity_id,
                document=document
            )
            logger.info(f"Indexed document {entity_id}")
        except Exception as e:
            logger.error(f"Failed to index document {entity_id}: {e}")

    async def delete_document(self, entity_id: str):
        try:
            await self.es_client.delete(
                index=self.index_name,
                id=entity_id
            )
            logger.info(f"Deleted document {entity_id}")
        except Exception as e:
            logger.error(f"Failed to delete document {entity_id}: {e}")

    def _transform_event_to_document(self, event: IndexableEntityEvent) -> dict:
        document = {
            "entity_id": event.entity_id,
            "entity_type": event.entity_type,
            "source_service": event.source_service,
            "event_timestamp": event.event_timestamp,
            **event.data
        }
        return document
