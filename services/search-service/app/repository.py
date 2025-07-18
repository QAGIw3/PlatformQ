"""
Repository for Search Service

Uses the enhanced repository patterns from platformq-shared.
"""

import uuid
import logging
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, func, desc

from platformq_shared import PostgresRepository, QueryBuilder
from platformq_shared.event_publisher import EventPublisher
from platformq_events import (
    SearchPerformedEvent,
    SearchResultClickedEvent
)

from .database import SearchIndex, SearchHistory, SavedSearch, SearchAlert
from .schemas import (
    SearchHistoryCreate,
    SavedSearchCreate,
    SearchAlertCreate,
    SearchIndexFilter
)

logger = logging.getLogger(__name__)


class SearchIndexRepository(PostgresRepository[SearchIndex]):
    """Repository for search index metadata"""
    
    def __init__(self, db_session_factory):
        super().__init__(db_session_factory, SearchIndex)
        
    def record_indexing(self, entity_id: str, entity_type: str,
                       operation: str, tenant_id: str) -> SearchIndex:
        """Record an indexing operation"""
        with self.get_session() as session:
            record = SearchIndex(
                id=uuid.uuid4(),
                entity_id=entity_id,
                entity_type=entity_type,
                operation=operation,
                tenant_id=tenant_id,
                indexed_at=datetime.utcnow(),
                status="completed"
            )
            
            session.add(record)
            session.commit()
            session.refresh(record)
            
            return record
            
    def get_index_status(self, entity_type: str, tenant_id: str) -> Dict[str, Any]:
        """Get indexing status for entity type"""
        with self.get_session() as session:
            # Total indexed
            total = session.query(SearchIndex).filter(
                and_(
                    SearchIndex.entity_type == entity_type,
                    SearchIndex.tenant_id == tenant_id,
                    SearchIndex.status == "completed"
                )
            ).count()
            
            # Recent operations
            recent_cutoff = datetime.utcnow() - timedelta(hours=24)
            recent_ops = session.query(
                SearchIndex.operation,
                func.count(SearchIndex.id)
            ).filter(
                and_(
                    SearchIndex.entity_type == entity_type,
                    SearchIndex.tenant_id == tenant_id,
                    SearchIndex.indexed_at >= recent_cutoff
                )
            ).group_by(SearchIndex.operation).all()
            
            # Last indexed
            last_indexed = session.query(SearchIndex).filter(
                and_(
                    SearchIndex.entity_type == entity_type,
                    SearchIndex.tenant_id == tenant_id
                )
            ).order_by(SearchIndex.indexed_at.desc()).first()
            
            return {
                "entity_type": entity_type,
                "total_indexed": total,
                "recent_operations": dict(recent_ops),
                "last_indexed_at": last_indexed.indexed_at.isoformat() if last_indexed else None
            }
            
    def get_pending_reindex(self, limit: int = 100) -> List[SearchIndex]:
        """Get entities pending reindexing"""
        with self.get_session() as session:
            return session.query(SearchIndex).filter(
                SearchIndex.status == "pending_reindex"
            ).order_by(SearchIndex.indexed_at).limit(limit).all()


class SearchHistoryRepository(PostgresRepository[SearchHistory]):
    """Repository for search history and analytics"""
    
    def __init__(self, db_session_factory, event_publisher: Optional[EventPublisher] = None):
        super().__init__(db_session_factory, SearchHistory)
        self.event_publisher = event_publisher
        
    def record_search(self, user_id: str, tenant_id: str,
                     query: str, filters: Dict[str, Any],
                     results_count: int, search_type: str = "unified") -> SearchHistory:
        """Record a search query"""
        with self.get_session() as session:
            history = SearchHistory(
                id=uuid.uuid4(),
                user_id=user_id,
                tenant_id=tenant_id,
                query=query,
                filters=filters,
                results_count=results_count,
                search_type=search_type,
                searched_at=datetime.utcnow()
            )
            
            session.add(history)
            session.commit()
            session.refresh(history)
            
            # Publish event
            if self.event_publisher:
                event = SearchPerformedEvent(
                    search_id=str(history.id),
                    user_id=user_id,
                    tenant_id=tenant_id,
                    query=query,
                    search_type=search_type,
                    results_count=results_count,
                    searched_at=history.searched_at.isoformat()
                )
                
                self.event_publisher.publish_event(
                    topic=f"persistent://platformq/{tenant_id}/search-performed-events",
                    event=event
                )
                
            return history
            
    def record_click(self, search_id: uuid.UUID, result_id: str,
                    result_type: str, position: int) -> bool:
        """Record a search result click"""
        with self.get_session() as session:
            history = session.query(SearchHistory).filter(
                SearchHistory.id == search_id
            ).first()
            
            if not history:
                return False
                
            # Update clicks
            clicks = history.clicked_results or []
            clicks.append({
                "result_id": result_id,
                "result_type": result_type,
                "position": position,
                "clicked_at": datetime.utcnow().isoformat()
            })
            history.clicked_results = clicks
            
            session.commit()
            
            # Publish event
            if self.event_publisher:
                event = SearchResultClickedEvent(
                    search_id=str(search_id),
                    user_id=history.user_id,
                    result_id=result_id,
                    result_type=result_type,
                    position=position,
                    query=history.query
                )
                
                self.event_publisher.publish_event(
                    topic=f"persistent://platformq/{history.tenant_id}/search-click-events",
                    event=event
                )
                
            return True
            
    def get_user_history(self, user_id: str, tenant_id: str,
                        limit: int = 50) -> List[SearchHistory]:
        """Get user's search history"""
        with self.get_session() as session:
            return session.query(SearchHistory).filter(
                and_(
                    SearchHistory.user_id == user_id,
                    SearchHistory.tenant_id == tenant_id
                )
            ).order_by(SearchHistory.searched_at.desc()).limit(limit).all()
            
    def get_popular_searches(self, tenant_id: str, days: int = 7,
                           limit: int = 20) -> List[Dict[str, Any]]:
        """Get popular searches in the last N days"""
        with self.get_session() as session:
            cutoff = datetime.utcnow() - timedelta(days=days)
            
            results = session.query(
                SearchHistory.query,
                func.count(SearchHistory.id).label("count")
            ).filter(
                and_(
                    SearchHistory.tenant_id == tenant_id,
                    SearchHistory.searched_at >= cutoff
                )
            ).group_by(SearchHistory.query).order_by(
                desc("count")
            ).limit(limit).all()
            
            return [
                {"query": query, "count": count}
                for query, count in results
            ]
            
    def get_search_analytics(self, tenant_id: str,
                           time_window: timedelta) -> Dict[str, Any]:
        """Get search analytics for time window"""
        with self.get_session() as session:
            cutoff = datetime.utcnow() - time_window
            
            base_query = session.query(SearchHistory).filter(
                and_(
                    SearchHistory.tenant_id == tenant_id,
                    SearchHistory.searched_at >= cutoff
                )
            )
            
            # Total searches
            total_searches = base_query.count()
            
            # Unique users
            unique_users = session.query(
                func.count(func.distinct(SearchHistory.user_id))
            ).filter(
                and_(
                    SearchHistory.tenant_id == tenant_id,
                    SearchHistory.searched_at >= cutoff
                )
            ).scalar()
            
            # Search types breakdown
            search_types = session.query(
                SearchHistory.search_type,
                func.count(SearchHistory.id)
            ).filter(
                and_(
                    SearchHistory.tenant_id == tenant_id,
                    SearchHistory.searched_at >= cutoff
                )
            ).group_by(SearchHistory.search_type).all()
            
            # Zero result rate
            zero_results = base_query.filter(
                SearchHistory.results_count == 0
            ).count()
            zero_result_rate = (zero_results / total_searches * 100) if total_searches > 0 else 0
            
            # Click-through rate
            searches_with_clicks = base_query.filter(
                SearchHistory.clicked_results.isnot(None)
            ).count()
            ctr = (searches_with_clicks / total_searches * 100) if total_searches > 0 else 0
            
            return {
                "total_searches": total_searches,
                "unique_users": unique_users,
                "search_types": dict(search_types),
                "zero_result_rate": round(zero_result_rate, 2),
                "click_through_rate": round(ctr, 2),
                "time_window_hours": int(time_window.total_seconds() / 3600)
            }


class SavedSearchRepository(PostgresRepository[SavedSearch]):
    """Repository for saved searches"""
    
    def __init__(self, db_session_factory):
        super().__init__(db_session_factory, SavedSearch)
        
    def create(self, search_data: SavedSearchCreate, user_id: str,
              tenant_id: str) -> SavedSearch:
        """Create a saved search"""
        with self.get_session() as session:
            saved_search = SavedSearch(
                id=uuid.uuid4(),
                user_id=user_id,
                tenant_id=tenant_id,
                name=search_data.name,
                query=search_data.query,
                filters=search_data.filters or {},
                search_type=search_data.search_type,
                is_public=search_data.is_public,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
            
            session.add(saved_search)
            session.commit()
            session.refresh(saved_search)
            
            return saved_search
            
    def get_user_saved_searches(self, user_id: str, tenant_id: str) -> List[SavedSearch]:
        """Get user's saved searches"""
        with self.get_session() as session:
            return session.query(SavedSearch).filter(
                and_(
                    SavedSearch.user_id == user_id,
                    SavedSearch.tenant_id == tenant_id
                )
            ).order_by(SavedSearch.created_at.desc()).all()
            
    def get_public_saved_searches(self, tenant_id: str,
                                 limit: int = 50) -> List[SavedSearch]:
        """Get public saved searches"""
        with self.get_session() as session:
            return session.query(SavedSearch).filter(
                and_(
                    SavedSearch.tenant_id == tenant_id,
                    SavedSearch.is_public == True
                )
            ).order_by(SavedSearch.created_at.desc()).limit(limit).all()


class SearchAlertRepository(PostgresRepository[SearchAlert]):
    """Repository for search alerts"""
    
    def __init__(self, db_session_factory):
        super().__init__(db_session_factory, SearchAlert)
        
    def create(self, alert_data: SearchAlertCreate, user_id: str,
              tenant_id: str) -> SearchAlert:
        """Create a search alert"""
        with self.get_session() as session:
            alert = SearchAlert(
                id=uuid.uuid4(),
                user_id=user_id,
                tenant_id=tenant_id,
                name=alert_data.name,
                query=alert_data.query,
                filters=alert_data.filters or {},
                frequency=alert_data.frequency,
                is_active=True,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
            
            session.add(alert)
            session.commit()
            session.refresh(alert)
            
            return alert
            
    def get_active_alerts(self, frequency: str) -> List[SearchAlert]:
        """Get active alerts for given frequency"""
        with self.get_session() as session:
            return session.query(SearchAlert).filter(
                and_(
                    SearchAlert.is_active == True,
                    SearchAlert.frequency == frequency
                )
            ).all()
            
    def update_last_run(self, alert_id: uuid.UUID, results_count: int):
        """Update alert last run time"""
        with self.get_session() as session:
            alert = session.query(SearchAlert).filter(
                SearchAlert.id == alert_id
            ).first()
            
            if alert:
                alert.last_run_at = datetime.utcnow()
                alert.last_results_count = results_count
                session.commit() 