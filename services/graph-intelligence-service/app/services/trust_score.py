from ..db.janusgraph_client import janusgraph_service

class TrustScoreService:
    def __init__(self):
        self.g = janusgraph_service.g

    def add_user_activity(self, user_id: str, activity_type: str, activity_id: str):
        """
        Adds a user activity to the graph.
        """
        if self.g:
            # Create user vertex if it doesn't exist
            user_vertex = self.g.V().has("user", "user_id", user_id).fold().coalesce(
                __.unfold(),
                __.addV("user").property("user_id", user_id)
            ).next()
            
            # Create activity vertex
            activity_vertex = self.g.addV(activity_type).property("activity_id", activity_id).next()
            
            # Create edge
            self.g.V(user_vertex).addE("participated_in").to(activity_vertex).iterate()

    def calculate_trust_score(self, user_id: str) -> float:
        """
        Calculates a trust score for a user based on their activities.
        For now, this is a simple count of their outgoing edges.
        """
        if self.g:
            score = self.g.V().has("user", "user_id", user_id).outE().count().next()
            return float(score)
        return 0.0

    def consume_events(self):
        """
        Consumes events from Pulsar and updates the graph.
        Placeholder for now.
        """
        # TODO: Implement Pulsar consumer for VC-related events
        pass

trust_score_service = TrustScoreService() 