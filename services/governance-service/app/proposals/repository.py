from uuid import uuid4, UUID
from typing import List, Optional, Dict, Any
from cassandra.cluster import Session

from .db.cassandra import get_cassandra_session
from .schemas.proposal import Proposal, ProposalCreate
from platformq.shared.repository import AbstractRepository

class ProposalRepository(AbstractRepository[Proposal]):
    def __init__(self, db_session: Session):
        self.db_session = db_session

    def add(self, obj_in: ProposalCreate, proposer: str) -> Proposal:
        proposal_id = uuid4()
        self.db_session.execute(
            """
            INSERT INTO proposals (
                id, title, description, proposer, targets, values, 
                calldatas, is_onchain, status, is_cross_chain
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                proposal_id,
                obj_in.title,
                obj_in.description,
                proposer,
                obj_in.targets,
                obj_in.values,
                obj_in.calldatas,
                False,
                "pending",
                obj_in.is_cross_chain
            ),
        )
        return self.get(proposal_id)

    def get(self, id: UUID) -> Optional[Proposal]:
        row = self.db_session.execute("SELECT * FROM proposals WHERE id = %s", (id,)).one()
        if row:
            return Proposal(**row)
        return None

    def list(self) -> List[Proposal]:
        rows = self.db_session.execute("SELECT * FROM proposals")
        return [Proposal(**row) for row in rows]

    def update_onchain_status(self, id: UUID, onchain_proposal_id: str) -> Optional[Proposal]:
        self.db_session.execute(
            """
            UPDATE proposals
            SET is_onchain = true, onchain_proposal_id = %s, status = 'on-chain'
            WHERE id = %s
            """,
            (onchain_proposal_id, id),
        )
        return self.get(id)

    def update_cross_chain_status(self, id: UUID, cross_chain_details: Dict[str, Any]) -> Optional[Proposal]:
        self.db_session.execute(
            """
            UPDATE proposals
            SET cross_chain_details = %s, status = 'cross-chain-active'
            WHERE id = %s
            """,
            (cross_chain_details, id),
        )
        return self.get(id)

    def update_status_from_event(self, onchain_proposal_id: str, new_status: str) -> Optional[Proposal]:
        row = self.db_session.execute("SELECT id FROM proposals WHERE onchain_proposal_id = %s ALLOW FILTERING", (onchain_proposal_id,)).one()
        if row:
            proposal_id = row.id
            self.db_session.execute(
                "UPDATE proposals SET status = %s WHERE id = %s",
                (new_status, proposal_id)
            )
            return self.get(proposal_id)
        return None

    def get_voters(self, id: UUID) -> List[str]:
        return ["user_id_1", "user_id_2", "user_id_3"]

    def delete(self, id: UUID) -> bool:
        self.db_session.execute("DELETE FROM proposals WHERE id = %s", (id,))
        return True 