from uuid import uuid4, UUID
from typing import List, Optional

from cassandra.cluster import Session
from ..db.cassandra import get_cassandra_session
from ..schemas.proposal import Proposal, ProposalCreate

class CRUDProposal:
    def get_session(self) -> Session:
        return get_cassandra_session()

    def create(self, *, obj_in: ProposalCreate, proposer: str) -> Proposal:
        session = self.get_session()
        proposal_id = uuid4()
        session.execute(
            """
            INSERT INTO proposals (id, title, description, proposer, targets, values, calldatas, is_onchain, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                "pending"
            ),
        )
        return self.get(proposal_id)

    def get(self, id: UUID) -> Optional[Proposal]:
        session = self.get_session()
        row = session.execute("SELECT * FROM proposals WHERE id = %s", (id,)).one()
        if row:
            return Proposal(**row)
        return None

    def get_multi(self) -> List[Proposal]:
        session = self.get_session()
        rows = session.execute("SELECT * FROM proposals")
        return [Proposal(**row) for row in rows]

    def update_onchain_status(self, id: UUID, onchain_proposal_id: str) -> Optional[Proposal]:
        session = self.get_session()
        session.execute(
            """
            UPDATE proposals
            SET is_onchain = true, onchain_proposal_id = %s, status = 'on-chain'
            WHERE id = %s
            """,
            (onchain_proposal_id, id),
        )
        return self.get(id)

    def update_status_from_event(self, onchain_proposal_id: str, new_status: str) -> Optional[Proposal]:
        session = self.get_session()
        # This requires a secondary index on onchain_proposal_id
        # CREATE INDEX IF NOT EXISTS on proposals (onchain_proposal_id);
        row = session.execute("SELECT id FROM proposals WHERE onchain_proposal_id = %s ALLOW FILTERING", (onchain_proposal_id,)).one()
        if row:
            proposal_id = row.id
            session.execute(
                "UPDATE proposals SET status = %s WHERE id = %s",
                (new_status, proposal_id)
            )
            return self.get(proposal_id)
        return None

    def get_voters(self, id: UUID) -> List[str]:
        """
        Retrieves a list of user IDs who voted 'for' a specific proposal.
        This is a placeholder assuming a `proposal_votes` table exists.
        """
        # In a real implementation, you would query a 'proposal_votes' table
        # SELECT voter_id FROM proposal_votes WHERE proposal_id = %s AND vote = 'for'
        # For this implementation, we'll return a mock list.
        print(f"Fetching voters for proposal {id}. (Mock implementation)")
        return ["user_id_1", "user_id_2", "user_id_3"]


proposal = CRUDProposal() 