from decimal import Decimal
from typing import Dict, List, Optional
from datetime import datetime, timedelta
from enum import Enum
import asyncio

from app.models.governance import MarketProposal, ProposalStatus, VoteType
from app.integrations.verifiable_credentials import VCServiceClient
from app.integrations.graph_intelligence import GraphIntelligenceClient

class ProposalStatus(Enum):
    DRAFT = "draft"
    PENDING_REVIEW = "pending_review"
    VOTING = "voting"
    APPROVED = "approved"
    REJECTED = "rejected"
    IMPLEMENTED = "implemented"
    CANCELLED = "cancelled"

class MarketCreationDAO:
    """
    DAO governance for derivative market creation
    Ensures quality and prevents spam markets
    """
    
    def __init__(
        self,
        graph_client: GraphIntelligenceClient,
        vc_client: VCServiceClient,
        ignite_client,
        pulsar_client
    ):
        self.graph = graph_client
        self.vc = vc_client
        self.ignite = ignite_client
        self.pulsar = pulsar_client
        
        # DAO parameters
        self.config = {
            "proposal_threshold": Decimal("10000"),  # Platform tokens needed to propose
            "review_committee_size": 5,  # Technical review committee
            "voting_period": timedelta(days=7),  # 7 days for voting
            "quorum_percentage": Decimal("0.1"),  # 10% of tokens must vote
            "approval_threshold": Decimal("0.66"),  # 66% approval needed
            "implementation_delay": timedelta(days=2),  # Timelock
            "proposal_deposit": Decimal("1000"),  # Refundable deposit
        }
        
    async def submit_market_proposal(
        self,
        proposer_id: str,
        market_spec: Dict
    ) -> MarketProposal:
        """
        Submit a new derivative market proposal
        """
        # Check proposer eligibility
        proposer_balance = await self._get_token_balance(proposer_id)
        if proposer_balance < self.config["proposal_threshold"]:
            raise ValueError(
                f"Insufficient tokens: {proposer_balance} < {self.config['proposal_threshold']}"
            )
        
        # Validate market specification
        validation_result = await self._validate_market_spec(market_spec)
        if not validation_result["valid"]:
            raise ValueError(f"Invalid market spec: {validation_result['errors']}")
        
        # Check for duplicate markets
        if await self._check_duplicate_market(market_spec):
            raise ValueError("Similar market already exists or proposed")
        
        # Lock proposal deposit
        await self._lock_proposal_deposit(proposer_id, self.config["proposal_deposit"])
        
        # Create proposal
        proposal = MarketProposal(
            id=self._generate_proposal_id(),
            proposer_id=proposer_id,
            market_spec=market_spec,
            status=ProposalStatus.DRAFT,
            created_at=datetime.utcnow(),
            
            # Enhanced metadata
            risk_assessment=await self._assess_market_risk(market_spec),
            oracle_requirements=self._determine_oracle_needs(market_spec),
            estimated_liquidity=await self._estimate_initial_liquidity(market_spec),
            
            # Voting data
            votes_for=Decimal("0"),
            votes_against=Decimal("0"),
            votes_abstain=Decimal("0"),
            
            # Review data
            technical_review=None,
            committee_feedback=[],
        )
        
        # Store proposal
        await self._store_proposal(proposal)
        
        # Emit event
        await self._emit_proposal_event(proposal, "created")
        
        # Auto-submit for review if complete
        if self._is_proposal_complete(proposal):
            await self._submit_for_review(proposal)
        
        return proposal
    
    async def _validate_market_spec(self, market_spec: Dict) -> Dict:
        """
        Comprehensive validation of market specification
        """
        errors = []
        warnings = []
        
        # Required fields
        required_fields = [
            "underlying_asset",
            "market_type",  # perpetual, future, option, prediction
            "oracle_sources",
            "settlement_mechanism",
            "risk_parameters"
        ]
        
        for field in required_fields:
            if field not in market_spec:
                errors.append(f"Missing required field: {field}")
        
        # Validate market type specifics
        if market_spec.get("market_type") == "prediction":
            if "resolution_criteria" not in market_spec:
                errors.append("Prediction markets require resolution_criteria")
            if "expiry_date" not in market_spec:
                errors.append("Prediction markets require expiry_date")
        
        # Oracle validation
        if "oracle_sources" in market_spec:
            oracle_count = len(market_spec["oracle_sources"])
            if oracle_count < 3:
                warnings.append(f"Only {oracle_count} oracles specified, recommend 3+")
            
            # Check oracle reliability
            for oracle in market_spec["oracle_sources"]:
                reliability = await self._check_oracle_reliability(oracle)
                if reliability < 0.95:
                    warnings.append(f"Oracle {oracle} has low reliability: {reliability}")
        
        # Risk parameter validation
        if "risk_parameters" in market_spec:
            risk = market_spec["risk_parameters"]
            
            # Maximum leverage check
            max_leverage = risk.get("max_leverage", 1)
            if max_leverage > 100:
                warnings.append(f"Very high leverage ({max_leverage}x) increases risk")
            
            # Margin requirements
            initial_margin = risk.get("initial_margin_percent", 100)
            if initial_margin < 1:
                errors.append("Initial margin must be at least 1%")
        
        # Check for novel/exotic markets
        if market_spec.get("is_novel", False):
            # Require additional documentation
            if "whitepaper_url" not in market_spec:
                errors.append("Novel markets require a whitepaper")
            if "simulation_results" not in market_spec:
                warnings.append("Novel markets should include simulation results")
        
        return {
            "valid": len(errors) == 0,
            "errors": errors,
            "warnings": warnings,
            "risk_score": self._calculate_market_risk_score(market_spec, errors, warnings)
        }
    
    async def conduct_technical_review(
        self,
        proposal_id: str,
        reviewer_id: str
    ) -> Dict:
        """
        Technical committee review of proposal
        """
        proposal = await self._get_proposal(proposal_id)
        
        # Check if reviewer is on committee
        if not await self._is_committee_member(reviewer_id):
            # Check if they have required credentials
            required_vc = await self.vc.check_credential(
                reviewer_id,
                "DERIVATIVES_EXPERT"
            )
            if not required_vc:
                raise ValueError("Not authorized to review")
        
        # Conduct review
        review = {
            "reviewer_id": reviewer_id,
            "timestamp": datetime.utcnow(),
            "technical_feasibility": await self._assess_technical_feasibility(proposal),
            "oracle_assessment": await self._assess_oracle_design(proposal),
            "risk_assessment": await self._assess_risk_parameters(proposal),
            "economic_viability": await self._assess_economics(proposal),
            "recommendations": []
        }
        
        # Add specific recommendations
        if review["technical_feasibility"]["score"] < 0.8:
            review["recommendations"].append({
                "type": "technical",
                "description": "Improve oracle redundancy",
                "priority": "high"
            })
        
        # Store review
        await self._add_committee_review(proposal_id, review)
        
        # Check if enough reviews to proceed
        reviews = await self._get_committee_reviews(proposal_id)
        if len(reviews) >= self.config["review_committee_size"]:
            await self._complete_technical_review(proposal_id)
        
        return review
    
    async def vote_on_proposal(
        self,
        proposal_id: str,
        voter_id: str,
        vote: VoteType,
        delegation_proof: Optional[str] = None
    ) -> Dict:
        """
        Cast vote on market proposal
        """
        proposal = await self._get_proposal(proposal_id)
        
        # Check proposal is in voting phase
        if proposal.status != ProposalStatus.VOTING:
            raise ValueError(f"Proposal not in voting phase: {proposal.status}")
        
        # Check voting period
        if datetime.utcnow() > proposal.voting_end:
            raise ValueError("Voting period has ended")
        
        # Get voting power
        voting_power = await self._calculate_voting_power(
            voter_id,
            delegation_proof
        )
        
        if voting_power <= 0:
            raise ValueError("No voting power")
        
        # Check if already voted
        if await self._has_voted(proposal_id, voter_id):
            raise ValueError("Already voted on this proposal")
        
        # Record vote
        vote_record = {
            "proposal_id": proposal_id,
            "voter_id": voter_id,
            "vote": vote,
            "voting_power": voting_power,
            "timestamp": datetime.utcnow(),
            "delegation_proof": delegation_proof
        }
        
        await self._record_vote(vote_record)
        
        # Update proposal vote counts
        if vote == VoteType.FOR:
            proposal.votes_for += voting_power
        elif vote == VoteType.AGAINST:
            proposal.votes_against += voting_power
        else:  # ABSTAIN
            proposal.votes_abstain += voting_power
        
        await self._update_proposal(proposal)
        
        # Check if quorum reached
        total_votes = proposal.votes_for + proposal.votes_against + proposal.votes_abstain
        total_supply = await self._get_total_token_supply()
        
        if total_votes >= total_supply * self.config["quorum_percentage"]:
            proposal.quorum_reached = True
            await self._update_proposal(proposal)
        
        return {
            "vote_recorded": True,
            "voting_power": voting_power,
            "current_results": {
                "for": proposal.votes_for,
                "against": proposal.votes_against,
                "abstain": proposal.votes_abstain,
                "quorum_reached": proposal.quorum_reached
            }
        }
    
    async def finalize_proposal(self, proposal_id: str) -> Dict:
        """
        Finalize voting and determine outcome
        """
        proposal = await self._get_proposal(proposal_id)
        
        # Check voting period ended
        if datetime.utcnow() <= proposal.voting_end:
            raise ValueError("Voting period not ended")
        
        # Check quorum
        total_votes = proposal.votes_for + proposal.votes_against + proposal.votes_abstain
        total_supply = await self._get_total_token_supply()
        quorum_met = total_votes >= total_supply * self.config["quorum_percentage"]
        
        if not quorum_met:
            proposal.status = ProposalStatus.REJECTED
            proposal.rejection_reason = "Quorum not met"
        else:
            # Calculate approval percentage (excluding abstains)
            total_decisive = proposal.votes_for + proposal.votes_against
            if total_decisive > 0:
                approval_rate = proposal.votes_for / total_decisive
            else:
                approval_rate = Decimal("0")
            
            if approval_rate >= self.config["approval_threshold"]:
                proposal.status = ProposalStatus.APPROVED
                proposal.implementation_date = (
                    datetime.utcnow() + self.config["implementation_delay"]
                )
            else:
                proposal.status = ProposalStatus.REJECTED
                proposal.rejection_reason = f"Approval rate {approval_rate} < {self.config['approval_threshold']}"
        
        # Return proposal deposit if not spam
        if proposal.status == ProposalStatus.APPROVED or total_votes > 0:
            await self._return_proposal_deposit(proposal.proposer_id, proposal.deposit)
        
        await self._update_proposal(proposal)
        
        # Emit finalization event
        await self._emit_proposal_event(proposal, "finalized")
        
        return {
            "proposal_id": proposal_id,
            "status": proposal.status,
            "quorum_met": quorum_met,
            "approval_rate": approval_rate if quorum_met else None,
            "implementation_date": proposal.implementation_date
        }
    
    async def implement_approved_market(self, proposal_id: str) -> Dict:
        """
        Implement approved market after timelock
        """
        proposal = await self._get_proposal(proposal_id)
        
        # Verify approved and timelock passed
        if proposal.status != ProposalStatus.APPROVED:
            raise ValueError("Proposal not approved")
        
        if datetime.utcnow() < proposal.implementation_date:
            raise ValueError("Implementation timelock not passed")
        
        # Create market through derivatives engine
        market_id = await self._create_derivative_market(proposal.market_spec)
        
        # Update proposal
        proposal.status = ProposalStatus.IMPLEMENTED
        proposal.market_id = market_id
        proposal.implemented_at = datetime.utcnow()
        
        await self._update_proposal(proposal)
        
        # Reward proposer with governance tokens
        await self._reward_successful_proposer(
            proposal.proposer_id,
            self._calculate_proposer_reward(proposal)
        )
        
        return {
            "market_id": market_id,
            "proposal_id": proposal_id,
            "implemented_at": proposal.implemented_at
        } 