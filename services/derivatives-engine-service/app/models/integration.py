from enum import Enum
from typing import Optional, Dict, Any, List
from decimal import Decimal
from datetime import datetime
from dataclasses import dataclass, field
import uuid


class AssetType(Enum):
    """Types of assets that can have derivatives"""
    NFT = "nft"
    DIGITAL_ASSET = "digital_asset"
    COMPUTE_RESOURCE = "compute_resource"
    AI_MODEL = "ai_model"
    DATASET = "dataset"
    REAL_WORLD_ASSET = "rwa"
    SYNTHETIC = "synthetic"
    INDEX = "index"


@dataclass
class AssetDerivativeLink:
    """
    Link between a digital asset and its derivative market
    """
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    
    # Asset details
    asset_id: str = ""
    asset_type: AssetType = AssetType.DIGITAL_ASSET
    asset_contract_address: Optional[str] = None
    asset_metadata_uri: Optional[str] = None
    
    # Derivative market
    market_id: str = ""
    market_symbol: str = ""
    
    # Valuation
    oracle_ids: List[str] = field(default_factory=list)
    price_feed_active: bool = True
    last_oracle_price: Decimal = Decimal("0")
    last_price_update: datetime = field(default_factory=datetime.utcnow)
    
    # Collateral parameters
    is_collateral_eligible: bool = False
    collateral_tier: Optional[str] = None
    max_ltv: Decimal = Decimal("0.5")  # 50% default
    
    # Risk parameters
    volatility_30d: Decimal = Decimal("0")
    liquidity_score: Decimal = Decimal("0")  # 0-100
    risk_rating: str = "medium"  # low, medium, high
    
    # Usage metrics
    total_derivative_volume: Decimal = Decimal("0")
    active_positions: int = 0
    total_collateral_locked: Decimal = Decimal("0")
    
    # Status
    is_active: bool = True
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    
    # Metadata
    creator_id: str = ""
    creation_proposal_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def update_metrics(self, volume: Decimal, positions_delta: int, collateral_delta: Decimal):
        """Update usage metrics"""
        self.total_derivative_volume += volume
        self.active_positions += positions_delta
        self.total_collateral_locked += collateral_delta
        self.updated_at = datetime.utcnow()
        
    def update_price(self, new_price: Decimal, oracle_id: str):
        """Update price from oracle"""
        self.last_oracle_price = new_price
        self.last_price_update = datetime.utcnow()
        if oracle_id not in self.oracle_ids:
            self.oracle_ids.append(oracle_id)
            
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "id": self.id,
            "asset_id": self.asset_id,
            "asset_type": self.asset_type.value,
            "asset_contract_address": self.asset_contract_address,
            "asset_metadata_uri": self.asset_metadata_uri,
            "market_id": self.market_id,
            "market_symbol": self.market_symbol,
            "oracle_ids": self.oracle_ids,
            "price_feed_active": self.price_feed_active,
            "last_oracle_price": str(self.last_oracle_price),
            "last_price_update": self.last_price_update.isoformat(),
            "is_collateral_eligible": self.is_collateral_eligible,
            "collateral_tier": self.collateral_tier,
            "max_ltv": str(self.max_ltv),
            "volatility_30d": str(self.volatility_30d),
            "liquidity_score": str(self.liquidity_score),
            "risk_rating": self.risk_rating,
            "total_derivative_volume": str(self.total_derivative_volume),
            "active_positions": self.active_positions,
            "total_collateral_locked": str(self.total_collateral_locked),
            "is_active": self.is_active,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "creator_id": self.creator_id,
            "creation_proposal_id": self.creation_proposal_id,
            "metadata": self.metadata
        }


@dataclass
class CollateralAsset:
    """
    Asset approved for use as collateral
    """
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    
    # Asset identification
    asset_id: str = ""
    asset_symbol: str = ""
    asset_name: str = ""
    asset_type: AssetType = AssetType.DIGITAL_ASSET
    contract_address: Optional[str] = None
    decimals: int = 18
    
    # Collateral parameters
    collateral_tier: str = ""
    haircut: Decimal = Decimal("0.15")  # 15% haircut
    max_ltv: Decimal = Decimal("0.85")  # 85% max LTV
    liquidation_threshold: Decimal = Decimal("0.90")  # 90%
    liquidation_penalty: Decimal = Decimal("0.05")  # 5%
    
    # Caps and limits
    global_cap: Optional[Decimal] = None  # Total platform limit
    per_user_cap: Optional[Decimal] = None  # Per user limit
    current_total_deposited: Decimal = Decimal("0")
    
    # Oracle configuration
    primary_oracle_id: str = ""
    backup_oracle_ids: List[str] = field(default_factory=list)
    price_staleness_threshold: int = 300  # 5 minutes
    
    # Interest rates (for lending)
    base_borrow_rate: Decimal = Decimal("0.02")  # 2% APR
    base_supply_rate: Decimal = Decimal("0.015")  # 1.5% APR
    utilization_rate_model: str = "standard"  # standard, kinked
    
    # Status and governance
    is_active: bool = True
    is_borrowable: bool = True
    approval_proposal_id: Optional[str] = None
    approved_at: Optional[datetime] = None
    
    # Timestamps
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    
    # Metadata
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def can_deposit(self, amount: Decimal) -> bool:
        """Check if deposit is allowed"""
        if not self.is_active:
            return False
        if self.global_cap and self.current_total_deposited + amount > self.global_cap:
            return False
        return True
        
    def calculate_collateral_value(self, amount: Decimal) -> Decimal:
        """Calculate collateral value after haircut"""
        return amount * (Decimal("1") - self.haircut)
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "id": self.id,
            "asset_id": self.asset_id,
            "asset_symbol": self.asset_symbol,
            "asset_name": self.asset_name,
            "asset_type": self.asset_type.value,
            "contract_address": self.contract_address,
            "decimals": self.decimals,
            "collateral_tier": self.collateral_tier,
            "haircut": str(self.haircut),
            "max_ltv": str(self.max_ltv),
            "liquidation_threshold": str(self.liquidation_threshold),
            "liquidation_penalty": str(self.liquidation_penalty),
            "global_cap": str(self.global_cap) if self.global_cap else None,
            "per_user_cap": str(self.per_user_cap) if self.per_user_cap else None,
            "current_total_deposited": str(self.current_total_deposited),
            "primary_oracle_id": self.primary_oracle_id,
            "backup_oracle_ids": self.backup_oracle_ids,
            "price_staleness_threshold": self.price_staleness_threshold,
            "base_borrow_rate": str(self.base_borrow_rate),
            "base_supply_rate": str(self.base_supply_rate),
            "utilization_rate_model": self.utilization_rate_model,
            "is_active": self.is_active,
            "is_borrowable": self.is_borrowable,
            "approval_proposal_id": self.approval_proposal_id,
            "approved_at": self.approved_at.isoformat() if self.approved_at else None,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "metadata": self.metadata
        }


@dataclass
class MarketCreationRequest:
    """
    Request to create a new derivative market
    """
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    
    # Requester details
    requester_id: str = ""
    requester_type: str = "user"  # user, dao, system
    
    # Market basics
    market_type: str = ""  # perpetual, futures, options, etc.
    underlying_asset_id: str = ""
    underlying_asset_type: AssetType = AssetType.DIGITAL_ASSET
    
    # Market specifications
    symbol: str = ""  # e.g., "NFT-COLLECTION-PERP"
    name: str = ""
    description: str = ""
    
    # Contract parameters
    contract_specifications: Dict[str, Any] = field(default_factory=dict)
    
    # Risk parameters
    proposed_risk_params: Dict[str, Any] = field(default_factory=dict)
    
    # Oracle requirements
    required_oracles: List[str] = field(default_factory=list)
    price_methodology: str = ""  # floor_price, average_price, index
    
    # Liquidity commitment
    initial_liquidity_usd: Decimal = Decimal("0")
    liquidity_providers: List[Dict[str, Any]] = field(default_factory=list)
    
    # Governance
    requires_dao_approval: bool = True
    proposal_id: Optional[str] = None
    
    # Status
    status: str = "pending"  # pending, approved, rejected, deployed
    
    # Validation
    validation_checks: Dict[str, bool] = field(default_factory=dict)
    validation_errors: List[str] = field(default_factory=list)
    
    # Deployment
    deployed_market_id: Optional[str] = None
    deployment_tx_hash: Optional[str] = None
    deployed_at: Optional[datetime] = None
    
    # Timestamps
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    
    # Metadata
    supporting_documents: List[str] = field(default_factory=list)  # IPFS hashes
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def validate(self) -> bool:
        """Validate the market creation request"""
        self.validation_errors.clear()
        self.validation_checks.clear()
        
        # Check required fields
        if not self.symbol:
            self.validation_errors.append("Symbol is required")
        if not self.underlying_asset_id:
            self.validation_errors.append("Underlying asset is required")
        if not self.required_oracles:
            self.validation_errors.append("At least one oracle is required")
            
        # Check liquidity
        min_liquidity = Decimal("10000")  # $10k minimum
        if self.initial_liquidity_usd < min_liquidity:
            self.validation_errors.append(f"Minimum initial liquidity is ${min_liquidity}")
            
        # Check risk parameters
        if "max_leverage" in self.proposed_risk_params:
            max_leverage = Decimal(str(self.proposed_risk_params["max_leverage"]))
            if max_leverage > Decimal("100"):
                self.validation_errors.append("Maximum leverage cannot exceed 100x")
                
        self.validation_checks["has_required_fields"] = len(self.validation_errors) == 0
        self.validation_checks["has_sufficient_liquidity"] = self.initial_liquidity_usd >= min_liquidity
        self.validation_checks["has_valid_risk_params"] = "max_leverage" not in self.validation_errors
        
        self.updated_at = datetime.utcnow()
        return len(self.validation_errors) == 0
        
    def approve(self, proposal_id: str):
        """Approve the market creation request"""
        self.status = "approved"
        self.proposal_id = proposal_id
        self.updated_at = datetime.utcnow()
        
    def deploy(self, market_id: str, tx_hash: str):
        """Mark as deployed"""
        self.status = "deployed"
        self.deployed_market_id = market_id
        self.deployment_tx_hash = tx_hash
        self.deployed_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "id": self.id,
            "requester_id": self.requester_id,
            "requester_type": self.requester_type,
            "market_type": self.market_type,
            "underlying_asset_id": self.underlying_asset_id,
            "underlying_asset_type": self.underlying_asset_type.value,
            "symbol": self.symbol,
            "name": self.name,
            "description": self.description,
            "contract_specifications": self.contract_specifications,
            "proposed_risk_params": self.proposed_risk_params,
            "required_oracles": self.required_oracles,
            "price_methodology": self.price_methodology,
            "initial_liquidity_usd": str(self.initial_liquidity_usd),
            "liquidity_providers": self.liquidity_providers,
            "requires_dao_approval": self.requires_dao_approval,
            "proposal_id": self.proposal_id,
            "status": self.status,
            "validation_checks": self.validation_checks,
            "validation_errors": self.validation_errors,
            "deployed_market_id": self.deployed_market_id,
            "deployment_tx_hash": self.deployment_tx_hash,
            "deployed_at": self.deployed_at.isoformat() if self.deployed_at else None,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "supporting_documents": self.supporting_documents,
            "metadata": self.metadata
        } 