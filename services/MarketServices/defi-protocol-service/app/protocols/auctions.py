"""
Auction Protocol Implementation

Handles Dutch and English auctions for NFTs and other digital assets.
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from decimal import Decimal
from enum import Enum

from platformq_blockchain_common import (
    IBlockchainAdapter,
    Transaction,
    TransactionType,
    GasStrategy
)

from ..core.defi_manager import DeFiManager
from ..models.auction import AuctionType, AuctionStatus, Auction, Bid

logger = logging.getLogger(__name__)


class AuctionProtocol:
    """Manages on-chain auctions for digital assets"""
    
    def __init__(self, defi_manager: DeFiManager):
        self.defi_manager = defi_manager
        self._auction_contracts: Dict[str, Dict[str, str]] = {}  # chain -> type -> address
        self._active_auctions: Dict[str, Auction] = {}  # auction_id -> auction
        
    async def initialize(self):
        """Initialize auction protocol contracts"""
        logger.info("Initializing Auction Protocol")
        
        # Load auction contract addresses for each chain
        for chain_type in self.defi_manager.get_supported_chains():
            self._auction_contracts[chain_type.value] = {
                "dutch": await self._get_contract_address(chain_type, "DutchAuction"),
                "english": await self._get_contract_address(chain_type, "EnglishAuction")
            }
            
    async def shutdown(self):
        """Shutdown auction protocol"""
        logger.info("Shutting down Auction Protocol")
        self._active_auctions.clear()
        
    async def create_dutch_auction(
        self,
        chain: str,
        token_address: str,
        token_id: int,
        start_price: Decimal,
        end_price: Decimal,
        duration: int,
        seller: str,
        price_decrement: Optional[Decimal] = None
    ) -> Dict[str, Any]:
        """
        Create a Dutch auction where price decreases over time.
        
        Args:
            chain: Blockchain identifier
            token_address: NFT contract address
            token_id: NFT token ID
            start_price: Starting price
            end_price: Minimum/reserve price
            duration: Auction duration in seconds
            seller: Seller's wallet address
            price_decrement: Price decrease per time unit (optional)
            
        Returns:
            Transaction result with auction ID
        """
        try:
            async with self.defi_manager.get_adapter(chain) as adapter:
                # Validate inputs
                if start_price <= end_price:
                    raise ValueError("Start price must be greater than end price")
                    
                if duration <= 0:
                    raise ValueError("Duration must be positive")
                    
                # Calculate price decrement if not provided
                if price_decrement is None:
                    # Linear decrease over duration
                    price_decrement = (start_price - end_price) / Decimal(duration)
                
                # Prepare contract call
                contract_address = self._auction_contracts[chain]["dutch"]
                
                # Encode auction parameters
                params = [
                    token_address,
                    token_id,
                    int(start_price * 10**18),  # Convert to wei
                    int(end_price * 10**18),
                    duration,
                    int(price_decrement * 10**18)
                ]
                
                # Create transaction
                tx = await self._prepare_transaction(
                    adapter,
                    seller,
                    contract_address,
                    "createAuction",
                    params
                )
                
                # Send transaction
                result = await adapter.send_transaction(tx)
                
                # Extract auction ID from events
                auction_id = await self._extract_auction_id(adapter, result.transaction_hash)
                
                # Store auction info
                auction = Auction(
                    id=auction_id,
                    type=AuctionType.DUTCH,
                    chain=chain,
                    token_address=token_address,
                    token_id=token_id,
                    seller=seller,
                    start_price=start_price,
                    current_price=start_price,
                    end_price=end_price,
                    start_time=datetime.utcnow(),
                    end_time=datetime.utcnow() + timedelta(seconds=duration),
                    status=AuctionStatus.ACTIVE,
                    metadata={
                        "price_decrement": str(price_decrement),
                        "tx_hash": result.transaction_hash
                    }
                )
                self._active_auctions[auction_id] = auction
                
                logger.info(f"Created Dutch auction {auction_id} on {chain}")
                
                return {
                    "auction_id": auction_id,
                    "tx_hash": result.transaction_hash,
                    "gas_used": str(result.gas_used),
                    "status": "created"
                }
                
        except Exception as e:
            logger.error(f"Error creating Dutch auction: {e}")
            raise
            
    async def create_english_auction(
        self,
        chain: str,
        token_address: str,
        token_id: int,
        start_price: Decimal,
        min_bid_increment: Decimal,
        duration: int,
        seller: str,
        reserve_price: Optional[Decimal] = None
    ) -> Dict[str, Any]:
        """
        Create an English auction where price increases through bidding.
        
        Args:
            chain: Blockchain identifier
            token_address: NFT contract address
            token_id: NFT token ID
            start_price: Starting bid price
            min_bid_increment: Minimum bid increment
            duration: Auction duration in seconds
            seller: Seller's wallet address
            reserve_price: Optional reserve price
            
        Returns:
            Transaction result with auction ID
        """
        try:
            async with self.defi_manager.get_adapter(chain) as adapter:
                # Validate inputs
                if start_price <= 0:
                    raise ValueError("Start price must be positive")
                    
                if min_bid_increment <= 0:
                    raise ValueError("Minimum bid increment must be positive")
                    
                # Prepare contract call
                contract_address = self._auction_contracts[chain]["english"]
                
                # Encode auction parameters
                params = [
                    token_address,
                    token_id,
                    int(start_price * 10**18),
                    int(min_bid_increment * 10**18),
                    duration,
                    int(reserve_price * 10**18) if reserve_price else 0
                ]
                
                # Create transaction
                tx = await self._prepare_transaction(
                    adapter,
                    seller,
                    contract_address,
                    "createAuction",
                    params
                )
                
                # Send transaction
                result = await adapter.send_transaction(tx)
                
                # Extract auction ID
                auction_id = await self._extract_auction_id(adapter, result.transaction_hash)
                
                # Store auction info
                auction = Auction(
                    id=auction_id,
                    type=AuctionType.ENGLISH,
                    chain=chain,
                    token_address=token_address,
                    token_id=token_id,
                    seller=seller,
                    start_price=start_price,
                    current_price=start_price,
                    end_price=reserve_price,
                    start_time=datetime.utcnow(),
                    end_time=datetime.utcnow() + timedelta(seconds=duration),
                    status=AuctionStatus.ACTIVE,
                    metadata={
                        "min_bid_increment": str(min_bid_increment),
                        "highest_bidder": None,
                        "tx_hash": result.transaction_hash
                    }
                )
                self._active_auctions[auction_id] = auction
                
                logger.info(f"Created English auction {auction_id} on {chain}")
                
                return {
                    "auction_id": auction_id,
                    "tx_hash": result.transaction_hash,
                    "gas_used": str(result.gas_used),
                    "status": "created"
                }
                
        except Exception as e:
            logger.error(f"Error creating English auction: {e}")
            raise
            
    async def place_bid(
        self,
        chain: str,
        auction_id: str,
        bid_amount: Decimal,
        bidder: str
    ) -> Dict[str, Any]:
        """
        Place a bid on an English auction.
        
        Args:
            chain: Blockchain identifier
            auction_id: Auction identifier
            bid_amount: Bid amount
            bidder: Bidder's wallet address
            
        Returns:
            Transaction result
        """
        try:
            auction = self._active_auctions.get(auction_id)
            if not auction:
                raise ValueError(f"Auction {auction_id} not found")
                
            if auction.type != AuctionType.ENGLISH:
                raise ValueError("Can only bid on English auctions")
                
            if auction.status != AuctionStatus.ACTIVE:
                raise ValueError("Auction is not active")
                
            # Check minimum bid
            min_bid = auction.current_price + Decimal(auction.metadata["min_bid_increment"])
            if bid_amount < min_bid:
                raise ValueError(f"Bid must be at least {min_bid}")
                
            async with self.defi_manager.get_adapter(chain) as adapter:
                contract_address = self._auction_contracts[chain]["english"]
                
                # Prepare bid transaction
                params = [auction_id, int(bid_amount * 10**18)]
                
                tx = await self._prepare_transaction(
                    adapter,
                    bidder,
                    contract_address,
                    "placeBid",
                    params,
                    value=bid_amount  # Send ETH/native token with bid
                )
                
                # Send transaction
                result = await adapter.send_transaction(tx)
                
                # Update auction state
                auction.current_price = bid_amount
                auction.metadata["highest_bidder"] = bidder
                auction.bids.append(Bid(
                    bidder=bidder,
                    amount=bid_amount,
                    timestamp=datetime.utcnow(),
                    tx_hash=result.transaction_hash
                ))
                
                logger.info(f"Placed bid of {bid_amount} on auction {auction_id}")
                
                return {
                    "tx_hash": result.transaction_hash,
                    "gas_used": str(result.gas_used),
                    "status": "bid_placed",
                    "current_price": str(bid_amount)
                }
                
        except Exception as e:
            logger.error(f"Error placing bid: {e}")
            raise
            
    async def buy_dutch_auction(
        self,
        chain: str,
        auction_id: str,
        buyer: str
    ) -> Dict[str, Any]:
        """
        Buy from a Dutch auction at current price.
        
        Args:
            chain: Blockchain identifier
            auction_id: Auction identifier
            buyer: Buyer's wallet address
            
        Returns:
            Transaction result
        """
        try:
            auction = self._active_auctions.get(auction_id)
            if not auction:
                raise ValueError(f"Auction {auction_id} not found")
                
            if auction.type != AuctionType.DUTCH:
                raise ValueError("Can only buy from Dutch auctions")
                
            if auction.status != AuctionStatus.ACTIVE:
                raise ValueError("Auction is not active")
                
            # Calculate current price
            current_price = await self._calculate_dutch_auction_price(auction)
            
            async with self.defi_manager.get_adapter(chain) as adapter:
                contract_address = self._auction_contracts[chain]["dutch"]
                
                # Prepare buy transaction
                params = [auction_id]
                
                tx = await self._prepare_transaction(
                    adapter,
                    buyer,
                    contract_address,
                    "buy",
                    params,
                    value=current_price
                )
                
                # Send transaction
                result = await adapter.send_transaction(tx)
                
                # Update auction state
                auction.status = AuctionStatus.COMPLETED
                auction.metadata["buyer"] = buyer
                auction.metadata["final_price"] = str(current_price)
                
                logger.info(f"Dutch auction {auction_id} sold for {current_price}")
                
                return {
                    "tx_hash": result.transaction_hash,
                    "gas_used": str(result.gas_used),
                    "status": "purchased",
                    "price": str(current_price)
                }
                
        except Exception as e:
            logger.error(f"Error buying from Dutch auction: {e}")
            raise
            
    async def get_auction_details(
        self,
        chain: str,
        auction_id: str
    ) -> Dict[str, Any]:
        """Get details of an auction"""
        auction = self._active_auctions.get(auction_id)
        if not auction:
            # Try fetching from chain
            return await self._fetch_auction_from_chain(chain, auction_id)
            
        # Calculate current price for Dutch auctions
        if auction.type == AuctionType.DUTCH and auction.status == AuctionStatus.ACTIVE:
            auction.current_price = await self._calculate_dutch_auction_price(auction)
            
        return {
            "id": auction.id,
            "type": auction.type.value,
            "status": auction.status.value,
            "token_address": auction.token_address,
            "token_id": auction.token_id,
            "seller": auction.seller,
            "start_price": str(auction.start_price),
            "current_price": str(auction.current_price),
            "end_price": str(auction.end_price) if auction.end_price else None,
            "start_time": auction.start_time.isoformat(),
            "end_time": auction.end_time.isoformat(),
            "bids": [
                {
                    "bidder": bid.bidder,
                    "amount": str(bid.amount),
                    "timestamp": bid.timestamp.isoformat()
                }
                for bid in auction.bids
            ],
            "metadata": auction.metadata
        }
        
    async def _prepare_transaction(
        self,
        adapter: IBlockchainAdapter,
        from_address: str,
        to_address: str,
        method: str,
        params: List[Any],
        value: Decimal = Decimal("0")
    ) -> Transaction:
        """Prepare a transaction for auction contract interaction"""
        # This would encode the function call properly
        # For now, simplified version
        return Transaction(
            from_address=from_address,
            to_address=to_address,
            value=value,
            data=f"{method}({params})".encode(),  # Simplified
            type=TransactionType.CONTRACT_CALL,
            gas_strategy=GasStrategy.STANDARD
        )
        
    async def _extract_auction_id(
        self,
        adapter: IBlockchainAdapter,
        tx_hash: str
    ) -> str:
        """Extract auction ID from transaction events"""
        # This would parse the transaction logs
        # For now, return a mock ID
        return f"auction_{tx_hash[:8]}"
        
    async def _calculate_dutch_auction_price(
        self,
        auction: Auction
    ) -> Decimal:
        """Calculate current price for Dutch auction"""
        elapsed = (datetime.utcnow() - auction.start_time).total_seconds()
        duration = (auction.end_time - auction.start_time).total_seconds()
        
        if elapsed >= duration:
            return auction.end_price
            
        # Linear price decrease
        price_range = auction.start_price - auction.end_price
        price_decrease = price_range * Decimal(elapsed / duration)
        
        return auction.start_price - price_decrease
        
    async def _get_contract_address(
        self,
        chain_type,
        contract_name: str
    ) -> str:
        """Get deployed contract address for chain"""
        # This would fetch from configuration or registry
        # For now, return mock addresses
        return f"0x{hash(f'{chain_type.value}_{contract_name}') % 16**40:040x}"
        
    async def _fetch_auction_from_chain(
        self,
        chain: str,
        auction_id: str
    ) -> Dict[str, Any]:
        """Fetch auction details from blockchain"""
        # This would query the blockchain
        # For now, return empty
        return {
            "error": "Auction not found in cache",
            "auction_id": auction_id
        } 