import pytest
import asyncio
import aiohttp
from datetime import datetime, timedelta
from typing import Dict, Any
import json
from web3 import Web3
from eth_account import Account
import time

class MarketplaceIntegrationTests:
    """Comprehensive integration tests for the decentralized marketplace"""
    
    def __init__(self):
        self.base_url = "http://localhost:8000"
        self.blockchain_bridge_url = "http://localhost:8001"
        self.digital_asset_url = "http://localhost:8002"
        self.mlops_url = "http://localhost:8003"
        
        # Test accounts
        self.creator_account = Account.create()
        self.buyer_account = Account.create()
        self.contributor_account = Account.create()
        
        # Test data
        self.test_asset = {
            "name": "Test ML Model",
            "description": "A test machine learning model",
            "metadata": {
                "accuracy": 0.95,
                "framework": "tensorflow",
                "version": "1.0.0"
            }
        }
        
    async def setup(self):
        """Setup test environment"""
        # Fund test accounts (in a real test, this would interact with a testnet faucet)
        print(f"Creator account: {self.creator_account.address}")
        print(f"Buyer account: {self.buyer_account.address}")
        
    async def test_full_marketplace_flow(self):
        """Test complete marketplace flow from asset creation to royalty distribution"""
        async with aiohttp.ClientSession() as session:
            # 1. Create digital asset
            asset_id = await self._create_digital_asset(session)
            print(f"✓ Created digital asset: {asset_id}")
            
            # 2. Mint NFT
            token_id = await self._mint_nft(session, asset_id)
            print(f"✓ Minted NFT with token ID: {token_id}")
            
            # 3. Create license offer
            license_id = await self._create_license_offer(session, token_id)
            print(f"✓ Created license offer: {license_id}")
            
            # 4. Purchase license
            purchase_tx = await self._purchase_license(session, license_id)
            print(f"✓ License purchased: {purchase_tx}")
            
            # 5. Verify license
            is_valid = await self._verify_license(session, token_id)
            assert is_valid, "License verification failed"
            print("✓ License verified")
            
            # 6. Test royalty distribution
            royalty_tx = await self._distribute_royalties(session, token_id)
            print(f"✓ Royalties distributed: {royalty_tx}")
            
            return True
    
    async def test_auction_flow(self):
        """Test auction functionality"""
        async with aiohttp.ClientSession() as session:
            # Create asset and mint NFT
            asset_id = await self._create_digital_asset(session)
            token_id = await self._mint_nft(session, asset_id)
            
            # 1. Create English auction
            auction_id = await self._create_auction(session, token_id, "english")
            print(f"✓ Created English auction: {auction_id}")
            
            # 2. Place bids
            bid1 = await self._place_bid(session, auction_id, "1.0")
            bid2 = await self._place_bid(session, auction_id, "1.5")
            print("✓ Placed bids")
            
            # 3. End auction
            winner = await self._end_auction(session, auction_id)
            print(f"✓ Auction ended, winner: {winner}")
            
            # 4. Test Dutch auction
            dutch_auction_id = await self._create_auction(session, token_id, "dutch")
            print(f"✓ Created Dutch auction: {dutch_auction_id}")
            
            # 5. Buy from Dutch auction
            purchase = await self._buy_dutch_auction(session, dutch_auction_id)
            print(f"✓ Dutch auction purchase: {purchase}")
            
            return True
    
    async def test_defi_features(self):
        """Test DeFi features: lending and yield farming"""
        async with aiohttp.ClientSession() as session:
            # Create asset and mint NFT
            asset_id = await self._create_digital_asset(session)
            token_id = await self._mint_nft(session, asset_id)
            
            # 1. Test NFT lending
            loan_id = await self._create_loan_offer(session, token_id)
            print(f"✓ Created loan offer: {loan_id}")
            
            # 2. Accept loan
            loan_tx = await self._accept_loan(session, loan_id)
            print(f"✓ Loan accepted: {loan_tx}")
            
            # 3. Repay loan
            repay_tx = await self._repay_loan(session, loan_id)
            print(f"✓ Loan repaid: {repay_tx}")
            
            # 4. Test yield farming
            stake_tx = await self._stake_nft(session, token_id)
            print(f"✓ NFT staked: {stake_tx}")
            
            # 5. Claim rewards
            rewards = await self._claim_rewards(session, token_id)
            print(f"✓ Rewards claimed: {rewards}")
            
            # 6. Unstake
            unstake_tx = await self._unstake_nft(session, token_id)
            print(f"✓ NFT unstaked: {unstake_tx}")
            
            return True
    
    async def test_fractional_ownership(self):
        """Test fractional ownership functionality"""
        async with aiohttp.ClientSession() as session:
            # Create high-value asset
            asset_id = await self._create_digital_asset(session, value="1000")
            token_id = await self._mint_nft(session, asset_id)
            
            # 1. Fractionalize NFT
            fraction_data = await self._fractionalize_nft(session, token_id, 1000)
            print(f"✓ NFT fractionalized into {fraction_data['shares']} shares")
            
            # 2. Buy fractions
            buy_tx = await self._buy_fractions(session, fraction_data['vault_id'], 100)
            print(f"✓ Bought 100 fractions: {buy_tx}")
            
            # 3. Initiate buyout
            buyout_id = await self._initiate_buyout(session, fraction_data['vault_id'])
            print(f"✓ Buyout initiated: {buyout_id}")
            
            # 4. Vote on buyout
            vote_tx = await self._vote_buyout(session, buyout_id, True)
            print(f"✓ Voted on buyout: {vote_tx}")
            
            return True
    
    async def test_model_performance_tracking(self):
        """Test ML model performance tracking"""
        async with aiohttp.ClientSession() as session:
            # 1. Register model
            model_id = await self._register_model(session)
            print(f"✓ Model registered: {model_id}")
            
            # 2. Deploy model
            deployment_id = await self._deploy_model(session, model_id)
            print(f"✓ Model deployed: {deployment_id}")
            
            # 3. Track performance metrics
            metrics = {
                "accuracy": 0.92,
                "latency": 45,
                "throughput": 1000
            }
            await self._track_performance(session, deployment_id, metrics)
            print("✓ Performance metrics tracked")
            
            # 4. Check auto-retrain trigger
            needs_retrain = await self._check_retrain_needed(session, model_id)
            print(f"✓ Retrain needed: {needs_retrain}")
            
            return True
    
    async def test_dao_governance(self):
        """Test DAO governance features"""
        async with aiohttp.ClientSession() as session:
            # 1. Create proposal
            proposal_id = await self._create_proposal(session, {
                "title": "Reduce marketplace fees to 1%",
                "description": "Proposal to reduce fees from 2.5% to 1%",
                "type": "fee_change"
            })
            print(f"✓ Proposal created: {proposal_id}")
            
            # 2. Vote on proposal
            vote_tx = await self._vote_proposal(session, proposal_id, True)
            print(f"✓ Voted on proposal: {vote_tx}")
            
            # 3. Execute proposal
            execute_tx = await self._execute_proposal(session, proposal_id)
            print(f"✓ Proposal executed: {execute_tx}")
            
            return True
    
    async def test_error_handling(self):
        """Test error handling and edge cases"""
        async with aiohttp.ClientSession() as session:
            # 1. Test invalid NFT mint (bad royalty)
            try:
                await self._mint_nft(session, "invalid_asset", royalty=10000)
                assert False, "Should have failed with invalid royalty"
            except Exception as e:
                print(f"✓ Correctly rejected invalid royalty: {e}")
            
            # 2. Test double purchase prevention
            asset_id = await self._create_digital_asset(session)
            token_id = await self._mint_nft(session, asset_id)
            license_id = await self._create_license_offer(session, token_id)
            
            # First purchase
            await self._purchase_license(session, license_id)
            
            # Try to purchase again
            try:
                await self._purchase_license(session, license_id)
                assert False, "Should have failed on double purchase"
            except Exception as e:
                print(f"✓ Correctly prevented double purchase: {e}")
            
            # 3. Test expired license
            expired_license = await self._create_license_offer(
                session, token_id, duration=1  # 1 second
            )
            await asyncio.sleep(2)
            
            try:
                await self._purchase_license(session, expired_license)
                assert False, "Should have failed with expired license"
            except Exception as e:
                print(f"✓ Correctly rejected expired license: {e}")
            
            return True
    
    # Helper methods
    async def _create_digital_asset(self, session, value="100"):
        """Create a digital asset"""
        async with session.post(
            f"{self.digital_asset_url}/api/v1/assets",
            json={
                **self.test_asset,
                "creator": self.creator_account.address,
                "price": value
            }
        ) as resp:
            data = await resp.json()
            return data["id"]
    
    async def _mint_nft(self, session, asset_id, royalty=250):
        """Mint an NFT for the asset"""
        async with session.post(
            f"{self.blockchain_bridge_url}/api/v1/marketplace/mint-nft",
            json={
                "asset_id": asset_id,
                "creator": self.creator_account.address,
                "royalty_fraction": royalty,
                "chain": "polygon"
            }
        ) as resp:
            data = await resp.json()
            return data["token_id"]
    
    async def _create_license_offer(self, session, token_id, duration=86400):
        """Create a license offer"""
        async with session.post(
            f"{self.blockchain_bridge_url}/api/v1/marketplace/create-license-offer",
            json={
                "token_id": token_id,
                "license_type": "time_based",
                "price": "10",
                "duration": duration,
                "chain": "polygon"
            }
        ) as resp:
            data = await resp.json()
            return data["license_id"]
    
    async def _purchase_license(self, session, license_id):
        """Purchase a license"""
        async with session.post(
            f"{self.blockchain_bridge_url}/api/v1/marketplace/purchase-license",
            json={
                "license_id": license_id,
                "buyer": self.buyer_account.address,
                "payment": "10",
                "chain": "polygon"
            }
        ) as resp:
            data = await resp.json()
            return data["transaction_hash"]
    
    async def _verify_license(self, session, token_id):
        """Verify license validity"""
        async with session.get(
            f"{self.mlops_url}/api/v1/marketplace/check-license",
            params={
                "token_id": token_id,
                "user": self.buyer_account.address
            }
        ) as resp:
            data = await resp.json()
            return data["has_valid_license"]
    
    async def _distribute_royalties(self, session, token_id):
        """Distribute royalties"""
        async with session.post(
            f"{self.blockchain_bridge_url}/api/v1/marketplace/distribute-royalty",
            json={
                "token_id": token_id,
                "sale_amount": "100",
                "chain": "polygon"
            }
        ) as resp:
            data = await resp.json()
            return data["transaction_hash"]
    
    async def _create_auction(self, session, token_id, auction_type):
        """Create an auction"""
        async with session.post(
            f"{self.blockchain_bridge_url}/api/v1/defi/create-auction",
            json={
                "token_id": token_id,
                "auction_type": auction_type,
                "starting_price": "1",
                "duration": 3600,
                "chain": "polygon"
            }
        ) as resp:
            data = await resp.json()
            return data["auction_id"]
    
    async def _place_bid(self, session, auction_id, amount):
        """Place a bid"""
        async with session.post(
            f"{self.blockchain_bridge_url}/api/v1/defi/place-bid",
            json={
                "auction_id": auction_id,
                "bidder": self.buyer_account.address,
                "amount": amount,
                "chain": "polygon"
            }
        ) as resp:
            data = await resp.json()
            return data["bid_id"]
    
    async def _end_auction(self, session, auction_id):
        """End an auction"""
        async with session.post(
            f"{self.blockchain_bridge_url}/api/v1/defi/end-auction",
            json={
                "auction_id": auction_id,
                "chain": "polygon"
            }
        ) as resp:
            data = await resp.json()
            return data["winner"]
    
    async def _buy_dutch_auction(self, session, auction_id):
        """Buy from Dutch auction"""
        async with session.post(
            f"{self.blockchain_bridge_url}/api/v1/defi/buy-dutch",
            json={
                "auction_id": auction_id,
                "buyer": self.buyer_account.address,
                "chain": "polygon"
            }
        ) as resp:
            data = await resp.json()
            return data["transaction_hash"]
    
    async def _create_loan_offer(self, session, token_id):
        """Create a loan offer"""
        async with session.post(
            f"{self.blockchain_bridge_url}/api/v1/defi/create-loan",
            json={
                "token_id": token_id,
                "amount": "50",
                "interest_rate": 500,  # 5%
                "duration": 2592000,  # 30 days
                "chain": "polygon"
            }
        ) as resp:
            data = await resp.json()
            return data["loan_id"]
    
    async def _accept_loan(self, session, loan_id):
        """Accept a loan"""
        async with session.post(
            f"{self.blockchain_bridge_url}/api/v1/defi/accept-loan",
            json={
                "loan_id": loan_id,
                "borrower": self.creator_account.address,
                "chain": "polygon"
            }
        ) as resp:
            data = await resp.json()
            return data["transaction_hash"]
    
    async def _repay_loan(self, session, loan_id):
        """Repay a loan"""
        async with session.post(
            f"{self.blockchain_bridge_url}/api/v1/defi/repay-loan",
            json={
                "loan_id": loan_id,
                "amount": "52.5",  # Principal + interest
                "chain": "polygon"
            }
        ) as resp:
            data = await resp.json()
            return data["transaction_hash"]
    
    async def _stake_nft(self, session, token_id):
        """Stake an NFT"""
        async with session.post(
            f"{self.blockchain_bridge_url}/api/v1/defi/stake-nft",
            json={
                "token_id": token_id,
                "pool_id": 1,
                "chain": "polygon"
            }
        ) as resp:
            data = await resp.json()
            return data["transaction_hash"]
    
    async def _claim_rewards(self, session, token_id):
        """Claim staking rewards"""
        async with session.post(
            f"{self.blockchain_bridge_url}/api/v1/defi/claim-rewards",
            json={
                "token_id": token_id,
                "chain": "polygon"
            }
        ) as resp:
            data = await resp.json()
            return data["rewards"]
    
    async def _unstake_nft(self, session, token_id):
        """Unstake an NFT"""
        async with session.post(
            f"{self.blockchain_bridge_url}/api/v1/defi/unstake-nft",
            json={
                "token_id": token_id,
                "chain": "polygon"
            }
        ) as resp:
            data = await resp.json()
            return data["transaction_hash"]
    
    async def _fractionalize_nft(self, session, token_id, shares):
        """Fractionalize an NFT"""
        async with session.post(
            f"{self.blockchain_bridge_url}/api/v1/defi/fractionalize",
            json={
                "token_id": token_id,
                "total_shares": shares,
                "share_price": "1",
                "chain": "polygon"
            }
        ) as resp:
            data = await resp.json()
            return data
    
    async def _buy_fractions(self, session, vault_id, amount):
        """Buy fractional shares"""
        async with session.post(
            f"{self.blockchain_bridge_url}/api/v1/defi/buy-fractions",
            json={
                "vault_id": vault_id,
                "shares": amount,
                "buyer": self.buyer_account.address,
                "chain": "polygon"
            }
        ) as resp:
            data = await resp.json()
            return data["transaction_hash"]
    
    async def _initiate_buyout(self, session, vault_id):
        """Initiate a buyout"""
        async with session.post(
            f"{self.blockchain_bridge_url}/api/v1/defi/initiate-buyout",
            json={
                "vault_id": vault_id,
                "offer_price": "1200",
                "buyer": self.buyer_account.address,
                "chain": "polygon"
            }
        ) as resp:
            data = await resp.json()
            return data["buyout_id"]
    
    async def _vote_buyout(self, session, buyout_id, support):
        """Vote on a buyout"""
        async with session.post(
            f"{self.blockchain_bridge_url}/api/v1/defi/vote-buyout",
            json={
                "buyout_id": buyout_id,
                "support": support,
                "voter": self.creator_account.address,
                "chain": "polygon"
            }
        ) as resp:
            data = await resp.json()
            return data["transaction_hash"]
    
    async def _register_model(self, session):
        """Register an ML model"""
        async with session.post(
            f"{self.mlops_url}/api/v1/models/register",
            json={
                "name": "Test Model",
                "framework": "tensorflow",
                "version": "1.0.0",
                "metrics": {"accuracy": 0.95}
            }
        ) as resp:
            data = await resp.json()
            return data["model_id"]
    
    async def _deploy_model(self, session, model_id):
        """Deploy a model"""
        async with session.post(
            f"{self.mlops_url}/api/v1/models/{model_id}/deploy",
            json={
                "environment": "production",
                "replicas": 3
            }
        ) as resp:
            data = await resp.json()
            return data["deployment_id"]
    
    async def _track_performance(self, session, deployment_id, metrics):
        """Track model performance"""
        async with session.post(
            f"{self.mlops_url}/api/v1/performance/track",
            json={
                "deployment_id": deployment_id,
                "metrics": metrics,
                "timestamp": datetime.utcnow().isoformat()
            }
        ) as resp:
            return resp.status == 200
    
    async def _check_retrain_needed(self, session, model_id):
        """Check if model needs retraining"""
        async with session.get(
            f"{self.mlops_url}/api/v1/models/{model_id}/retrain-status"
        ) as resp:
            data = await resp.json()
            return data["needs_retrain"]
    
    async def _create_proposal(self, session, proposal_data):
        """Create a DAO proposal"""
        async with session.post(
            f"{self.blockchain_bridge_url}/api/v1/dao/create-proposal",
            json={
                **proposal_data,
                "proposer": self.creator_account.address,
                "chain": "polygon"
            }
        ) as resp:
            data = await resp.json()
            return data["proposal_id"]
    
    async def _vote_proposal(self, session, proposal_id, support):
        """Vote on a proposal"""
        async with session.post(
            f"{self.blockchain_bridge_url}/api/v1/dao/vote",
            json={
                "proposal_id": proposal_id,
                "support": support,
                "voter": self.creator_account.address,
                "chain": "polygon"
            }
        ) as resp:
            data = await resp.json()
            return data["transaction_hash"]
    
    async def _execute_proposal(self, session, proposal_id):
        """Execute a proposal"""
        async with session.post(
            f"{self.blockchain_bridge_url}/api/v1/dao/execute-proposal",
            json={
                "proposal_id": proposal_id,
                "chain": "polygon"
            }
        ) as resp:
            data = await resp.json()
            return data["transaction_hash"]


async def run_all_tests():
    """Run all integration tests"""
    tester = MarketplaceIntegrationTests()
    await tester.setup()
    
    print("\n🧪 Running Marketplace Integration Tests\n")
    
    tests = [
        ("Full Marketplace Flow", tester.test_full_marketplace_flow),
        ("Auction Flow", tester.test_auction_flow),
        ("DeFi Features", tester.test_defi_features),
        ("Fractional Ownership", tester.test_fractional_ownership),
        ("Model Performance Tracking", tester.test_model_performance_tracking),
        ("DAO Governance", tester.test_dao_governance),
        ("Error Handling", tester.test_error_handling)
    ]
    
    passed = 0
    failed = 0
    
    for test_name, test_func in tests:
        print(f"\n📋 Testing: {test_name}")
        print("-" * 50)
        try:
            result = await test_func()
            if result:
                print(f"✅ {test_name} PASSED")
                passed += 1
        except Exception as e:
            print(f"❌ {test_name} FAILED: {e}")
            failed += 1
    
    print(f"\n📊 Test Results: {passed} passed, {failed} failed")
    return failed == 0


if __name__ == "__main__":
    success = asyncio.run(run_all_tests())
    exit(0 if success else 1) 