import pytest
import asyncio
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import aiohttp
import json
import time
from typing import Dict, Any
from eth_account import Account
import os

class MarketplaceE2ETests:
    """End-to-end tests for the marketplace including UI and API interactions"""
    
    def __init__(self):
        self.frontend_url = "http://localhost:3000"
        self.api_base_url = "http://localhost:8000"
        self.driver = None
        
        # Test accounts
        self.test_accounts = {
            "creator": {
                "address": "0x1234567890123456789012345678901234567890",
                "private_key": "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
            },
            "buyer": {
                "address": "0x0987654321098765432109876543210987654321",
                "private_key": "0xfedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321"
            }
        }
    
    def setup(self):
        """Setup Selenium WebDriver"""
        chrome_options = Options()
        chrome_options.add_argument("--headless")  # Run in headless mode for CI
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        
        self.driver = webdriver.Chrome(options=chrome_options)
        self.driver.implicitly_wait(10)
    
    def teardown(self):
        """Cleanup"""
        if self.driver:
            self.driver.quit()
    
    async def test_marketplace_ui_flow(self):
        """Test complete marketplace flow through UI"""
        try:
            # 1. Navigate to marketplace
            self.driver.get(f"{self.frontend_url}/marketplace")
            wait = WebDriverWait(self.driver, 20)
            
            # 2. Connect wallet
            connect_button = wait.until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Connect Wallet')]"))
            )
            connect_button.click()
            
            # Select MetaMask (in test environment, this would be mocked)
            metamask_option = wait.until(
                EC.element_to_be_clickable((By.XPATH, "//div[contains(text(), 'MetaMask')]"))
            )
            metamask_option.click()
            
            # Wait for connection
            await asyncio.sleep(2)
            
            # 3. Create new asset listing
            create_button = wait.until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Create Listing')]"))
            )
            create_button.click()
            
            # Fill in asset details
            name_input = self.driver.find_element(By.ID, "asset-name")
            name_input.send_keys("Test ML Model E2E")
            
            description_input = self.driver.find_element(By.ID, "asset-description")
            description_input.send_keys("End-to-end test model with high accuracy")
            
            price_input = self.driver.find_element(By.ID, "asset-price")
            price_input.send_keys("10")
            
            # Select license type
            license_select = self.driver.find_element(By.ID, "license-type")
            license_select.send_keys("Time-based")
            
            # Set royalty
            royalty_input = self.driver.find_element(By.ID, "royalty-percentage")
            royalty_input.clear()
            royalty_input.send_keys("5")
            
            # Submit listing
            submit_button = self.driver.find_element(By.XPATH, "//button[@type='submit']")
            submit_button.click()
            
            # Wait for transaction confirmation
            success_message = wait.until(
                EC.presence_of_element_located((By.CLASS_NAME, "success-message"))
            )
            assert "successfully created" in success_message.text.lower()
            
            # 4. Browse marketplace
            browse_link = self.driver.find_element(By.XPATH, "//a[contains(text(), 'Browse')]")
            browse_link.click()
            
            # Search for our asset
            search_input = wait.until(
                EC.presence_of_element_located((By.ID, "search-assets"))
            )
            search_input.send_keys("Test ML Model E2E")
            
            # Click on asset card
            asset_card = wait.until(
                EC.element_to_be_clickable((By.XPATH, "//div[@class='asset-card'][1]"))
            )
            asset_card.click()
            
            # 5. Purchase license
            purchase_button = wait.until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Purchase License')]"))
            )
            purchase_button.click()
            
            # Confirm transaction
            confirm_button = wait.until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Confirm')]"))
            )
            confirm_button.click()
            
            # Wait for purchase confirmation
            purchase_success = wait.until(
                EC.presence_of_element_located((By.CLASS_NAME, "purchase-success"))
            )
            assert "successfully purchased" in purchase_success.text.lower()
            
            # 6. Check my licenses
            profile_link = self.driver.find_element(By.XPATH, "//a[contains(text(), 'My Profile')]")
            profile_link.click()
            
            licenses_tab = wait.until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'My Licenses')]"))
            )
            licenses_tab.click()
            
            # Verify license appears
            license_item = wait.until(
                EC.presence_of_element_located((By.XPATH, "//div[contains(text(), 'Test ML Model E2E')]"))
            )
            assert license_item is not None
            
            print("✅ UI flow test completed successfully")
            return True
            
        except Exception as e:
            # Take screenshot on failure
            self.driver.save_screenshot("test_failure.png")
            raise e
    
    async def test_auction_ui_flow(self):
        """Test auction functionality through UI"""
        try:
            # Navigate to auctions
            self.driver.get(f"{self.frontend_url}/marketplace/auctions")
            wait = WebDriverWait(self.driver, 20)
            
            # Create new auction
            create_auction_button = wait.until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Create Auction')]"))
            )
            create_auction_button.click()
            
            # Select asset
            asset_select = wait.until(
                EC.presence_of_element_located((By.ID, "auction-asset"))
            )
            asset_select.send_keys("Test ML Model")
            
            # Select auction type
            auction_type = self.driver.find_element(By.ID, "auction-type")
            auction_type.send_keys("English Auction")
            
            # Set starting price
            starting_price = self.driver.find_element(By.ID, "starting-price")
            starting_price.send_keys("5")
            
            # Set duration
            duration = self.driver.find_element(By.ID, "auction-duration")
            duration.send_keys("24")  # 24 hours
            
            # Submit
            submit_button = self.driver.find_element(By.XPATH, "//button[@type='submit']")
            submit_button.click()
            
            # Wait for confirmation
            success = wait.until(
                EC.presence_of_element_located((By.CLASS_NAME, "auction-created"))
            )
            assert "auction created" in success.text.lower()
            
            # Place a bid
            bid_input = wait.until(
                EC.presence_of_element_located((By.ID, "bid-amount"))
            )
            bid_input.send_keys("6")
            
            place_bid_button = self.driver.find_element(By.XPATH, "//button[contains(text(), 'Place Bid')]")
            place_bid_button.click()
            
            # Verify bid placed
            bid_success = wait.until(
                EC.presence_of_element_located((By.CLASS_NAME, "bid-success"))
            )
            assert "bid placed" in bid_success.text.lower()
            
            print("✅ Auction UI test completed successfully")
            return True
            
        except Exception as e:
            self.driver.save_screenshot("auction_test_failure.png")
            raise e
    
    async def test_defi_features_ui(self):
        """Test DeFi features through UI"""
        try:
            # Navigate to DeFi section
            self.driver.get(f"{self.frontend_url}/marketplace/defi")
            wait = WebDriverWait(self.driver, 20)
            
            # Test NFT Lending
            lending_tab = wait.until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'NFT Lending')]"))
            )
            lending_tab.click()
            
            # Create loan offer
            create_loan_button = wait.until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Offer Loan')]"))
            )
            create_loan_button.click()
            
            # Fill loan details
            nft_select = self.driver.find_element(By.ID, "loan-nft")
            nft_select.send_keys("Test ML Model #1")
            
            loan_amount = self.driver.find_element(By.ID, "loan-amount")
            loan_amount.send_keys("50")
            
            interest_rate = self.driver.find_element(By.ID, "interest-rate")
            interest_rate.send_keys("5")
            
            loan_duration = self.driver.find_element(By.ID, "loan-duration")
            loan_duration.send_keys("30")  # 30 days
            
            # Submit loan offer
            submit_loan = self.driver.find_element(By.XPATH, "//button[@type='submit']")
            submit_loan.click()
            
            # Verify loan created
            loan_success = wait.until(
                EC.presence_of_element_located((By.CLASS_NAME, "loan-created"))
            )
            assert "loan offer created" in loan_success.text.lower()
            
            # Test Yield Farming
            farming_tab = self.driver.find_element(By.XPATH, "//button[contains(text(), 'Yield Farming')]")
            farming_tab.click()
            
            # Select pool
            pool_card = wait.until(
                EC.element_to_be_clickable((By.XPATH, "//div[@class='pool-card'][1]"))
            )
            pool_card.click()
            
            # Stake NFT
            stake_button = wait.until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Stake NFT')]"))
            )
            stake_button.click()
            
            # Select NFT to stake
            nft_to_stake = wait.until(
                EC.element_to_be_clickable((By.XPATH, "//div[@class='nft-option'][1]"))
            )
            nft_to_stake.click()
            
            confirm_stake = self.driver.find_element(By.XPATH, "//button[contains(text(), 'Confirm Stake')]")
            confirm_stake.click()
            
            # Verify staking success
            stake_success = wait.until(
                EC.presence_of_element_located((By.CLASS_NAME, "stake-success"))
            )
            assert "successfully staked" in stake_success.text.lower()
            
            print("✅ DeFi features UI test completed successfully")
            return True
            
        except Exception as e:
            self.driver.save_screenshot("defi_test_failure.png")
            raise e
    
    async def test_api_integration(self):
        """Test API endpoints directly"""
        async with aiohttp.ClientSession() as session:
            # Test health endpoints
            services = [
                "blockchain-event-bridge",
                "digital-asset-service",
                "mlops-service",
                "graph-intelligence-service"
            ]
            
            for service in services:
                async with session.get(f"{self.api_base_url}/{service}/health") as resp:
                    assert resp.status == 200
                    data = await resp.json()
                    assert data["status"] == "healthy"
                    print(f"✓ {service} is healthy")
            
            # Test marketplace stats API
            async with session.get(f"{self.api_base_url}/api/v1/marketplace/stats") as resp:
                assert resp.status == 200
                stats = await resp.json()
                assert "total_volume" in stats
                assert "total_assets" in stats
                assert "active_users" in stats
                print("✓ Marketplace stats API working")
            
            # Test search API
            async with session.get(
                f"{self.api_base_url}/api/v1/search",
                params={"q": "machine learning", "type": "asset"}
            ) as resp:
                assert resp.status == 200
                results = await resp.json()
                assert "results" in results
                print("✓ Search API working")
            
            return True
    
    async def test_performance_benchmarks(self):
        """Test performance benchmarks for key operations"""
        benchmarks = {
            "nft_mint": {"target": 3000, "actual": 0},  # Target: 3 seconds
            "license_purchase": {"target": 2000, "actual": 0},  # Target: 2 seconds
            "auction_bid": {"target": 1500, "actual": 0},  # Target: 1.5 seconds
            "search_query": {"target": 500, "actual": 0},  # Target: 500ms
            "ui_load": {"target": 2000, "actual": 0}  # Target: 2 seconds
        }
        
        async with aiohttp.ClientSession() as session:
            # Benchmark NFT minting
            start = time.time()
            async with session.post(
                f"{self.api_base_url}/api/v1/marketplace/mint-nft",
                json={
                    "asset_id": "test-asset-123",
                    "creator": self.test_accounts["creator"]["address"],
                    "royalty_fraction": 250
                }
            ) as resp:
                benchmarks["nft_mint"]["actual"] = int((time.time() - start) * 1000)
            
            # Benchmark license purchase
            start = time.time()
            async with session.post(
                f"{self.api_base_url}/api/v1/marketplace/purchase-license",
                json={
                    "license_id": "test-license-123",
                    "buyer": self.test_accounts["buyer"]["address"],
                    "payment": "10"
                }
            ) as resp:
                benchmarks["license_purchase"]["actual"] = int((time.time() - start) * 1000)
            
            # Benchmark search
            start = time.time()
            async with session.get(
                f"{self.api_base_url}/api/v1/search",
                params={"q": "test"}
            ) as resp:
                benchmarks["search_query"]["actual"] = int((time.time() - start) * 1000)
        
        # UI load benchmark
        start = time.time()
        self.driver.get(f"{self.frontend_url}/marketplace")
        WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, "marketplace-container"))
        )
        benchmarks["ui_load"]["actual"] = int((time.time() - start) * 1000)
        
        # Report results
        print("\n📊 Performance Benchmarks:")
        print("-" * 50)
        all_passed = True
        
        for operation, metrics in benchmarks.items():
            passed = metrics["actual"] <= metrics["target"]
            status = "✅" if passed else "❌"
            print(f"{status} {operation}: {metrics['actual']}ms (target: {metrics['target']}ms)")
            if not passed:
                all_passed = False
        
        return all_passed
    
    async def test_multi_chain_support(self):
        """Test multi-chain functionality"""
        chains = ["ethereum", "polygon", "avalanche", "solana", "near"]
        
        for chain in chains:
            try:
                # Switch chain in UI
                self.driver.get(f"{self.frontend_url}/marketplace")
                wait = WebDriverWait(self.driver, 10)
                
                # Click chain selector
                chain_selector = wait.until(
                    EC.element_to_be_clickable((By.ID, "chain-selector"))
                )
                chain_selector.click()
                
                # Select chain
                chain_option = wait.until(
                    EC.element_to_be_clickable((By.XPATH, f"//option[@value='{chain}']"))
                )
                chain_option.click()
                
                # Verify chain switched
                current_chain = self.driver.find_element(By.CLASS_NAME, "current-chain")
                assert chain in current_chain.text.lower()
                
                print(f"✓ Successfully switched to {chain}")
                
            except Exception as e:
                print(f"❌ Failed to switch to {chain}: {e}")
                return False
        
        return True


async def run_e2e_tests():
    """Run all E2E tests"""
    tester = MarketplaceE2ETests()
    tester.setup()
    
    print("\n🧪 Running Marketplace E2E Tests\n")
    
    tests = [
        ("Marketplace UI Flow", tester.test_marketplace_ui_flow),
        ("Auction UI Flow", tester.test_auction_ui_flow),
        ("DeFi Features UI", tester.test_defi_features_ui),
        ("API Integration", tester.test_api_integration),
        ("Performance Benchmarks", tester.test_performance_benchmarks),
        ("Multi-chain Support", tester.test_multi_chain_support)
    ]
    
    passed = 0
    failed = 0
    
    try:
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
        
        print(f"\n📊 E2E Test Results: {passed} passed, {failed} failed")
        
    finally:
        tester.teardown()
    
    return failed == 0


if __name__ == "__main__":
    success = asyncio.run(run_e2e_tests())
    exit(0 if success else 1) 