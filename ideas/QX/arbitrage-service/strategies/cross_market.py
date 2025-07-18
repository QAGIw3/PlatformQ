# services/arbitrage-service/app/strategies/cross_market.py
class CrossMarketArbitrage:
    """
    Arbitrage opportunities across marketplaces
    """
    
    async def compute_storage_arbitrage(self):
        """
        Arbitrage between compute and storage markets
        Store when cheap, compute when expensive
        """
        
        strategy = ComputeStorageArbitrage(
            signal="compute_futures_price / storage_cost",
            threshold=2.5,  # Compute 2.5x more expensive than storage
            
            actions={
                "when_compute_expensive": [
                    "buy_storage_capacity",
                    "precompute_and_cache",
                    "sell_compute_futures"
                ],
                "when_storage_expensive": [
                    "buy_compute_futures",
                    "reduce_cache_size",
                    "compute_on_demand"
                ]
            }
        )
        
        return strategy
    
    async def create_market_neutral_basket(self):
        """
        Market neutral across all platform markets
        """
        
        return MarketNeutralBasket(
            long_positions=[
                ("compute_gpu_futures", 0.25),
                ("high_quality_data_index", 0.25),
                ("top_model_performance_index", 0.25),
                ("digital_asset_creation_rate", 0.25)
            ],
            short_positions=[
                ("compute_cpu_futures", 0.25),
                ("raw_data_index", 0.25),
                ("legacy_model_index", 0.25),
                ("asset_floor_price_index", 0.25)
            ],
            rebalancing="daily",
            target_beta=0.0
        )