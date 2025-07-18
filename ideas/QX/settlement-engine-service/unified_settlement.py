# services/settlement-engine-service/app/unified_settlement.py
class UnifiedSettlementEngine:
    """
    Settles across all marketplaces and products
    """
    
    def __init__(self):
        self.settlement_cycles = {
            "spot": "T+0",          # Immediate
            "futures": "T+1",       # Next day
            "complex": "T+2"        # Structured products
        }
        
        self.clearing_house = UnifiedClearingHouse()
        
    async def atomic_cross_market_settlement(
        self,
        transaction_bundle: TransactionBundle
    ):
        """
        Atomic settlement across multiple markets
        """
        
        # Begin transaction
        tx = await self.begin_atomic_transaction()
        
        try:
            # Settle each leg
            for leg in transaction_bundle.legs:
                if leg.market == "compute":
                    await self.settle_compute_leg(leg, tx)
                elif leg.market == "data":
                    await self.settle_data_leg(leg, tx)
                elif leg.market == "model":
                    await self.settle_model_leg(leg, tx)
                elif leg.market == "asset":
                    await self.settle_asset_leg(leg, tx)
                    
            # Net settlement
            net_payment = await self.calculate_net_payment(transaction_bundle)
            await self.execute_payment(net_payment, tx)
            
            # Commit
            await tx.commit()
            
        except Exception as e:
            await tx.rollback()
            raise SettlementError(f"Failed to settle: {e}")