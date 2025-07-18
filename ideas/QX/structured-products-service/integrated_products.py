# services/structured-products-service/app/bundles/integrated_products.py
class IntegratedMarketProducts:
    """
    Products that span multiple marketplaces
    """
    
    async def create_ml_training_bundle(self, requirements: MLRequirements):
        """
        Bundle compute + data + model access
        """
        
        # Create structured product with multiple legs
        bundle = MLTrainingBundle(
            # Compute leg
            compute_future=ComputeFuture(
                resource_type="GPU",
                quantity=requirements.gpu_hours,
                quality="A100_equivalent",
                delivery_period=requirements.timeline
            ),
            
            # Data leg  
            data_future=DataFuture(
                dataset_type=requirements.data_type,
                size=requirements.data_size,
                quality_score=requirements.min_quality,
                preprocessing="included"
            ),
            
            # Model leg
            model_access=ModelAccessRight(
                base_model=requirements.base_model,
                fine_tuning_allowed=True,
                inference_calls=requirements.inference_budget
            ),
            
            # Pricing
            pricing_method="sum_of_parts_minus_synergy_discount",
            synergy_discount=0.15  # 15% discount for bundle
        )
        
        return bundle
    
    async def create_inference_service_future(self):
        """
        Future delivery of inference-as-a-service
        """
        
        return InferenceServiceFuture(
            model_specification="llm_70b_equivalent",
            throughput_guarantee="1000_requests_per_second",
            latency_sla="p99 < 100ms",
            availability_sla="99.9%",
            
            # Composite pricing
            pricing_components={
                "compute_cost": "spot_gpu_price",
                "model_license": "per_token_fee",
                "data_egress": "bandwidth_charges",
                "sla_penalties": "automated_rebates"
            }
        )