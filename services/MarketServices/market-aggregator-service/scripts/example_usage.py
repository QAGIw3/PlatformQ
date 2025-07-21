#!/usr/bin/env python3
"""
Example usage of the Market Aggregator Service
"""
import asyncio
import httpx
from datetime import datetime


BASE_URL = "http://localhost:8028/api/v1"


async def create_hybrid_bundle():
    """Example: Create a quantum-ML hybrid workload bundle"""
    async with httpx.AsyncClient() as client:
        # Create bundle with quantum and AI resources
        bundle_request = {
            "name": "Quantum-ML Hybrid Analysis",
            "description": "Quantum feature extraction with ML classification",
            "requirements": [
                {
                    "resource_type": "quantum",
                    "min_qubit_count": 20,
                    "min_coherence_minutes": 10,
                    "max_error_rate": 0.01,
                    "priority": 9
                },
                {
                    "resource_type": "ai",
                    "accelerator_type": "GPU",
                    "min_tflops": 100,
                    "duration_hours": 2,
                    "priority": 8
                },
                {
                    "resource_type": "network",
                    "source_node": "quantum_center",
                    "destination_node": "ai_cluster",
                    "min_bandwidth_mbps": 1000,
                    "max_latency_ms": 10,
                    "duration_hours": 2,
                    "priority": 7
                }
            ],
            "optimization_objective": "balance_cost_performance"
        }
        
        response = await client.post(
            f"{BASE_URL}/bundles",
            json=bundle_request,
            params={"user_address": "0x1234567890abcdef"}
        )
        
        bundle_data = response.json()
        print(f"Created bundle: {bundle_data['bundle']['bundle_id']}")
        print(f"Estimated cost: ${bundle_data['estimated_cost']:.2f}")
        print(f"Availability: {bundle_data['availability_status']}")
        
        return bundle_data['bundle']['bundle_id']


async def optimize_and_allocate_bundle(bundle_id: str):
    """Example: Optimize and allocate a bundle"""
    async with httpx.AsyncClient() as client:
        allocation_request = {
            "bundle_id": bundle_id,
            "duration_hours": 2,
            "budget_limit": 5000,
            "quality_thresholds": {
                "quantum": 85,
                "ai": 90,
                "network": 80
            }
        }
        
        response = await client.post(
            f"{BASE_URL}/bundles/{bundle_id}/allocate",
            json=allocation_request
        )
        
        allocation_data = response.json()
        print(f"\nAllocation ID: {allocation_data['allocation']['allocation_id']}")
        print(f"Total cost: ${allocation_data['allocation']['total_cost']:.2f}")
        print(f"Bundle discount: {allocation_data['allocation']['bundle_discount'] * 100:.1f}%")
        print(f"Final cost: ${allocation_data['allocation']['final_cost']:.2f}")
        print(f"Optimization score: {allocation_data['allocation']['optimization_score']:.1f}")
        
        print("\nResource allocations:")
        for resource in allocation_data['resource_details']:
            print(f"  - {resource['resource_type']}: {resource['resource_id']}")
            print(f"    Cost: ${resource['total_cost']:.2f}")
            print(f"    Quality: {resource.get('quality_score', 'N/A')}")
        
        return allocation_data['allocation']['allocation_id']


async def search_arbitrage_opportunities():
    """Example: Search for arbitrage opportunities"""
    async with httpx.AsyncClient() as client:
        search_request = {
            "min_profit_margin": 0.05,  # 5% minimum
            "max_risk_score": 0.5,
            "time_horizon_minutes": 120
        }
        
        response = await client.post(
            f"{BASE_URL}/arbitrage/search",
            json=search_request
        )
        
        arbitrage_data = response.json()
        print(f"\nFound {len(arbitrage_data['opportunities'])} arbitrage opportunities")
        print(f"Total potential profit: ${arbitrage_data['total_potential_profit']:.2f}")
        
        for opp in arbitrage_data['opportunities'][:3]:  # Top 3
            print(f"\nOpportunity: {opp['opportunity_id']}")
            print(f"  Type: {opp['arbitrage_type']}")
            print(f"  Resource: {opp['resource_type']} - {opp['resource_id']}")
            print(f"  Buy at: ${opp['price_a']:.2f} ({opp['market_a']})")
            print(f"  Sell at: ${opp['price_b']:.2f} ({opp['market_b']})")
            print(f"  Profit: ${opp['potential_profit']:.2f} ({opp['profit_margin'] * 100:.1f}%)")
            print(f"  Risk: {opp['risk_score']:.2f}")
        
        return arbitrage_data['opportunities']


async def compare_markets():
    """Example: Compare market prices"""
    async with httpx.AsyncClient() as client:
        comparison_request = {
            "resource_type": "ai",
            "specifications": {
                "accelerator_type": "GPU",
                "min_tflops": 100
            },
            "duration_hours": 168,  # 1 week
            "include_quality_adjusted": True
        }
        
        response = await client.post(
            f"{BASE_URL}/markets/compare",
            json=comparison_request
        )
        
        comparison_data = response.json()
        print(f"\nMarket comparison for {comparison_request['resource_type']}:")
        print("Market prices:")
        for market, price in comparison_data['market_prices'].items():
            print(f"  {market}: ${price:.2f}/hour")
        
        print("\nQuality-adjusted prices:")
        for market, price in comparison_data['quality_adjusted_prices'].items():
            print(f"  {market}: ${price:.2f}/hour")
        
        print(f"\nBest option: {comparison_data['best_option']}")
        print(f"Savings potential: ${comparison_data['savings_potential']:.2f}/hour")
        
        print("\nRecommendations:")
        for rec in comparison_data['recommendations']:
            print(f"  - {rec}")


async def get_workload_template():
    """Example: Get and optimize a workload template"""
    async with httpx.AsyncClient() as client:
        # Get specific template
        template_id = "quantum_ml_hybrid"
        response = await client.get(f"{BASE_URL}/markets/workload-templates/{template_id}")
        
        template = response.json()
        print(f"\nWorkload Template: {template['name']}")
        print(f"Description: {template['description']}")
        print(f"Estimated cost: ${template['estimated_cost_range'][0]:.2f} - ${template['estimated_cost_range'][1]:.2f}")
        
        # Optimize the template
        response = await client.post(
            f"{BASE_URL}/markets/optimize-workload/{template_id}",
            params={
                "budget_limit": 1000,
                "performance_priority": 0.7  # 70% performance, 30% cost
            }
        )
        
        optimization = response.json()
        print(f"\nOptimization profile:")
        print(f"  Performance priority: {optimization['optimization_profile']['performance_priority'] * 100:.0f}%")
        print(f"  Budget limit: ${optimization['optimization_profile']['budget_limit']}")
        
        print("\nRecommendations:")
        for rec in optimization['recommendations']:
            print(f"  - {rec}")
        
        print("\nResource selections:")
        for resource, selection in optimization['resource_selections'].items():
            print(f"  {resource}: {selection}")


async def main():
    """Run all examples"""
    print("=== Market Aggregator Service Examples ===\n")
    
    # Create and allocate a bundle
    print("1. Creating hybrid quantum-ML bundle...")
    bundle_id = await create_hybrid_bundle()
    
    print("\n2. Optimizing and allocating bundle...")
    allocation_id = await optimize_and_allocate_bundle(bundle_id)
    
    # Search for arbitrage
    print("\n3. Searching for arbitrage opportunities...")
    opportunities = await search_arbitrage_opportunities()
    
    # Compare markets
    print("\n4. Comparing market prices...")
    await compare_markets()
    
    # Work with templates
    print("\n5. Using workload templates...")
    await get_workload_template()
    
    print("\n=== Examples completed ===")


if __name__ == "__main__":
    asyncio.run(main()) 