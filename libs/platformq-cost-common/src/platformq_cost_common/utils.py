"""Cost management utility functions"""

from decimal import Decimal
from typing import Dict, Tuple


def calculate_hourly_cost(resource_costs: Dict[str, Decimal],
                         cpu_cores: float,
                         memory_gb: float,
                         gpu_count: int = 0) -> Decimal:
    """Calculate hourly cost based on resource usage"""
    cpu_cost = Decimal(str(cpu_cores)) * resource_costs.get('cpu_core_hour', Decimal('0.05'))
    memory_cost = Decimal(str(memory_gb)) * resource_costs.get('memory_gb_hour', Decimal('0.01'))
    gpu_cost = Decimal(str(gpu_count)) * resource_costs.get('gpu_hour', Decimal('0.90'))
    
    return cpu_cost + memory_cost + gpu_cost


def parse_resource_string(resource_str: str) -> Tuple[float, str]:
    """Parse resource string like '2Gi' or '500m' to value and unit"""
    if not resource_str:
        return 0.0, ''
    
    # Handle memory units
    if resource_str.endswith('Gi'):
        return float(resource_str[:-2]), 'Gi'
    elif resource_str.endswith('Mi'):
        return float(resource_str[:-2]) / 1024, 'Gi'
    elif resource_str.endswith('Ki'):
        return float(resource_str[:-2]) / (1024 * 1024), 'Gi'
    elif resource_str.endswith('G'):
        return float(resource_str[:-1]), 'G'
    elif resource_str.endswith('M'):
        return float(resource_str[:-1]) / 1024, 'G'
    
    # Handle CPU units
    elif resource_str.endswith('m'):
        return float(resource_str[:-1]) / 1000, 'cores'
    
    # Default to numeric value
    try:
        return float(resource_str), ''
    except ValueError:
        return 0.0, ''


def format_cost(cost: Decimal, currency: str = 'USD') -> str:
    """Format cost for display"""
    if currency == 'USD':
        return f"${cost:,.2f}"
    elif currency == 'EUR':
        return f"€{cost:,.2f}"
    elif currency == 'GBP':
        return f"£{cost:,.2f}"
    else:
        return f"{currency} {cost:,.2f}" 