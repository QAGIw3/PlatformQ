"""Structured products templates API endpoints."""

from typing import Dict, List, Any
from fastapi import APIRouter, Depends, HTTPException
from decimal import Decimal

from app.models.products import ProductType

router = APIRouter(prefix="/api/v1/templates", tags=["templates"])


# Product Templates

PRODUCT_TEMPLATES = {
    "autocallable": {
        "name": "Autocallable Note",
        "description": "Early redemption with periodic coupon payments",
        "risk_level": "medium",
        "typical_tenor": "1-3 years",
        "features": [
            "Early redemption at predetermined levels",
            "Periodic coupon payments",
            "Downside barrier protection",
            "Memory coupon feature"
        ],
        "suitable_for": [
            "Moderately bullish outlook",
            "Income seeking investors",
            "Partial downside protection"
        ],
        "example_terms": {
            "autocall_levels": [1.0, 0.95, 0.90],
            "barrier_level": 0.7,
            "coupon_rate": 0.08,
            "observation_frequency": "quarterly"
        }
    },
    "reverse_convertible": {
        "name": "Reverse Convertible",
        "description": "High coupon with potential share delivery",
        "risk_level": "high",
        "typical_tenor": "3-12 months",
        "features": [
            "Guaranteed coupon payment",
            "Physical or cash settlement",
            "Strike below current spot",
            "Optional knock-in barrier"
        ],
        "suitable_for": [
            "Income seeking in sideways markets",
            "Willing to own underlying at discount",
            "High risk tolerance"
        ],
        "example_terms": {
            "strike_percent": 0.9,
            "coupon_rate": 0.15,
            "barrier_level": 0.8
        }
    },
    "range_accrual": {
        "name": "Range Accrual Note",
        "description": "Accrues coupon when underlying stays in range",
        "risk_level": "medium",
        "typical_tenor": "6-24 months",
        "features": [
            "Daily accrual monitoring",
            "Defined range boundaries",
            "Capital protection options",
            "Enhanced yield in stable markets"
        ],
        "suitable_for": [
            "Range-bound market view",
            "Enhanced yield seeking",
            "Moderate risk appetite"
        ],
        "example_terms": {
            "lower_bound_percent": 0.9,
            "upper_bound_percent": 1.1,
            "daily_accrual_rate": 0.0003,
            "protection_level": 0.95
        }
    },
    "accumulator": {
        "name": "Accumulator",
        "description": "Accumulate shares at discount with leverage",
        "risk_level": "very_high",
        "typical_tenor": "3-12 months",
        "features": [
            "Accumulate below strike",
            "Leveraged accumulation",
            "Knock-out protection",
            "Maximum accumulation limit"
        ],
        "suitable_for": [
            "Very bullish outlook",
            "Professional investors",
            "High risk tolerance"
        ],
        "example_terms": {
            "strike_percent": 0.95,
            "leverage": 2,
            "knock_out_level": 1.05,
            "accumulation_frequency": "daily"
        }
    },
    "volatility_target": {
        "name": "Volatility Target Note",
        "description": "Participation with volatility control",
        "risk_level": "low_medium",
        "typical_tenor": "2-5 years",
        "features": [
            "Dynamic exposure adjustment",
            "Volatility cap mechanism",
            "Participation in upside",
            "Downside mitigation"
        ],
        "suitable_for": [
            "Conservative growth seeking",
            "Volatility averse investors",
            "Long-term investment horizon"
        ],
        "example_terms": {
            "target_volatility": 0.1,
            "participation_cap": 1.5,
            "protection_level": 0.9
        }
    }
}


@router.get("/")
async def list_templates() -> List[Dict[str, Any]]:
    """List all available product templates."""
    return [
        {
            "product_type": key,
            "name": template["name"],
            "description": template["description"],
            "risk_level": template["risk_level"],
            "typical_tenor": template["typical_tenor"]
        }
        for key, template in PRODUCT_TEMPLATES.items()
    ]


@router.get("/{product_type}")
async def get_template(product_type: str) -> Dict[str, Any]:
    """Get detailed template for a specific product type."""
    if product_type not in PRODUCT_TEMPLATES:
        raise HTTPException(status_code=404, detail="Template not found")
    
    template = PRODUCT_TEMPLATES[product_type]
    return {
        "product_type": product_type,
        **template
    }


@router.get("/{product_type}/examples")
async def get_template_examples(product_type: str) -> List[Dict[str, Any]]:
    """Get example configurations for a product type."""
    if product_type not in PRODUCT_TEMPLATES:
        raise HTTPException(status_code=404, detail="Template not found")
    
    # Generate example configurations
    examples = []
    
    if product_type == "autocallable":
        examples = [
            {
                "name": "Conservative Autocallable",
                "underlying": "SPX",
                "maturity_months": 24,
                "autocall_levels": [1.0, 0.98, 0.96],
                "barrier_level": 0.75,
                "coupon_rate": 0.06,
                "expected_return": "6-8% p.a."
            },
            {
                "name": "High Yield Autocallable",
                "underlying": "Single Stock",
                "maturity_months": 12,
                "autocall_levels": [1.0, 0.95, 0.90],
                "barrier_level": 0.65,
                "coupon_rate": 0.12,
                "expected_return": "10-15% p.a."
            }
        ]
    elif product_type == "reverse_convertible":
        examples = [
            {
                "name": "Defensive Reverse Convertible",
                "underlying": "Blue Chip Stock",
                "maturity_months": 6,
                "strike_percent": 0.85,
                "coupon_rate": 0.08,
                "barrier_level": 0.75,
                "expected_return": "8% p.a."
            },
            {
                "name": "High Coupon Reverse Convertible",
                "underlying": "Volatile Stock",
                "maturity_months": 3,
                "strike_percent": 0.95,
                "coupon_rate": 0.20,
                "expected_return": "20% p.a."
            }
        ]
    
    return examples


@router.get("/risk-profiles")
async def get_risk_profiles() -> Dict[str, Any]:
    """Get risk profiles for different product types."""
    return {
        "risk_levels": {
            "low": {
                "description": "Capital protection focused",
                "max_loss": "0-10%",
                "suitable_products": ["volatility_target"]
            },
            "medium": {
                "description": "Balanced risk-return",
                "max_loss": "10-30%",
                "suitable_products": ["autocallable", "range_accrual"]
            },
            "high": {
                "description": "Yield enhancement focused",
                "max_loss": "30-50%",
                "suitable_products": ["reverse_convertible"]
            },
            "very_high": {
                "description": "Speculative strategies",
                "max_loss": ">50%",
                "suitable_products": ["accumulator"]
            }
        }
    }


@router.get("/underlyings")
async def get_suitable_underlyings() -> Dict[str, Any]:
    """Get suitable underlying assets for each product type."""
    return {
        "autocallable": {
            "suitable": ["equity_indices", "single_stocks", "baskets"],
            "recommended": ["SPX", "EURO STOXX 50", "Nikkei 225"],
            "min_liquidity": "high"
        },
        "reverse_convertible": {
            "suitable": ["single_stocks", "equity_indices"],
            "recommended": ["Large cap stocks", "Sector ETFs"],
            "min_liquidity": "medium"
        },
        "range_accrual": {
            "suitable": ["fx_pairs", "commodities", "equity_indices"],
            "recommended": ["EUR/USD", "Gold", "SPX"],
            "min_liquidity": "high"
        },
        "accumulator": {
            "suitable": ["single_stocks", "commodities"],
            "recommended": ["Blue chip stocks", "Gold", "Oil"],
            "min_liquidity": "very_high"
        }
    } 