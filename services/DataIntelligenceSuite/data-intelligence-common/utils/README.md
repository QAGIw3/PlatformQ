# Utility Functions

This directory contains utility functions that extend or simplify standard library functionality.

## Guidelines

Utilities should only be included here if they:
1. Provide significant value beyond standard library functions
2. Combine multiple standard library functions in a useful way
3. Add domain-specific logic that's reused across the platform
4. Provide better error handling or type safety

## Modules

### converters.py
**Purpose**: Data format conversions beyond standard library
- **DataFrameConverter**: Converts between pandas, polars, and other dataframe formats
- **FormatConverter**: Handles complex format conversions (Avro, Parquet, etc.)
- **SchemaConverter**: Converts between different schema formats
- ✅ **Justification**: Provides domain-specific conversions not in standard library

### datetime_utils.py
**Purpose**: Enhanced datetime handling
- **DateTimeParser**: Fuzzy datetime parsing with multiple format support
- **TimeZoneUtils**: Complex timezone conversions and business hours
- **BusinessDateUtils**: Business day calculations
- ⚠️ **Note**: Some functions like `now()` and `today()` are thin wrappers - consider using `datetime.now(tz)` directly
- ✅ **Justification**: Business date logic and fuzzy parsing add value

### encryption.py
**Purpose**: Encryption and hashing utilities
- **AESEncryption**: Simplified AES encryption with key derivation
- **PasswordUtils**: Password hashing with bcrypt/argon2
- **TokenUtils**: Secure token generation
- ✅ **Justification**: Simplifies cryptography library usage with secure defaults

### graph_utils.py
**Purpose**: Graph analysis utilities
- **calculate_centrality**: Various centrality measures
- **detect_communities**: Community detection algorithms
- **analyze_influence_propagation**: Influence analysis
- ✅ **Justification**: Domain-specific graph algorithms for platform use cases

### quantum_utils.py
**Purpose**: Quantum computing utilities
- **qubo_to_ising**: QUBO/Ising model conversions
- **create_optimization_problems**: Problem formulation helpers
- ✅ **Justification**: Specialized quantum computing utilities

### helpers.py
**Purpose**: General helper functions
- **retry_async**: Async retry with backoff
- **chunk_list**: List chunking
- **flatten_dict**: Dictionary flattening
- **merge_dicts**: Deep dictionary merging
- ⚠️ **Consider**: Some functions duplicate `itertools` or could use standard library

### validators.py
**Purpose**: Input validation utilities
- **validate_email**: Email validation
- **validate_url**: URL validation
- **validate_json_schema**: JSON schema validation
- ⚠️ **Consider**: Use libraries like `pydantic` for validation instead

## Recommendations for Refactoring

1. **Remove thin wrappers**: Functions like `now()` that just wrap `datetime.now()`
2. **Use standard library**: Replace `chunk_list` with `itertools.batched` (Python 3.12+)
3. **Leverage existing libraries**: Use `pydantic` for validation instead of custom validators
4. **Consolidate retry logic**: We have retry decorators in multiple places - consolidate into one

## Usage Examples

```python
# Good - provides real value
from data_intelligence_common.utils import DateTimeParser, BusinessDateUtils

# Parse fuzzy date
date = DateTimeParser.parse("next Tuesday at 3pm")

# Calculate business days
days = BusinessDateUtils.add_business_days(date, 5, holidays=US_HOLIDAYS)

# Less ideal - thin wrapper
from data_intelligence_common.utils import now
current_time = now()  # Just use datetime.now(timezone.utc)
```

## Migration Plan

For utilities that should be removed:

1. **Deprecate first**: Add deprecation warnings
2. **Update imports**: Change all usages to standard library
3. **Remove in next major version**: Clean up deprecated functions

Example deprecation:
```python
import warnings
from datetime import datetime, timezone

def now(tz: str = "UTC") -> datetime:
    warnings.warn(
        "utils.now() is deprecated. Use datetime.now(timezone.utc) instead.",
        DeprecationWarning,
        stacklevel=2
    )
    return datetime.now(timezone.utc)
``` 