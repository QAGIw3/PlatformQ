from setuptools import setup, find_packages

setup(
    name="data-intelligence-common",
    version="2.0.0",
    description="Enhanced common utilities and patterns for DataIntelligenceSuite services - Enterprise Edition",
    packages=find_packages(),
    install_requires=[
        # Web framework
        "fastapi>=0.104.1",
        "uvicorn[standard]>=0.24.0",
        "pydantic>=2.5.0",
        "httpx>=0.25.2",
        
        # Logging and monitoring
        "structlog>=23.2.0",
        "prometheus-client>=0.19.0",
        "opentelemetry-api>=1.21.0",
        "opentelemetry-sdk>=1.21.0",
        "opentelemetry-instrumentation-fastapi>=0.42b0",
        "python-json-logger>=2.0.7",
        
        # HashiCorp tools
        "python-consul>=1.1.0",
        "hvac>=2.1.0",  # HashiCorp Vault client
        
        # Messaging and caching - Standardized on Pulsar
        "pulsar-client>=3.3.0",
        "pyignite>=0.6.1",
        
        # Async and resilience
        "asyncio>=3.4.3",
        "tenacity>=8.2.3",
        "aiohttp>=3.9.0",
        "aiocache>=0.12.2",
        
        # Security
        "cryptography>=41.0.0",
        "bcrypt>=4.1.2",
        "pyjwt>=2.8.0",
        
        # Data processing
        "pandas>=2.0.0",
        "numpy>=1.24.0",
        "pyarrow>=14.0.0",
        "polars>=0.19.0",  # High-performance dataframes
        
        # ML dependencies
        "scikit-learn>=1.3.0",
        "optuna>=3.4.0",
        "mlflow>=2.8.0",
        "joblib>=1.3.0",
        
        # Database
        "sqlalchemy>=2.0.0",
        "asyncpg>=0.29.0",
        "motor>=3.3.0",  # Async MongoDB
        
        # Graph processing
        "gremlinpython>=3.7.0",
        "networkx>=3.0",
        
        # Utilities
        "python-dateutil>=2.8.0",
        "pytz>=2023.3",
        "pyyaml>=6.0",
        "boto3>=1.29.0",  # For S3/MinIO
        "orjson>=3.9.0",  # Fast JSON
        "xxhash>=3.4.0",  # Fast hashing
        
        # Distributed computing
        "ray>=2.8.0",
        "dask>=2023.12.0",
        
        # API and serialization
        "grpcio>=1.60.0",
        "grpcio-tools>=1.60.0",
        "avro-python3>=1.11.0",
        
        # Optional ML explainability (install with pip install data-intelligence-common[ml])
        # "shap>=0.43.0",
        # "lime>=0.2.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.4.0",
            "pytest-asyncio>=0.21.0",
            "pytest-cov>=4.1.0",
            "black>=23.0.0",
            "ruff>=0.1.0",
            "mypy>=1.7.0",
            "pytest-benchmark>=4.0.0",
        ],
        "ml": [
            "shap>=0.43.0",
            "lime>=0.2.0",
            "matplotlib>=3.7.0",
            "seaborn>=0.13.0",
            "torch>=2.1.0",
            "tensorflow>=2.15.0",
            "xgboost>=2.0.0",
            "lightgbm>=4.1.0",
            "catboost>=1.2.0",
        ],
        "spark": [
            "pyspark>=3.5.0",
            "delta-spark>=3.0.0",
        ],
        "flink": [
            "apache-flink>=1.18.0",
            "pyflink>=1.18.0",
        ],
        "lakehouse": [
            "pyiceberg>=0.5.0",
            "deltalake>=0.14.0",
            "hudi>=0.14.0",
        ],
        "quality": [
            "great-expectations>=0.18.0",
            "pydeequ>=1.1.0",
            "soda-core>=3.0.0",
        ],
        "catalog": [
            "datahub>=0.12.0",
            "openlineage-python>=1.7.0",
            "apache-atlas>=0.0.5",
        ],
        "realtime": [
            "clickhouse-driver>=0.2.6",
            "aiochclient>=2.2.0",
            "pypinot>=0.1.0",
            "pymysql>=1.1.0",
            "apache-druid>=0.6.0",
            "trino>=0.327.0",
        ],
        "unity": [
            "databricks-sdk>=0.12.0",
        ],
        "flink-sql": [
            "py4j>=0.10.9",
            "apache-flink>=1.18.0",
        ],
        "graphql": [
            "strawberry-graphql[federation]>=0.200.0",
            "aiohttp>=3.9.0",
        ],
        "governance": [
            "opa-python-client>=1.3.0",
            "rego>=0.4.0",
        ],
        "federated-ml": [
            "cryptography>=41.0.0",
            "numpy>=1.24.0",
            "tenseal>=0.3.0",  # Homomorphic encryption
            "diffprivlib>=0.6.0",  # Differential privacy
        ],
        "security": [
            "apache-ranger>=2.4.0",
        ],
        "orchestration": [
            "temporal-python-sdk>=1.4.0",
            "apache-dolphinscheduler>=3.1.0",
            "apache-airflow-client>=2.7.0",
            "prefect>=2.14.0",
        ],
        "streaming": [
            "apache-beam>=2.52.0",
            "benthos-py>=0.1.0",
            "bytewax>=0.18.0",
        ],
        "advanced": [
            "qiskit>=0.45.0",  # Quantum computing
            "pennylane>=0.33.0",  # Quantum ML
            "brian2>=2.5.0",  # Neuromorphic computing
        ],
        "all": [
            "pyiceberg>=0.5.0",
            "deltalake>=0.14.0",
            "hudi>=0.14.0",
            "great-expectations>=0.18.0",
            "pydeequ>=1.1.0",
            "soda-core>=3.0.0",
            "datahub>=0.12.0",
            "openlineage-python>=1.7.0",
            "clickhouse-driver>=0.2.6",
            "aiochclient>=2.2.0",
            "pypinot>=0.1.0",
            "pymysql>=1.1.0",
            "databricks-sdk>=0.12.0",
            "py4j>=0.10.9",
            "strawberry-graphql[federation]>=0.200.0",
            "opa-python-client>=1.3.0",
            "rego>=0.4.0",
            "apache-atlas>=0.0.5",
            "apache-druid>=0.6.0",
            "trino>=0.327.0",
            "tenseal>=0.3.0",
            "diffprivlib>=0.6.0",
            "qiskit>=0.45.0",
            "pennylane>=0.33.0",
            "brian2>=2.5.0",
        ]
    },
    python_requires=">=3.10",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
) 