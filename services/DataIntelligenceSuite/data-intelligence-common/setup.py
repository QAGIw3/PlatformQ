from setuptools import setup, find_packages

setup(
    name="data-intelligence-common",
    version="1.0.0",
    description="Common utilities and patterns for DataIntelligenceSuite services",
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
        
        # Messaging and caching
        "pulsar-client>=3.3.0",
        "pyignite>=0.6.1",
        
        # Async and resilience
        "asyncio>=3.4.3",
        "tenacity>=8.2.3",
        "aiohttp>=3.9.0",
        
        # Security
        "cryptography>=41.0.0",
        "bcrypt>=4.1.2",
        "pyjwt>=2.8.0",
        
        # Data processing
        "pandas>=2.0.0",
        "numpy>=1.24.0",
        "pyarrow>=14.0.0",
        
        # ML dependencies
        "scikit-learn>=1.3.0",
        "optuna>=3.4.0",
        "mlflow>=2.8.0",
        "joblib>=1.3.0",
        
        # Database
        "sqlalchemy>=2.0.0",
        "asyncpg>=0.29.0",
        
        # Utilities
        "python-dateutil>=2.8.0",
        "pytz>=2023.3",
        "pyyaml>=6.0",
        "boto3>=1.29.0",  # For S3/MinIO
        "networkx>=3.0",  # For lineage graphs
        
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
        ],
        "ml": [
            "shap>=0.43.0",
            "lime>=0.2.0",
            "matplotlib>=3.7.0",
            "seaborn>=0.13.0",
        ],
        "spark": [
            "pyspark>=3.5.0",
        ],
        "flink": [
            "apache-flink>=1.18.0",
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
        ],
        "olap": [
            "clickhouse-driver>=0.2.6",
            "pypinot>=0.1.0",
        ],
        "security": [
            "apache-ranger>=2.4.0",
        ],
        "orchestration": [
            "temporal-python-sdk>=1.4.0",
            "apache-dolphinscheduler>=3.1.0",
        ],
        "streaming": [
            "apache-beam>=2.52.0",
            "benthos-py>=0.1.0",
        ]
    },
    python_requires=">=3.10",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
) 