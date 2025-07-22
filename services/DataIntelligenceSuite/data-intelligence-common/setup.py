from setuptools import setup, find_namespace_packages

setup(
    name="data-intelligence-common",
    version="0.1.0",
    description="Common utilities and patterns for DataIntelligenceSuite services",
    packages=find_namespace_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "fastapi>=0.104.1",
        "uvicorn[standard]>=0.24.0",
        "pydantic>=2.5.0",
        "httpx>=0.25.2",
        "structlog>=23.2.0",
        "prometheus-client>=0.19.0",
        "opentelemetry-api>=1.21.0",
        "opentelemetry-sdk>=1.21.0",
        "opentelemetry-instrumentation-fastapi>=0.42b0",
        "python-consul>=1.1.0",
        "hvac>=2.1.0",  # HashiCorp Vault client
        "pulsar-client>=3.3.0",
        "pyignite>=0.6.1",
        "asyncio>=3.4.3",
        "tenacity>=8.2.3",
        "python-json-logger>=2.0.7",
        "cryptography>=41.0.0",  # For Fernet encryption
        "consul-python>=0.0.1",  # For async consul operations
    ],
    extras_require={
        "dev": [
            "pytest>=7.4.0",
            "pytest-asyncio>=0.21.0",
            "pytest-cov>=4.1.0",
            "black>=23.0.0",
            "ruff>=0.1.0",
        ]
    },
    python_requires=">=3.10",
) 