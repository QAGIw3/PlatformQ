from setuptools import setup, find_packages

setup(
    name="platformq-unified-data",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "cassandra-driver>=3.25.0",
        "pyignite>=0.5.0",
        "elasticsearch>=8.0.0",
        "minio>=7.1.0",
        "aiogremlin>=3.3.0",
        "pulsar-client>=3.0.0",
        "trino>=0.320.0",
        "sqlalchemy>=1.4.0",
        "redis>=4.5.0",
        "pandas>=1.5.0",
        "pyarrow>=10.0.0",
        "avro-python3>=1.10.0",
        "prometheus-client>=0.15.0",
        "tenacity>=8.1.0",
        "cachetools>=5.2.0"
    ],
    python_requires=">=3.8",
    description="Unified data access layer for PlatformQ",
    author="PlatformQ Team",
) 