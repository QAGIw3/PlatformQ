from setuptools import setup, find_packages

setup(
    name="platformq-event-framework",
    version="2.0.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "pulsar-client[avro]>=3.0.0",
        "aiopulsar>=0.2.0",
        "pydantic>=2.0.0",
        "tenacity>=8.0.0",
        "pyignite[async]>=0.5.2",
        "prometheus-client>=0.15.0",
        "opentelemetry-api>=1.15.0",
        "opentelemetry-instrumentation>=0.35b0",
    ],
    python_requires=">=3.8",
    author="PlatformQ Team",
    description="Enhanced event processing framework for PlatformQ services",
) 