from setuptools import setup, find_packages

setup(
    name="platformq-shared",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "python-consul",
        "fastapi",
        "pulsar-client",
        "avro",
        "cassandra-driver",
        "hvac",
        "opentelemetry-api",
        "opentelemetry-sdk",
        "opentelemetry-exporter-otlp",
        "opentelemetry-instrumentation-fastapi",
        "python-json-logger",
    ],
)
