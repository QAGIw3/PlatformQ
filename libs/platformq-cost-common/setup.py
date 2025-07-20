from setuptools import setup, find_packages

setup(
    name="platformq-cost-common",
    version="0.1.0",
    description="Common cost management models and utilities for PlatformQ",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    python_requires=">=3.8",
    install_requires=[
        "pydantic>=2.0.0",
        "platformq-shared>=0.1.0",
        "decimal",
    ],
) 