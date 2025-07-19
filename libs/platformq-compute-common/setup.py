"""Setup for PlatformQ Compute Common Library"""

from setuptools import setup, find_packages

setup(
    name="platformq-compute-common",
    version="0.1.0",
    description="Common compute resource models and utilities for PlatformQ",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "pydantic>=2.0.0",
        "pyignite>=0.5.0",
        "pulsar-client>=3.0.0",
    ],
    python_requires=">=3.8",
) 