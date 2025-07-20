from setuptools import setup, find_packages

setup(
    name="platformq-vc-common",
    version="0.1.0",
    description="Common library for PlatformQ verifiable credential operations",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "pydantic>=1.10.0",
        "pyld>=2.0.3",
        "python-jose[cryptography]>=3.3.0",
        "jsonschema>=4.17.0",
        "base58>=2.1.0",
        "cryptography>=41.0.0",
    ],
    python_requires=">=3.10",
) 