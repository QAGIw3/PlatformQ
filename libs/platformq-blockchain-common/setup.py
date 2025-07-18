from setuptools import setup, find_packages

setup(
    name="platformq-blockchain-common",
    version="1.0.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "web3>=6.5.0",
        "eth-account>=0.9.0",
        "eth-utils>=2.1.0",
        "eth-abi>=4.0.0",
        "rlp>=3.0.0",
        "pydantic>=2.0.0",
        "pyignite[async]>=0.5.2",
    ],
    python_requires=">=3.8",
    author="PlatformQ Team",
    description="Common blockchain functionality for PlatformQ services",
) 