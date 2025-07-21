"""Setup for PlatformQ Direct Communication Library."""

from setuptools import setup, find_packages

setup(
    name="platformq-direct-comm",
    version="0.1.0",
    description="Ultra-low latency direct communication library for PlatformQ services",
    author="PlatformQ",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "pyignite>=0.6.0",
        "msgpack>=1.0.5",
        "msgpack-numpy>=0.4.8",
        "uvloop>=0.19.0",
    ],
    python_requires=">=3.11",
) 