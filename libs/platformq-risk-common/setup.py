from setuptools import setup, find_packages

setup(
    name="platformq-risk-common",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "numpy>=1.21.0",
        "scipy>=1.7.0",
        "pandas>=1.3.0",
    ],
    python_requires=">=3.8",
    description="Common risk management functionality for PlatformQ",
    author="PlatformQ",
) 