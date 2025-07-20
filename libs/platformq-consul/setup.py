"""Setup for PlatformQ Consul Integration Library."""

from setuptools import setup, find_packages

setup(
    name="platformq-consul",
    version="0.1.0",
    description="Consul service mesh integration for PlatformQ services",
    author="PlatformQ",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "python-consul2>=0.1.5",
        "httpx>=0.25.0",
    ],
    python_requires=">=3.8",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
) 