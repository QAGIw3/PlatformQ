"""Setup configuration for platformq-notifications library."""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="platformq-notifications",
    version="1.0.0",
    author="PlatformQ Team",
    author_email="team@platformq.io",
    description="Multi-channel notification library for PlatformQ services",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/platformq/platformq-notifications",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.8",
    install_requires=[
        "platformq-shared>=1.0.0",
        "httpx>=0.24.0",
        "pydantic>=2.0.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-asyncio>=0.21.0",
            "pytest-cov>=4.0.0",
            "black>=23.0.0",
            "ruff>=0.0.270",
        ],
        "email": [
            "aiosmtplib>=2.0.0",  # For async email sending
        ],
        "sms": [
            "twilio>=8.0.0",  # For SMS via Twilio
        ],
    },
) 