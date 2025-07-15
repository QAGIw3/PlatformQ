from setuptools import setup, find_packages

setup(
    name="platformqctl",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "click",
    ],
    entry_points={
        "console_scripts": [
            "platformqctl = platformqctl.main:cli",
        ],
    },
) 