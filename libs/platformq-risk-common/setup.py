from setuptools import setup, find_packages

setup(
    name="platformq-risk-common",
    version="0.1.0",
    package_dir={"": "src"},
    packages=find_packages("src"),
    install_requires=[
        "numpy>=1.20.0",
        "numba>=0.55.0",  # For SIMD optimizations
        "cupy-cuda11x>=10.0.0",  # For GPU acceleration (optional)
        "pandas>=1.3.0",
        "scipy>=1.7.0",
        "scikit-learn>=1.0.0",
    ],
    extras_require={
        "gpu": ["cupy-cuda11x>=10.0.0"],
    }
) 