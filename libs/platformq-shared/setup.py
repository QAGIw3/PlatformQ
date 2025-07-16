from setuptools import setup, find_packages

setup(
    name='platformq-shared',
    version='0.1.0',
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    include_package_data=True,
    install_requires=[
        'fastapi',
        'pydantic',
        'requests',
        'opentelemetry-api',
        'opentelemetry-sdk',
        'opentelemetry-instrumentation-fastapi',
        'hvac',
    ],
) 