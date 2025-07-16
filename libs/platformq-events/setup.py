from setuptools import setup, find_packages

setup(
    name='platformq-events',
    version='0.1.0',
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    package_data={
        'platformq.events': ['schemas/*.avsc'],
    },
    include_package_data=True,
    install_requires=[
        'avro',
    ],
) 