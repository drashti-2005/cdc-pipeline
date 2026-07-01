"""
Minimal setup.py to make the src package importable
when running from the project root.
"""
from setuptools import setup, find_packages

setup(
    name="cdc-pipeline",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "kafka-python",
        "psycopg2-binary",
        "boto3",
        "prometheus-client",
        "pyyaml",
        "python-dotenv",
    ],
)
