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
    # Don't specify install_requires - use requirements.txt instead
    # This avoids conflicts when pip install . is run after pip install -r requirements.txt
)
