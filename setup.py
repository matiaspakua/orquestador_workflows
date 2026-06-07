from setuptools import setup, find_packages

setup(
    name="orquestador-workflows",
    version="1.0.0",
    packages=find_packages(include=["common", "tests_support"]),
    python_requires=">=3.11",
)
