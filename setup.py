from setuptools import find_packages, setup

setup(
    name="dagster_etl_enrich_demo",
    packages=find_packages(exclude=["dagster_etl_enrich_demo_tests"]),
    install_requires=[
        "dagster",
        "dagster-snowflake-pandas",
        "dagster-aws",
        "pandas",
        "openpyxl", 
        "responses"
    ],
    extras_require={"dev": ["dagit", "pytest"]},
)
