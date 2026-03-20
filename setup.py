from setuptools import setup, find_packages

setup(
    name="dbt-looker-sync",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "typer",
        "lkml",
        "rich",
    ],
    entry_points={
        "console_scripts": [
            "dbt-looker-sync = app.main:app",
        ],
    },
)
