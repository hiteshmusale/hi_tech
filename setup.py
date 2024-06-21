from setuptools import find_packages, setup

setup(
    name="bookings_etl",
    version="0.1.0",
    description="A Dagster project for booking ETL.",
    author="Chris Poulter",
    author_email="chris@shortstaysuccess.com",
    packages=find_packages(exclude=["bookings_etl_tests"]),
    install_requires=[
        "dagster==1.7.10",
        "dagit==1.7.10",
        "python-dotenv>=0.21.0",
        "supabase>=2.5.1",
        "dagster-webserver==1.7.10",
        "pendulum==3.0.0",
        "time-machine>=2.5.0",
        "pandas>=1.3.0",
        "numpy>=1.21.0"
    ],
    extras_require={
        "dev": [
            "pytest>=6.2.5"
        ]
    },
)