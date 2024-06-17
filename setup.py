from setuptools import find_packages, setup

setup(
    name="bookings_etl",
    packages=find_packages(exclude=["bookings_etl_tests"]),
    install_requires=[
        "dagster",
        "pandas",
        "dagster-cloud",
        "supabase-py",
        "numpy"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
