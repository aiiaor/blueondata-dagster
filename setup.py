from setuptools import find_packages, setup

setup(
    name="ecommerce",
    packages=find_packages(exclude=["ecommerce_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
