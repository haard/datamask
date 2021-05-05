#!/usr/bin/env python3

from distutils.core import setup

try:
    longdesc = open("README.md", "rt").read()
except:  # Just don't break if we're running this some funny way
    longdesc = None

setup(
    name="pgdatacleaner",
    version="1.0",
    description="Data cleaning/masking for PostgreSQL databases",
    long_description=longdesc,
    author="Fredrik Håård",
    author_email="fredrik@metallapan.se",
    url="https://github.com/haard/pgdatacleaner",
    packages=["pgdatacleaner"],
    entry_points={
        "console_scripts": [
            "dataclean=pgdatacleaner.cleaner:main",
            "datadict=pgdatacleaner.datadict:main",
        ]
    },
    install_requires=["psycopg2-binary"],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.7",
    ],
)
