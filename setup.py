#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-firebird",
    version="0.1.0",
    description="Singer.io tap for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_firebird"],
    install_requires=[
        # NB: Pin these to a more specific version for tap reliability
        "singer-python==5.12.2",
        "requests",
        "sqlalchemy",
        "fdb==2.0.2"
    ],
    entry_points="""
    [console_scripts]
    tap-firebird=tap_firebird:main
    """,
    packages=["tap_firebird"],
    package_data = {
        "schemas": ["tap_firebird/schemas/*.json"]
    },
    include_package_data=True,
)
