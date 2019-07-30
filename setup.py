import os
import sys

from setuptools import setup, find_packages


open_kwds = {}
if sys.version_info > (3,):
    open_kwds["encoding"] = "utf-8"

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="iterlib",
    version="1.1.5",
    description="Parallel and concurrent iterators",
    long_description=long_description,
    long_description_content_type='text/markdown',
    classifiers=[
        "Programming Language :: Python :: 3",
    ],
    keywords="iterators, generators, threading, multiprocessing, map",
    author="Rohit Singh",
    author_email="singhrohit2@hotmail.com",
    url="https://github.com/rohits2/iterlib",
    license="BSD",
    packages=find_packages(exclude=["ez_setup", "examples", "tests"]),
    include_package_data=True,
    zip_safe=False,
    install_requires=["loguru"],
    extras_require={
        "dev": ["check-manifest"],
        "test": ["coveralls", "pytest-cov", "pydocstyle", "pytest-timeout"],
    },
)