#!/usr/bin/env python

from setuptools import setup

setup(
    name="gentleman",
    setup_requires=["vcversioner"],
    vcversioner={},
    packages=["gentleman"],
    author="Corbin Simpson",
    author_email="cds@corbinsimpson.com",
    description="Ganeti RAPI interface",
    long_description=open("README.rst").read(),
    license="GPLv2+",
    url="http://github.com/MostAwesomeDude/gentleman",
)
