#!/usr/bin/env python

from setuptools import setup

setup(
    name="gentleman",
    packages=["gentleman"],
    setup_requires=["vcversioner"],
    vcversioner={},
    install_requires=open("requirements.txt").read().split("\n"),
    author="Corbin Simpson",
    author_email="cds@corbinsimpson.com",
    description="Ganeti RAPI client",
    long_description=open("README.rst").read(),
    license="GPLv2 or later",
    url="http://github.com/MostAwesomeDude/gentleman",
)
