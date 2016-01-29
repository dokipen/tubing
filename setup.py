#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys

from codecs import open
from datetime import datetime
from setuptools import setup

minor_version = "0.0.1"


if sys.argv[-1] == "publish":
    os.system("python setup.py sdist upload")
    sys.exit()


packages = [
    "tubing",
]


requires = []


date = datetime.now().strftime("b%Y%m%d_%H%M%S")
revision = os.environ.get("REVISION", date)
version = "{}{}".format(minor_version, date)
with open("tubing/VERSION", "w", "utf-8") as f:
    f.write(version)


with open("README.rst", "r", "utf-8") as f:
    readme = f.read()


setup(
    name="tubing",
    version=version,
    description="Python I/O pipe utilities",
    long_description=readme,
    author="Bob Corsaro",
    author_email="rcorsaro@gmail.com",
    url="http://github.com/dokipen/tubing",
    packages=packages,
    package_data={"": ["LICENSE"], "tubing": ["VERSION"]},
    package_dir={"tubing": "tubing"},
    include_package_data=True,
    install_requires=requires,
    license="MIT",
    zip_safe=False,
    classifiers=(
    ),
    extras_require={
        "build": [
            "nose",
        ],
    },
)
