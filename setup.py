#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys

from codecs import open
from datetime import datetime
from setuptools import setup, find_packages

minor_version = "0.0.1"


if sys.argv[-1] == "publish":
    os.system("python setup.py sdist upload")
    sys.exit()


if os.environ.get("TRAVIS_BRANCH") == "release":
    revision = "r" + os.environ.get("TRAVIS_BUILD_NUMBER")
    version = minor_version + revision
else:
    date = datetime.now().strftime(".dev%Y%m%d%H%M%S")
    revision = os.environ.get("REVISION", date)
    version = minor_version + revision

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
    package_data={"": ["LICENSE"], "tubing": ["VERSION"]},
    packages=find_packages(exclude='tests'),
    include_package_data=True,
    install_requires=[],
    license="MIT",
    zip_safe=False,
    classifiers=(
        'Development Status :: 3 - Alpha',

        'Intended Audience :: Developers',

        'License :: OSI Approved :: MIT License',

        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.2',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
    ),
    extras_require={
        "build": [
            "nose",
            "unittest2",
        ],
        "s3": [
            "boto3",
        ],
        "elasticsearch": [
            "requests",
        ],
    },
)
