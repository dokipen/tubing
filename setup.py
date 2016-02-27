#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import os.path
import sys

from codecs import open
from datetime import datetime
from setuptools import setup, find_packages

minor_version = "0.0.2"


if sys.argv[-1] == "publish":
    os.system("python setup.py sdist upload")
    sys.exit()


VERSIONFILE = os.path.join(os.path.dirname(__file__), 'tubing/VERSION')
if os.path.exists(os.path.join(os.path.dirname(__file__), 'tubing/VERSION')):
    with open(VERSIONFILE, "r", "utf-8") as f:
        version = f.read().strip()
else:
    if os.environ.get("TRAVIS_BRANCH") == "release":
        revision = "r" + os.environ.get("TRAVIS_BUILD_NUMBER")
        version = minor_version + revision
    else:
        date = datetime.now().strftime(".dev%Y%m%d%H%M%S")
        revision = os.environ.get("REVISION", date)
        version = minor_version + revision

with open(VERSIONFILE, "w", "utf-8") as f:
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
    install_requires=[
        "requests",
        "urllib3",
    ],
    license="MIT",
    zip_safe=False,
    classifiers=(
        'Development Status :: 3 - Alpha',

        'Intended Audience :: Developers',

        'License :: OSI Approved :: MIT License',

        'Programming Language :: Python',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.2',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy',
    ),
    extras_require={
        "build": [
            "yapf",
            "pyflakes",
            "nose",
            "unittest2",
            "coveralls",
            "boto3",
        ],
        "s3": [
            "boto3",
        ],
        "elasticsearch": [],
        "docs": [
            "sphinx",
            "sphinx_rtd_theme",
        ],
        "ujson": [
            "ujson",
        ],
    },
)
