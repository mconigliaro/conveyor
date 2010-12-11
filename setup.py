#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os

try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup

import conveyor

def read(file):
    with open(os.path.abspath(os.path.join(os.path.dirname(__file__), file))) as f:
        result = f.read()
    f.closed
    return result

setup(
    name = conveyor.__name__.lower(),
    version = conveyor.__version__,
    packages = find_packages(),
    scripts = ['bin/conveyor', 'bin/hoist'],

    install_requires = ['nose', 'setuptools', 'zookeeper'],

    author = conveyor.__author__,
    author_email = conveyor.__author_email__,
    description = 'A simple continuous deployment framework built on top of Apache Zookeeper',
    long_description = read('README.rst'),
    url = conveyor.__url__,

    keywords = 'continuous deployment',
    classifiers = [
        "Development Status :: 4 - Beta",
        "Environment :: Console",
        "Intended Audience :: System Administrators",
        "License :: OSI Approved :: GNU General Public License (GPL)",
        "Natural Language :: English",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 2.6",
        "Topic :: System :: Software Distribution"
    ]
)
