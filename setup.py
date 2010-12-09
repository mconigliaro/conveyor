#!/usr/bin/env python
# -*- coding: utf-8 -*-

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


setup(
    name = 'conveyor',
    version = '0.1.0',
    packages = ['conveyor'],
    scripts = ['bin/conveyor', 'bin/hoist'],

    install_requires = ['nose', 'setuptools', 'zookeeper'],

    package_data = {
      '': ['*.rst']
    },

    author = 'Michael T. Conigliaro',
    author_email = 'mike [at] conigliaro [dot] org',
    description = 'A simple continuous deployment framework built on top of Apache Zookeeper',
    url = 'http://github.com/mconigliaro/conveyor',

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
