#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on 2021.03.18
setup for package.
@author: zoharslong
"""
from setuptools import setup, find_packages

setup(
    name="pyzohar",
    version="0.0.1",
    author="zoharslong",
    author_email="zoharslong@hotmail.com",
    description="a private package on data pre-processing.",
    url="https://www.xzzsmeadow.com/",
    license="MIT",
    packages=find_packages(),
    classifiers=[
        'Development Status :: 3 - Alpha',  # {3:Alpha,4:Beta,5:Production/Stable}
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Data Pre-Processing',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
    ],
    keywords='data pre-processing',
    python_requires='>=3',
    install_requires=[
        'pymongo>=3.9.0',
        'pymysql>=0.9.3',
        'fake-useragent>=0.1.11',
        'requests>=2.22.0',
    ],
)
