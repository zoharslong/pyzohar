#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on 2021.03.18
setup for package.
@author: zoharslong
"""
from setuptools import setup, find_packages
from os.path import join as os_join, abspath as os_abspath, dirname as os_dirname

here = os_abspath(os_dirname(__file__))
with open(os_join(here, 'README.md')) as f:
    README = f.read()

setup(
    name="pyzohar",
    version="0.1.6",
    author="zoharslong",
    author_email="zoharslong@hotmail.com",
    description="a private package on data pre-processing.",
    long_description=README,
    url="https://www.xzzsmeadow.com/",
    license="MIT",
    classifiers=[
        'Development Status :: 3 - Alpha',  # {3:Alpha,4:Beta,5:Production/Stable}
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
    ],
    packages=find_packages(),
    keywords='data pre-processing',
    python_requires='>=3',
    install_requires=[
        'numpy>=1.18.1',
        'pandas>=1.0.1',
        'pymongo>=3.9.0',
        'pymysql>=0.9.3',
        'fake-useragent>=0.1.11',
        'requests>=2.22.0',
        'openpyxl>=3.0.3',  # excel files resolving
        'urllib3>=1.25.8',  # some error type of http requests
    ],
    package_data={'pyzohar': ['samples/*.*']},
    include_package_data=True,
)
