#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 22 16:30:38 2019
@author: zoharslong
"""
from platform import system as chk_sys
from os.path import realpath as os_realpath
from os import listdir as os_listdir
from pyzohar.sub_slt_bsc.bsz import stz, lsz, dcz, dtz
from pyzohar.sub_slt_bsc.dfz import dfz


__all__ = [
    'stz',
    'lsz',
    'dcz',
    'dtz',
    'dfz',
]   # https://www.cnblogs.com/tp1226/p/8453854.html

chk_sys = chk_sys()
lst_xcd = ['samples', '__pycache__', '__init__.py']  # list for exclude
lst_slt = [i for i in os_listdir(os_realpath(__file__).replace('__init__.py', '')) if i not in lst_xcd]
__version__ = '0.1.0'
__doc__ = """
pyzohar
=====================================================================
** pyzohar ** is a Python package for data manipulation
** main features
  - basic data type pre-processing.
  - pandas.DataFrame manipulating.
  - data input and output.
  - fast modelling.
"""
print('info: pyzohar_%s initiation succeeded.' % __version__)
print('info: slots stand by: %s' % lst_slt)


