#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 22 16:30:38 2019
@author: zoharslong
"""
from platform import system as chk_sys
chk_sys = chk_sys()
__VERSION__ = '0.0.1'
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

if __name__ == '__main__':
    print('info: running as a new program.')
else:
    from pyzohar.bsz import lsz, dtz, dcz, stz
    from pyzohar.dfz import dfz
    from pyzohar.mdz import mdz
    __all__ = ['stz', 'lsz', 'dcz', 'dtz', 'dfz', 'mdz']    # https://www.cnblogs.com/tp1226/p/8453854.html
    print('info: pyzohar_%s initiation succeeded.' % __VERSION__)
