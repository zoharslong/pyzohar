#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 22 16:30:38 2019
@author: zoharslong
"""
from platform import system as chk_sys
chk_sys = chk_sys()
__VERSION__ = '0.0.1'
if __name__ == '__main__':
    print('info: running as a new program.')
else:
    __all__ = ['bsz', 'dfz', 'ioz']  # https://www.cnblogs.com/tp1226/p/8453854.html
    print('info: pyzohar_%s initiation succeeded.' % __VERSION__)
    print('info: needs pymongo_3.9.0, pymysql_0.9.3, fake-useragent_0.1.11, openpyxl3.0.5, urllib3-1.25.10, requests2.24.0.')
