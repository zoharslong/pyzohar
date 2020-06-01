#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 22 16:30:38 2019
@author: zoharslong
"""


__VERSION__ = '0.0.1'

if __name__ == '__main__':
    print('info: running as a new program.')
else:
    print('info: pyzohar_%s initiation succeeded.' % __VERSION__)
    print('info: needs pymongo_3.9.0, pymysql_0.9.3, fake-useragent_0.1.11.')
