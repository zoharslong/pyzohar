#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 22 16:30:38 2019

@author: sl
@alters: 19-03-26 sl
"""
from random import randint
from re import sub, match
from hashlib import md5 as hsh_md5


dct_ncd_int = {
    '0': ['IJ','KL','MN','OP','QR'],
    '1': ['GH','IJ','KL','MN','OP'],
    '2': ['EF','GH','IJ','KL','MN'],
    '3': ['CD','EF','GH','IJ','KL'],
    '4': ['AB','CD','EF','GH','IJ'],
    '5': ['ZY','XW','VU','TS','AB'],
    '6': ['XW','VU','TS','RQ','CD'],
    '7': ['VU','TS','RQ','AB','EF'],
    '8': ['TS','RQ','PO','CD','GH'],
    '9': ['RQ','PO','AB','EF','ZY'],
}


def fnc_ncd_mbl_str(str_mbl, dct_ncd=None):
    """
    数字类字符串加密
    """
    try:
        if match("^\d+?$", str_mbl) is not None:
            dct_ncd = dct_ncd_int if dct_ncd is None else dct_ncd
            int_vrt = randint(0,len(dct_ncd['0'])-1)
            lst_vrt = [dct_ncd[str(i)][int_vrt] for i in range(0,10)]
            for i in range(0,10):
                str_mbl = sub(str(i), lst_vrt[i], str_mbl)
            str_mbl += str(int_vrt)
        return str_mbl
    except TypeError: return str_mbl


def fnc_dcd_mbl_str(str_ncd, dct_ncd=None):
    """
    数字类字符串解密
    """
    try:
        if match("^[A-Z]+[0-9]+$", str_ncd) is not None:
            dct_ncd = dct_ncd_int if dct_ncd is None else dct_ncd
            int_vrt = int(sub('[A-Z]', '', str_ncd))
            lst_vrt = [dct_ncd[str(i)][int_vrt] for i in range(0,10)]
            str_tmp = sub('[0-9]', '', str_ncd)
            for i in range(0, 10):
                str_tmp = sub(lst_vrt[i], str(i), str_tmp)
            str_ncd = str_tmp if match("^\d+?$", str_tmp) else str_ncd
        return str_ncd
    except TypeError: return str_ncd


def fnc_ncd_str_hsh(str_mpt, str_ncd='slong'):
    """
    使用哈希MD5对文本进行加密后转化为唯一标记
    :param str_mpt: a str waiting for encoding
    :param str_ncd: the encoding code
    :return: a str of code
    """
    if type(str_mpt) is str:
        hsh = hsh_md5(bytes(str_ncd, encoding='utf-8'))
        hsh.update(bytes(str_mpt, encoding="utf-8"))
        return hsh.hexdigest()
    # else: return str_mpt
