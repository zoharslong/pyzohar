#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 22 16:30:38 2019
basic type's alteration.
@author: zoharslong
"""
from datetime import datetime as dt_datetime
from datetime import date as typ_dt_date, time as typ_dt_time, timedelta as typ_dt_timedelta
from time import struct_time as typ_tm_structtime, strftime as tm_strftime, mktime as tm_mktime
from bson.objectid import ObjectId as typ_ObjectId  # _id from mongobd
from calendar import monthrange     # how many days in any month
from re import search as re_search, sub as re_sub, match as re_match
from math import isnan as math_isnan
from collections import Iterable
from random import randint
from hashlib import md5 as hsh_md5
from numpy import array as np_array, ndarray as typ_np_ndarray
from pandas.core.series import Series as typ_pd_Series                  # 定义series类型
from pandas.core.frame import DataFrame as typ_pd_DataFrame             # 定义dataframe类型
from pandas.core.indexes.base import Index as typ_pd_Index              # 定义dataframe.columns类型
from pandas.core.indexes.range import RangeIndex as typ_pd_RangeIndex   # 定义dataframe.index类型
from pandas._libs.tslibs.nattype import NaTType as pd_NaT
from pandas._libs.tslib import Timestamp as typ_pd_Timestamp
from pandas import DataFrame as pd_DataFrame
from pandas import to_datetime as pd_to_datetime
dct_ncd_int = {
    '0': ['IJ', 'KL', 'MN', 'OP', 'QR'],
    '1': ['GH', 'IJ', 'KL', 'MN', 'OP'],
    '2': ['EF', 'GH', 'IJ', 'KL', 'MN'],
    '3': ['CD', 'EF', 'GH', 'IJ', 'KL'],
    '4': ['AB', 'CD', 'EF', 'GH', 'IJ'],
    '5': ['ZY', 'XW', 'VU', 'TS', 'AB'],
    '6': ['XW', 'VU', 'TS', 'RQ', 'CD'],
    '7': ['VU', 'TS', 'RQ', 'AB', 'EF'],
    '8': ['TS', 'RQ', 'PO', 'CD', 'GH'],
    '9': ['RQ', 'PO', 'AB', 'EF', 'ZY'],
}
dct_tms_fTr = {
    '%Y-%m-%d %H:%M:%S ': '^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2} [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2} $',
    '%Y-%m-%d %H:%M:%S': '^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2} [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}$',
    '%Y-%m-%d %H:%M:%S.%f': '^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2} [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}[.][0-9]+',
    '%Y-%m-%d': '^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}$',
    '%Y/%m/%d %H:%M:%S': '^[0-9]{4}/[0-9]{1,2}/[0-9]{1,2} [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}$',
    '%Y/%m/%d': '^[0-9]{4}/[0-9]{1,2}/[0-9]{1,2}$',
    '%Y.%m.%d %H:%M:%S': '^[0-9]{4}[.][0-9]{1,2}[.][0-9]{1,2} [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}$',
    '%Y.%m.%d': '^[0-9]{4}[.][0-9]{1,2}[.][0-9]{1,2}$',
    '%Y年%m月%d日 %H:%M:%S': '^[0-9]{2,4}年[0-9]{1,2}月[0-9]{1,2}日 [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}$',
    '%Y年%m月%d日 %H时%M分%S秒': '^[0-9]{2,4}年[0-9]{1,2}月[0-9]{1,2}日 [0-9]{1,2}时[0-9]{1,2}分[0-9]{1,2}秒$',
    '%Y年%m月%d日': '^[0-9]{2,4}年[0-9]{1,2}月[0-9]{1,2}日$',
    'int': '^[0-9]+[.]{0}[0-9]*$',
    'float': '^[0-9]+[.]{1}[0-9]*$',
    '%Yw%w': '^[0-9]{4}[w][0-9]{2}$',
    '%Ym%m': '^[0-9]{4}[m][0-9]{2}$',
    '%yw%w': '^[0-9]{2}[w][0-9]{2}$',
    '%ym%m': '^[0-9]{2}[m][0-9]{2}$',
}


class stz(str):
    """
    type str altered by zoharslong.
    >>> stz('1989-02-14').add_rgx(rtn=True)     # generate regex of the string content
    ['^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}$']
    >>> stz('1989-02-14').add_fmt(rtn=True)     # generate format of the string content
    ['%Y-%m-%d']
    >>> stz('15814079977').ncd_int(rtn=True)
    'IJXWRQIJCDKLTSPOPOTSTS1'
    >>> stz('IJXWRQIJCDKLTSPOPOTSTS1').dcd_int(rtn=True)
    '15814079977'
    >>> stz('15814079977').ncd_md5(rtn=True)
    'f8ceb85867e2df236e11211366be6479'
    >>> stz('15814079977').ncd_cvr([3,7])
    '158****9977'
    """
    def __init__(self, val=''):
        """
        create a new string object from the given object.
        :param val: 指定的值
        """
        super().__init__()
        self.__val, self.rgx, self.fmt, self.cod = val, None, None, None

    def add_rgx(self, *, lst_rgx=None, rtn=False):
        """
        match regex.
        从传入的正则列表中筛选出符合类的正则表达式组成列表.
        :param lst_rgx: 参与匹配的正则表达式列表
        :param rtn: return or not, default False
        :return: None
        """
        lst_rgx = list(dct_tms_fTr.values()) if lst_rgx is None else lst_rgx
        rgx_xpt = [i_rgx for i_rgx in lst_rgx if re_search(i_rgx, self)]
        self.rgx = rgx_xpt
        if rtn:
            return self.rgx

    def add_fmt(self, *, dct_rgx=None, rtn=False):
        """
        match format.
        从传入的格式-正则字典中筛选出符合类的格式组成列表.
        :param dct_rgx: 参与匹配的格式-正则表达式字典
        :param rtn: return or not, default False
        :return: None
        """
        if self.rgx is None or len(self.rgx) <= 0:
            self.add_rgx()
        self.fmt = [] if self.fmt is None else self.fmt
        dct_rgx = dct_tms_fTr if dct_rgx is None else dct_rgx
        for i in range(len(self.rgx)):
            self.fmt.append(list(dct_rgx.keys())[list(dct_rgx.values()).index(self.rgx[i])])
        if rtn:
            return self.fmt

    def ncd_int(self, *, dct_ncd=None, rtn=False):
        """
        数字类字符串加密，特别用于电话号码.
        :param dct_ncd: 默认转换字典dct_ncd_int
        :param rtn: if rtn, return the target self.cod, default False
        :return: return self.cod
        """
        try:
            if re_match("^\d+?$", self) is not None:
                self.cod = self
                dct_ncd = dct_ncd_int if dct_ncd is None else dct_ncd
                int_vrt = randint(0, len(dct_ncd['0']) - 1)
                lst_vrt = [dct_ncd[str(i)][int_vrt] for i in range(0, 10)]
                for i in range(0, 10):
                    self.cod = re_sub(str(i), lst_vrt[i], self.cod)
                self.cod += str(int_vrt)
            if rtn:
                return self.cod
        except TypeError:
            if rtn:
                return self

    def dcd_int(self, *, dct_dcd=None, rtn=False):
        """
        数字类字符串解密.
        :param dct_dcd: 默认转换字典dct_ncd_int
        :param rtn: if rtn, return the target self.cod, default False
        :return: return self.cod
        """
        try:
            if re_match("^[A-Z]+[0-9]+$", self) is not None:
                self.cod = self
                dct_ncd = dct_ncd_int if dct_dcd is None else dct_dcd
                int_vrt = int(re_sub('[A-Z]', '', self.cod))
                lst_vrt = [dct_ncd[str(i)][int_vrt] for i in range(0, 10)]
                self.cod = re_sub('[0-9]', '', self.cod)
                for i in range(0, 10):
                    self.cod = re_sub(lst_vrt[i], str(i), self.cod)
            if rtn:
                return self.cod
        except TypeError:
            if rtn:
                return self

    def ncd_md5(self, *, prm=None, rtn=False):
        """
        使用哈希MD5对文本进行加密后转化为唯一标记.
        :param prm: the encoding code
        :param rtn: if rtn is True, return self.cod
        :return: a str of code
        """
        prm = 'zohar' if not prm else prm   # 默认的哈希参数
        self.cod = self
        if type(self) in [str, stz]:
            hsh = hsh_md5(bytes(prm, encoding='utf-8'))
            hsh.update(bytes(self.cod, encoding="utf-8"))
            self.cod = hsh.hexdigest()
        if rtn:
            return self.cod

    def ncd_cvr(self, lst_cvr, *, rtn=True):
        """
        encode by covering '*'
        :param lst_cvr: cover targeted index by *
        :param rtn: if rtn is True, return self.cod
        :return: a str of code
        """
        bgn = lst_cvr[0]
        end = len(self) + 1 + lst_cvr[1] if lst_cvr[1] < 0 else lst_cvr[1]
        lst = list(self)
        for i in range(bgn, end):
            if len(lst) >= i:
                lst[i] = '*'
        self.cod = ''.join(lst)
        if rtn:
            return self.cod


class lsz(list):
    """
    type list altered by zoharslong.
    >>> lsz([[1,2,3],[1,2]]).edg_of_len(rtn=True)
    [2, 3]
    >>> lsz({'A':1,'B':2,'C':3}).typ_to_lst(rtn=True)
    [('A', 1), ('B', 2), ('C', 3)]
    >>> lsz([1,2,3]).lst_to_typ('dict', ['A','B','C'], rtn=True)
    [{'A': 1, 'B': 2, 'C': 3}]
    >>> lsz([1,2,3]).cpy_tal(5, rtn=True)
    [1, 2, 3, 3, 3]
    >>> lsz([1,2,3]).mrg_cll(['_A','_B','_C'], rtn=True)
    ['1_A', '2_B', '3_C']
    >>> lsz([[1,2],3,[4,5,[6,7,[8,9]]]]).nfd(rtn=True)
    [1, 2, 3, 4, 5, 6, 7, 8, 9]
    >>> lsz().mrg('inter', [1, 2, 3], [2, 3, 4], rtn=True)
    [2, 3]
    >>> lsz(['O']).chk_dtf_dtp(pd_DataFrame([{"A":1,"B":"a"},{"A":2,"B":"b"}]), rtn=True)
    ['B']
    """
    __slots__ = ('__seq', 'typ', 'len', 'len_min', 'len_max')
    lst_typ_lsz = [
        str,
        stz,
        int,
        float,
        list,
        dict,
        tuple,
        typ_np_ndarray,
        typ_pd_DataFrame,
        typ_pd_Series,
        typ_pd_Index,
        typ_pd_RangeIndex,
    ]   # lsz.seq's type

    def __init__(self, seq=None, spr=True, lst=False):
        """
        create a new list object from the given object.
        :param seq: first, save target into lsz.seq
        :param spr: let lsz = lsz.seq or not, default False
        :param lst:
        """
        super().__init__()
        self.__seq, self.typ, self.len, self.len_max, self.len_min = None, None, None, None, None
        self.__init_rst(seq, spr, lst)

    def __init_rst(self, seq=None, spr=False, lst=False):
        """
        private reset initiation.
        :param seq: a list content in any type, None for []
        :param spr: let lsz = lsz.seq or not, default False
        :return: None
        """
        if self.seq is None:
            self.seq = seq if seq is not None else []
        if lst:
            self.typ_to_lst()
        if spr:
            self.spr_nit()

    def spr_nit(self, rtn=False, prn=False):
        """
        super initiation. let lsz = lsz.seq.
        :param rtn: return lsz or not, default False
        :param prn: if print or not, default False
        :return: if rtn is True, return lsz
        """
        try:
            super(lsz, self).__init__(self.__seq)
        except TypeError:
            if prn:
                print('info: %s cannot convert to list.' % (str(self.__seq)[:8]+'..'))
        if rtn:
            return self

    @property
    def seq(self):
        """
        @property get & set lsz.seq.
        :return: lsz.seq
        """
        return self.__seq

    @seq.setter
    def seq(self, seq):
        """
        lsz.seq = seq.
        :param seq: a sequence to import.
        :return: None
        """
        if seq is None or type(seq) in self.lst_typ_lsz:
            self.__seq = seq
            self.__attr_rst()
        else:
            raise TypeError('seq\'s type %s is not available.' % type(seq))

    def edg_of_len(self, rtn=False):
        """
        get the max and min length of cells in lsz.seq.
        :param rtn: return the result or not, default False
        :return: if rtn is True, return the result in format [min, max]
        """
        lst_len = [len(i) for i in self.seq if isinstance(i, Iterable)]
        self.len_max = max(lst_len) if lst_len != [] else None
        self.len_min = min(lst_len) if lst_len != [] else None
        if rtn:
            return [self.len_min, self.len_max]

    def __attr_rst(self, prn=False):
        """
        reset attributes lsz.typ.
        :return: None
        """
        self.typ = type(self.__seq)
        try:
            self.len = len(self.__seq)
            self.edg_of_len()
        except TypeError:
            if prn:
                print('info: %s is not available for __len__().' % (str(self.__seq)[:8] + '..'))

    def typ_to_lst(self, *, spr=False, rtn=False, prm='record'):
        """
        alter lsz.seq's type to list.
        :param spr: let lsz = lsz.seq or not, default False
        :param rtn: return the result or not, default False
        :param prm: if lsz.seq is pd.DataFrame, method in ['dict', 'list', 'series', 'split', 'records', 'index']
        :return: if rtn is True, return the final result
        """
        if self.typ in [list]:
            pass
        elif self.typ in [dict]:
            self.seq = list(self.seq.items())
        elif self.typ in [tuple, typ_pd_Series, typ_pd_Index, typ_pd_RangeIndex]:
            self.seq = list(self.seq)
        elif self.typ in [typ_np_ndarray]:
            self.seq = self.seq.tolist()
        elif self.typ in [typ_pd_DataFrame]:
            self.seq = self.seq.to_dict(orient=prm)
        else:
            self.seq = [self.seq]
        if spr:
            self.spr_nit()
        if rtn:
            return self.seq

    def lst_to_typ(self, str_typ='list', *args, spr=False, rtn=False):
        """
        alter list to other type.
        :param str_typ: target data type
        :param args: if needs other list to merge, import here, len(args) in [1,2]
        :param spr: let lsz = lsz.seq or not, default False
        :param rtn: return the result or not, default False
        :return: if rtn is True, return the final result
        """
        self.typ_to_lst()
        if str_typ.lower() in ['list', 'lst']:
            pass
        elif str_typ.lower() in ['listt', 'lstt', 't']:
            self.seq = np_array(self.seq).T.tolist()
        elif str_typ.lower() in ['str']:  # 用于将list转化为不带引号的字符串:[1,'a',...] -> "1,a,..."
            self.seq = str(self.seq).replace("'", '')[1:-1]
        elif str_typ.lower() in ['dict', 'dct', '[dict]', '[dct]', 'listdict', 'lstdct']:  # [a,b]+[1,1]->[{a:1},{b:1}]
            self.seq = args[-1] if not self.seq else self.seq  # 当lsz.seq为空时，从*args中依次取值
            self.seq = pd_DataFrame([self.seq], columns=lsz(args[0]).typ_to_lst(rtn=True)).to_dict(orient='record')
        elif str_typ.lower() in ['listtuple', 'lsttpl', 'list_tuple', 'lst_tpl']:   # [a,b,..]+[1,1,..] ->[(a,1),..]
            if not self.seq:  # 当lsz.seq为空时，从*args中依次取值
                int_bgn, self.seq = -1, args[0]
            else:
                int_bgn = 0
            lst_prm = lsz(args[int_bgn]).cpy_tal(self.len, rtn=True)
            self.seq = [(i, lst_prm[self.seq.index(i)]) for i in self.seq]
        if spr:
            self.spr_nit()
        if rtn:
            return self.seq

    def cpy_tal(self, flt_len, *, spr=False, rtn=False):
        """
        copy the last cell to tails until lsz.len equals to flt_len.
        :param flt_len: the target length of lsz after running this def
        :param spr: let lsz = lsz.seq or not, default False
        :param rtn: return the result or not, default False
        :return: if rtn is True, return the final result
        """
        self.typ_to_lst()
        lst_xpt = self.seq.copy()
        while len(lst_xpt) < flt_len:
            lst_xpt.append(self.seq[-1])
        self.seq = lst_xpt
        if spr:
            self.spr_nit()
        if rtn:
            return self.seq

    def mrg_cll(self, *args, spr=False, rtn=False):
        """
        in lsz.seq, cell to cell merging.
        structuring: ['a','b',...] + ['x'] -> ['ax','bx',...].
        :param args: lists to merge in method cell by cell
        :param spr: let lsz = lsz.seq or not, default False
        :param rtn: return the result or not, default False
        :return: if rtn is True, return the final result
        """
        self.typ_to_lst()
        flt_max = lsz(args).edg_of_len(True)[1]
        if not self.seq:    # 当lsz.seq为空时，从*args中依次取值
            int_bgn, self.seq = 1, args[0]
        else:               # 当lsz.seq有值时，该list也加入计算
            int_bgn = 0
        self.cpy_tal(flt_max)
        for i in range(int_bgn, len(args)):
            lst_mrg = lsz(args[i]).cpy_tal(flt_max, rtn=True)
            self.seq = [str(self.seq[i]) + str(lst_mrg[i]) for i in range(self.len)]
        if spr:
            self.spr_nit()
        if rtn:
            return self.seq

    def nfd(self, spr=False, rtn=False):
        """
        unfold lists in lsz.seq.
        >>> lsz([[1,2],3,[4,5,[6,7,[8,9]]]]).nfd(False,True)
        [1, 2, 3, 4, 5, 6, 7, 8, 9]
        :param spr: let lsz = lsz.seq or not, default False
        :param rtn: return the result or not, default False
        :return: if rtn is True, return the final result
        """
        self.typ_to_lst()
        while [True for i_cll in self.seq if type(i_cll) in [list, tuple]]:     # check if there is any cell in list
            lst_nfd = []
            for i_cll in self.seq:
                if type(i_cll) in [list]:
                    for i in i_cll:
                        lst_nfd.append(i)
                else:
                    lst_nfd.append(i_cll)
            self.seq = lst_nfd
        if spr:
            self.spr_nit()
        if rtn:
            return self.seq

    def mrg(self, str_mtd, *args, spr=False, rtn=False):
        """
        merge lists in method intersection, difference or union.
        >>> lsz().mrg('inter', False, True, [1, 2, 3], [2, 3, 4])
        [2, 3]
        :param str_mtd: ['inter','differ','union']
        :param args: lists to be merged
        :param spr: let lsz = lsz.seq or not, default False
        :param rtn: return the result or not, default False
        :return: if rtn is True, return the final result
        """
        self.typ_to_lst()
        if not self.seq:    # 当lsz.seq为空时，从*args中依次取值
            int_bgn, self.seq = 1, args[0]
        else:               # 当lsz.seq有值时，该list也加入计算
            int_bgn = 0
        for i in range(int_bgn, len(args)):
            if str_mtd.lower() in ['intersection', 'inter']:    # 交
                self.seq = list(set(self.seq) & set(args[i]))
            elif str_mtd.lower() in ['difference', 'differ']:   # 差
                self.seq = list(set(self.seq) ^ set(args[i]))
            elif str_mtd.lower() in ['union']:                  # 并
                self.seq = list(set(self.seq) | set(args[i]))
        if spr:
            self.spr_nit()
        if rtn:
            return self.seq

    def chk_dtf_dtp(self, dtf_chk, *, spr=False, rtn=False, prm=None):
        """
        check dataframe's columns' dtype.
        >>> lsz(['O']).chk_dtf_dtp(pd_DataFrame([{"A":1,"B":"a"},{"A":2,"B":"b"}]),rtn=True)
        ['B']
        :param dtf_chk: the target dataframe to be listing
        :param spr: let lsz = lsz.seq or not, default False
        :param rtn: return the result or not, default False
        :param prm: a list of columns' dtype, such as ['O','int64','float64',...], can be shown by dataframe.dtypes
        :return: if rtn is True, return the final result
        """
        self.typ_to_lst()
        lst_clm_dtp = []
        if self.seq is None and prm is not None:
            self.seq = lsz(prm).typ_to_lst(rtn=True)
        for i_dtp in self.seq:
            lst_clm_dtp.extend(dtf_chk.dtypes[dtf_chk.dtypes == i_dtp].index.values)
        self.seq = lst_clm_dtp
        if spr:
            self.spr_nit()
        if rtn:
            return self.seq

    def _rg_to_typ(self, *, spr=False, rtn=False, prm=None, prn=False):
        """
        args in functions to list. 仅用于函数中对*args的处理.
        >>> lsz(('a',))._rg_to_typ(rtn=True)
        ['a', 'a']
        >>> lsz(({'a':'b'},))._rg_to_typ(rtn=True)
        [['a'], ['b']]
        >>> lsz((['a','b'],'x'))._rg_to_typ(prm='dct', rtn=True)
        {'a': 'x', 'b': 'x'}
        >>> lsz((['a','c'],'b'))._rg_to_typ(prm='list',rtn=True)
        [['a', 'c'], ['b', 'b']]
        >>> lsz(('a',['b','c']))._rg_to_typ(prm='list',rtn=True)
        [['a', 'a'], ['b', 'c']]
        :param prm: in ['equal', 'eql', 'dict', 'dct']
        :param spr:
        :param rtn:
        :return:
        """
        if self.seq in [None, []]:
            pass
        elif type(self.seq[0]) is dict and self.len == 1:   # args from ({a:A,b:B},) to [[a, b], [A, B]]
            str_mpt = list(self.seq[0].keys())
            str_xpt = list(self.seq[0].values())
            self.seq = [str_mpt, str_xpt]
        elif self.len == 1:                                 # args from (x,) to [x, x]
            self.typ_to_lst()
            self.cpy_tal(2)
        else:                                               # args from (x, y, ...) to [x, y, ...]
            self.typ_to_lst()
        if prm in ['dct', 'dict', 'dictionary'] and self.seq not in [None, []]:  # args from [[x,y],[a,b]] to {x:y,a:b}
            arg = self.seq.copy()
            self.seq = arg[0]
            lst_bgn = self.typ_to_lst(rtn=True)
            self.seq = arg[1]
            self.cpy_tal(len(lst_bgn))
            self.seq = self.lst_to_typ('dct', lst_bgn, rtn=True)[0]
        elif prm in ['eql', 'equal']:                       # args from [x, [a,b]] to [[x,x],[a,b]]
            arg = self.seq.copy()
            self.seq = arg[0]
            lst_bgn = self.typ_to_lst(rtn=True)
            self.seq = arg[1]
            lst_end = self.typ_to_lst(rtn=True)
            self.seq = [lst_bgn, lst_end]
            len_fnl = self.len_max
            if len(lst_bgn) < len_fnl:
                self.seq = lst_bgn.copy()
                self.cpy_tal(len_fnl)
                lst_bgn = self.seq
            elif len(lst_end) < len_fnl:
                self.seq = lst_end.copy()
                self.cpy_tal(len_fnl)
                lst_end = self.seq
            self.seq = [lst_bgn, lst_end]
        elif prn:
            print('info: prm needs ["dct","eql"] for format [{x:y}, [[x,x],[a,b]]].')
        else:
            pass
        if spr:
            self.spr_nit()
        if rtn:
            return self.seq


class dcz(dict):
    """
    type dict altered by zoharslong.
    >>> dcz().typ_to_dct(['A','B','C'], [1,0], rtn=True)
    {'A': 1, 'B': 0, 'C': 0}
    >>> dcz([1,0]).typ_to_dct(['A','B','C'], rtn=True)
    {'A': 1, 'B': 0, 'C': 0}
    >>> dcz([{'A':1},{'B':2}]).nfd(rtn=True)
    {'A': 1, 'B': 2}
    >>> dcz({'A':{'B':1,'C':2},'D':3}).nfd(rtn=True)
    {'B': 1, 'C': 2, 'D': 3}
    """
    __slots__ = ('__seq', 'typ', 'len', 'kys', 'vls')
    lst_typ_dcz = [
        str,
        stz,
        int,
        float,
        list,
        dict,
        tuple,
        typ_np_ndarray,
        typ_pd_DataFrame,
        typ_pd_Series,
        typ_pd_Index,
        typ_pd_RangeIndex,
    ]   # lsz.seq's type

    def __init__(self, seq=None, spr=True):
        """
        create a new list object from the given object.
        :param seq: first, save target into dcz.seq
        :param spr: let dcz = dcz.seq or not, default False
        """
        super().__init__()
        self.__seq, self.typ, self.len, self.kys, self.vls = None, None, None, None, None
        self.__init_rst(seq, spr)

    def __init_rst(self, seq=None, spr=False):
        """
        private reset initiation.
        :param seq: a list content in any type, None for []
        :param spr: let dcz = dcz.seq or not, default False
        :return: None
        """
        if self.seq is None:
            self.seq = seq if seq is not None else {}
        if spr:
            self.spr_nit()

    def spr_nit(self, rtn=False):
        """
        super initiation. let dcz = dcz.seq.
        :param rtn: return dcz or not, default False
        :return: if rtn is True, return dcz
        """
        try:
            self.clear()    # 由于直接super会导致dcz.seq拼接到原dcz之后，因此先调用dcz.clear()清空原dcz
            super(dcz, self).__init__(self.__seq)
        except (TypeError, ValueError):
            print('info: %s cannot convert to list.' % (str(self.__seq)[:8]+'..'))
        if rtn:
            return self

    @property
    def seq(self):
        """
        @property get & set lsz.seq.
        :return: dcz.seq
        """
        return self.__seq

    @seq.setter
    def seq(self, seq):
        """
        dcz.seq = seq.
        :param seq: a sequence to import.
        :return: None
        """
        if seq is None or type(seq) in self.lst_typ_dcz:
            self.__seq = seq
            self.__attr_rst()
        else:
            raise TypeError('seq\'s type %s is not available.' % type(seq))

    def __attr_rst(self, prn=False):
        """
        reset attributes dcz.typ.
        :return: None
        """
        self.typ = type(self.__seq)
        try:
            self.len = len(self.__seq)
        except TypeError:
            if prn:
                print('info: %s is not available for __len__().' % (str(self.__seq)[:8] + '..'))
        self.kys = list(self.__seq.keys()) if self.typ in [dict] else None
        self.vls = list(self.__seq.values()) if self.typ in [dict] else None

    def typ_to_dct(self, *args, spr=False, rtn=False):
        """
        alter dcz.seq's type from others to dict.
        :param args: if needs other objects to merge, import here, len(args) in [1,2]
        :param spr: let dcz = dcz.seq or not, default False
        :param rtn: return the result or not, default False
        :return: if rtn is True, return the final result
        """
        if self.seq and self.typ in [dict]:
            pass
        elif (self.seq and self.typ in [list, lsz]) or (not self.seq and [True for i in args if type(i) in [list]]):
            lst_clm = lsz(args[0]).typ_to_lst(rtn=True)
            # 当dcz.seq为空时，从*args中依次取值
            self.seq = args[-1] if not self.seq else self.seq           # dcz.seq为空则取末尾的args
            self.seq = lsz(self.seq).cpy_tal(len(args[0]), rtn=True)    # dcz.seq填充至长度等同于args[0]即dict的keys
            self.seq = pd_DataFrame([self.seq], columns=lst_clm).to_dict(orient='record')[0]
        if spr:
            self.spr_nit()
        if rtn:
            return self.seq

    def nfd(self, spr=False, rtn=False):
        """
        unfold dict cells inside a list or dict object.
        :param spr: let dcz = dcz.seq or not, default False
        :param rtn: return the result or not, default False
        :return: if rtn is True, return the final result in dict format {A,B,..}
        """
        dct_tmp = {}
        if self.typ in [list, lsz]:     # [{A:1},{B:2},...] -> {A:1,B:2}
            for i_dct in self.seq:
                if type(i_dct) is dict:
                    dct_tmp.update(i_dct)
        elif self.typ in [dict]:        # {A:{B:1,C:2},D:3,...} -> {B:1,C:2,D:3,...}
            for i_dct in self.seq:
                if type(self.seq[i_dct]) is dict:
                    dct_tmp.update(self.seq[i_dct])
                else:
                    dct_tmp.update({i_dct: self.seq[i_dct]})
        self.seq = dct_tmp
        if spr:
            self.spr_nit()
        if rtn:
            return self.seq


class dtz(object):
    """
    type datetime altered by zoharslong.
    >>> dtz(15212421.1).typ_to_dtt(rtn=True)    # from any type to datetime
    datetime.datetime(1970, 6, 26, 9, 40, 21)
    >>> dtz(dtz(15212421.1).typ_to_dtt(rtn=True)).dtt_to_typ('str', rtn=True)   # from datetime to any type
    '1970-06-26'
    >>> dtz(dtz(15212421.1).typ_to_dtt(rtn=True)).shf(5, rtn=True)    # 5 days after 1970-06-26 in type datetime
    datetime.datetime(1970, 7, 1, 9, 40, 21)
    >>> dtz('2019m03').prd_to_dtt(-1, rtn=True) # get the last day in 2019 March
    datetime.datetime(2019, 3, 31, 0, 0)
    >>> dtz('2019w03').prd_to_dtt(-1, rtn=True) # get the last day in 2019 3th week
    datetime.datetime(2019, 3, 31, 0, 0)
    """
    __slots__ = ('__val', 'typ', 'len', 'fmt',)
    lst_typ_dtz = [
        type,
        str,
        stz,
        int,
        float,
        list,
        pd_NaT,
        dt_datetime,
        typ_dt_date,
        typ_dt_time,
        typ_pd_Timestamp,
        typ_tm_structtime,
        typ_ObjectId
    ]   # dtz.val's type

    def __init__(self, val=None, prm=None):
        """
        initiating dtz.val, dtz.typ.
        :param val: a datetime content in any type, None for datetime.datetime.now()
        :param prm: 默认None不指定当初始化为空时返回当前时间，任意赋值（如'now'）则赋值当前时间
        """
        self.__val, self.typ, self.len, self.fmt = None, None, None, None
        self.__init_rst(val, prm)

    def __init_rst(self, val=None, prm=None):
        """
        private reset initiation.
        :param val: a datetime content in any type, None for datetime.datetime.now()
        :param prm: 默认None不指定当初始化为空时返回当前时间，任意赋值（如'now'）则赋值当前时间
        :return: None
        """
        if val == 'now' or (self.val is None and prm is not None):
            self.val = dt_datetime.now()
        else:
            self.val = val

    @property
    def val(self):
        """
        @property get & set dtz.val.
        :return: dtz.val
        """
        return self.__val

    @val.setter
    def val(self, val):
        """
        dtz.val = val.
        :param val: a value to import.
        :return: None
        """
        if val is None or type(val) in self.lst_typ_dtz:
            self.__val = val
            self.__attr_rst()
        else:
            raise TypeError('info: val\'s type %s is not available.' % type(val))

    def __attr_rst(self):
        """
        reset attributes dtz.typ, dtz.fmt.
        :return: None
        """
        self.typ = type(self.__val)
        self.len = len(self.__val) if self.typ in [str, stz] else None
        self.fmt = stz(self.__val).add_fmt(rtn=True) if self.typ == str else None

    def __str__(self):
        """
        print(className).
        :return: None
        """
        if type(self.__val) == typ_tm_structtime:
            str_xpt = 'time.struct_time'+str(tuple(self.__val))
        else:
            str_xpt = self.__val
        return '%s' % str_xpt
    __repr__ = __str__  # 调用类名的输出与print(className)相同

    def typ_to_dtt(self, rtn=False):
        """
        alter other type to datetime.datetime type.
        fit for datetime.datetime, pd.Timestamp, tm.structtime, float, int and str.
        :param rtn: return the result or not, default False
        :return: if rtn is True, return dtz.val
        """
        if self.typ in [dt_datetime]:
            pass
        elif self.val is None or self.typ in [pd_NaT]:
            self.val = None
        elif self.typ in [typ_dt_date]:
            self.val = dt_datetime.combine(self.val, typ_dt_time())
        elif self.typ in [typ_pd_Timestamp]:
            self.val = dt_datetime.combine(self.val.date(), self.val.time())
        elif self.typ in [typ_tm_structtime]:
            self.val = dt_datetime.strptime(tm_strftime('%Y-%m-%d %H:%M:%S', self.val), '%Y-%m-%d %H:%M:%S')
        elif self.typ in [typ_ObjectId]:
            self.val = tm_strftime('%Y-%m-%d %H:%M:%S', self.val.generation_time.timetuple())
            self.val = dt_datetime.strptime(self.val, '%Y-%m-%d %H:%M:%S')
        elif self.typ in [int, float]:
            if not math_isnan(self.val):    # 正常情况：非空float或int
                self.val = dt_datetime.fromtimestamp(int(str(self.val).rsplit('.')[0]))
            else:                           # 对np.nan赋空
                self.val = None
        elif self.typ in [str, stz]:
            if [True for i in self.fmt if i in ['float', 'int']]:
                self.val = dt_datetime.fromtimestamp(int(self.val.rsplit('.')[0]))
            else:
                try:                # 正常情况：匹配正则表达式后转为datatime
                    self.val = dt_datetime.strptime(self.val, self.fmt[0])
                except IndexError:  # 对无法找到合适正则匹配（如''空字符串）则赋空
                    self.val = None
                    print('info: %s cannot convert to datetime type.' % str(self.val))
        else:
            raise TypeError('type of value not in [dt_datetime, dt_date, tm_structtime, pd_Timestamp, int, float, str]')
        if rtn:
            return self.val

    def dtt_to_typ(self, str_typ='str', str_fmt='%Y-%m-%d', *, rtn=False):
        """
        alter datetime.datetime type to other type.
        fit for datetime.datetime, pd.Timestamp, tm.structtime, float, int and str.
        :param str_typ: target type
        :param str_fmt: if str_typ in ['str'],
        :param rtn: return the result or not, default False
        :return: if rtn is True, return self.val in type str_typ, format str_fmt
        """
        if self.typ not in [dt_datetime]:
            self.typ_to_dtt()
        if str_typ.lower() in ['datetime', 'dt_datetime', 'dt'] or self.val is None:
            pass
        elif str_typ.lower() in ['date', 'dt_date']:
            self.val = self.val.date()
        elif str_typ.lower() in ['time', 'dt_time']:
            self.val = self.val.time()
        elif str_typ.lower() in ['hour']:
            self.val = self.val.hour()
        elif str_typ.lower() in ['week']:
            self.val = self.val.isocalendar()[1]
        elif str_typ.lower() in ['weekDay', 'weekday']:
            self.val = self.val.isocalendar()[2]
        elif str_typ.lower() in ['day']:
            self.val = self.val.day
        elif str_typ.lower() in ['month', 'mnth']:
            self.val = self.val.month
        elif str_typ.lower() in ['year']:
            self.val = self.val.year
        elif str_typ.lower() in ['pd_structtime', 'pd_timestamp']:
            self.val = pd_to_datetime(dt_datetime.strftime(self.val, '%Y-%m-%d %H:%M:%S'), format='%Y-%m-%d %H:%M:%S')
        elif str_typ.lower() in ['timetuple', 'structtime', 'tm_structtime']:
            self.val = self.val.timetuple()
        elif str_typ.lower() in ['mday']:
            self.val = self.val.timetuple().tm_mday
        elif str_typ.lower() in ['yday']:
            self.val = self.val.timetuple().tm_yday
        elif str_typ.lower() in ['float', 'flt', 'int']:
            self.val = int(tm_mktime(self.val.timetuple()))
        elif str_typ.lower() in ['string', 'str', 'stz']:
            if str_fmt in ['%Y年%m月%d日']:
                self.val = dt_datetime.strftime(self.val, '%Y{y}%m{m}%d{d}').format(y='年', m='月', d='日')
            else:
                self.val = dt_datetime.strftime(self.val, str_fmt)
        else:
            raise KeyError("str_typ needs [None,NaN,'str','int','float','datetime','pd_timestamp','tm_structtime']")
        if rtn:
            return self.val

    def dtt_to_prd(self, str_kwd='w', rtn=False):
        """
        alter datetime.datetime type to period string in format ['%Yw%w','%Ym%m'].
        :param str_kwd: in ['W','w','M','m'] for ['%Yw%w','%yw%w','%Ym%m','%ym%m']
        :param rtn: return or not
        :return: result in format ['%Ym%m','%Yw%w']
        """
        if self.typ not in [dt_datetime]:
            self.typ_to_dtt()
        if self.val is not None:
            slf_dwk = self.val.isocalendar()                        # 得到tuple(year, week, weekday)
            slf_dyr = self.val.timetuple().tm_year                  # 得到year
            slf_dmh = self.val.timetuple().tm_mon                   # 得到month
            int_kyr = slf_dwk[0] if str_kwd.lower() == 'w' else slf_dyr     # 根据str_kwd判断标志年
            int_kwd = slf_dwk[1] if str_kwd.lower() == 'w' else slf_dmh     # 根据str_kwd判断标志字符
            str_kyr = str(int_kyr)[2:] if str_kwd.islower else str(int_kyr)
            self.val = "%s%s%s" % (str_kyr, str_kwd.lower(), str(int_kwd).zfill(2))
        else:
            print('info: None cannot convert to "%y[wm]%d" format.')
        if rtn:
            return self.val

    def dwk_to_dtt(self, flt_dlt=1, rtn=False):
        """
        alter string in format '19w01' to a certain datetime in this period.
        :param flt_dlt: in range(1,7) for '%Yw%w', range(-7,-1) also equals to range(1,7)
        :param rtn: return the result or not
        :return: if rtn is True, return
        """
        if [True for i in self.fmt if i in ['%Yw%w', '%yw%w']]:
            int_dyr = int(self.val.rsplit('w')[0])
            if int_dyr < 50:
                int_dyr = int(int_dyr + 2000)
            elif int_dyr < 100:
                int_dyr = int(int_dyr + 1900)
            int_dwk = int(self.val.rsplit('w')[1])
            dtt_jan = typ_dt_date(int_dyr, 1, 4)
            dtt_dlt = typ_dt_timedelta(dtt_jan.isoweekday() - 1)
            bgn_dyr = dtt_jan - dtt_dlt
            flt_dlt = 8 + flt_dlt if flt_dlt < 0 else flt_dlt
            self.val = bgn_dyr + typ_dt_timedelta(days=flt_dlt-1, weeks=int_dwk-1)
        else:
            print('info: {}, {} not in [\'%Yw%w\',\'%yw%w\']'.format(self.val, self.fmt))
        if rtn:
            return self.val

    def dmh_to_dtt(self, flt_dlt=1, rtn=False):
        """
        alter string in format '19m01' to a certain datetime in this period.
        :param flt_dlt: in range(1,28/31) for '%Ym%m', range(-31/-28,-1) also equals to range(1,28/31)
        :param rtn: return the result or not
        :return: if rtn is True, return
        """
        if [True for i in self.fmt if i in ['%Ym%m', '%ym%m']]:
            int_dyr = int(self.val.rsplit('m')[0])
            if int_dyr < 50:
                int_dyr = int(int_dyr + 2000)
            elif int_dyr < 100:
                int_dyr = int(int_dyr + 1900)
            int_dmh = int(self.val.rsplit('m')[1])
            int_max = monthrange(int_dyr, int_dmh)[1]
            if int_max - flt_dlt < 0:
                raise KeyError('this month has only %s days' % str(int_max))
            else:
                int_day = flt_dlt if flt_dlt > 0 else int_max + 1 + flt_dlt
                self.val = dt_datetime(int_dyr, int_dmh, int_day)
        else:
            print('info: {}, {} not in [\'%Ym%m\',\'%ym%m\']'.format(self.val, self.fmt))
        if rtn:
            return self.val

    def prd_to_dtt(self, flt_dlt=1, rtn=False):
        """
        alter period string in format ['%[Yy]w%w','%[Yy]m%m'] to datetime.datetime type.
        :param flt_dlt: which day in the period to export, in range(1,7) for week and range(1,28-31) for month
        :param rtn: return the result or not
        :return: if rtn is True, return
        """
        if [True for i in self.fmt if i in ['%Yw%w', '%yw%w']]:
            self.dwk_to_dtt(flt_dlt)
        elif [True for i in self.fmt if i in ['%Ym%m', '%ym%m']]:
            self.dmh_to_dtt(flt_dlt)
        else:
            raise AttributeError('%s is not in format [\'%Yw%w\',\'%yw%w\',\'%Ym%m\',\'%ym%m\']')
        if rtn:
            return self.val

    def edg_of_prd(self, lst_flt=None, str_typ='str', str_fmt='%Y-%m-%d', *, rtn=False):
        """
        export the edg of period value in format '[0-9]{2,4}[wm][0-9]{2}'.
        >>> dtz('20w14').edg_of_prd([1, -1], rtn=True)
        ['2020-03-30', '2020-04-05']
        :param lst_flt: a list of float in self.val period
        :param str_typ: type of the result
        :param str_fmt: format of the result if type is str
        :param rtn: return the result if True, default False
        :return: return the result if rtn is True
        """
        lst_prd, prd_tmp, lst_flt = [], self.val, [1, -1] if lst_flt is None else lst_flt
        for i in lsz(lst_flt).typ_to_lst(rtn=True):
            self.val = prd_tmp
            self.prd_to_dtt(i)
            lst_prd.append(self.dtt_to_typ(str_typ, str_fmt, rtn=True))
        self.val = lst_prd
        if rtn:
            return self.val

    def shf(self, flt_dlt=0, rtn=False):
        """
        shift days from dtz.val, type of dtz.val will be datetime.datetime.
        >>> dtz('2020-01-01').shf(5, rtn=True)
        datetime.datetime(2020, 1, 6, 0, 0)
        >>> dtz('2020-01-01').shf(-5, rtn=True)
        datetime.datetime(2019, 12, 27, 0, 0)
        :param flt_dlt: how many days to shift, delta in type float
        :param rtn: return or not, default False
        :return: if rtn is True, return the result
        """
        if self.typ not in [dt_datetime]:
            self.typ_to_dtt()
        if self.val is not None:
            self.val += typ_dt_timedelta(days=flt_dlt)
        else:
            print('info: None cannot shift.')
        if rtn:
            return self.val

    def lst_of_day(self, lst_stn, rtn=False):
        """
        generate a list of day.
        >>> dtz().lst_of_day(['2019-01-01','2019-01-04'], rtn=True)
        [datetime.datetime(2019, 1, 1, 0, 0),
         datetime.datetime(2019, 1, 2, 0, 0),
         datetime.datetime(2019, 1, 3, 0, 0)]
        :param lst_stn: a list from start to the end, if lenthg is 1, dtz.val should be the start point
        :param rtn: return or not, default False
        """
        lst_stn = lsz(lst_stn).typ_to_lst(rtn=True)
        if len(lst_stn) == 1:
            lst_stn = [self.val, lst_stn[0]]
        lst_dtt = []
        for i in lst_stn:
            self.val = i
            lst_dtt.append(self.typ_to_dtt(rtn=True))
        self.val = [lst_dtt[0]+typ_dt_timedelta(days=i) for i in range((lst_dtt[1] - lst_dtt[0]).days + 1) if
                    (lst_dtt[0]+typ_dt_timedelta(days=i)).date() != lst_dtt[1].date()]
        if rtn:
            return self.val


# def fnc_lst_dtt_bth(lst_mpt, flt_len, lst_typ=None, lst_fmt=None, bln_day=True, str_typ='list', lst_dct=None):
#     """
#     function separating a list of datetime in batches
#     eg.param: (['2016-01-01','2018-01-08'], 500, 'str','%Y-%m-%d',True,'[dict]',['RegDate_from','RegDate_to'])
#     :param lst_mpt: a list of datetime in format [datetime_beginning, datetime_ending]
#     :param flt_len: the width of a batch
#     :param lst_typ: the type of list input and output, in format ['str','str']
#     :param lst_fmt: the format of list input and output if type is 'str', informat ['%Y-%m-%d','%Y-%m-%d']
#     :param bln_day: the unit of batches is day or short than a day, default True
#     :param str_typ: 'list' -> [[A,B],[C,D],..]; 'listT' -> [[A,C,...],[B,D,...]]; '[dict]' -> [{A,B},{C,D},...]
#     :param lst_dct: define columns' names if str_typ is '[dict]', default [{'beginning':A,'end':B},...]
#     :return: a list of datetime batches, default in format [[tms_011,tms_012],[tms_021,tms_022],...]
#     """
#     lst_typ = [None, 'str'] if lst_typ is None else fnc_lst_cpy_cll(lst_typ, 2)         # 未指定时默认输出str
#     if lst_fmt is None:                                                                 # 未指定时默认输出标准格式
#         lst_fmt = [None, '%Y-%m-%d'] if bln_day else [None, '%Y-%m-%d %H:%M:%S']
#     else:
#         lst_fmt = fnc_lst_cpy_cll(lst_fmt, 2)
#     lst_mpt[0], lst_mpt[1] = fnc_tms_frm_all(lst_mpt[0]), fnc_tms_frm_all(lst_mpt[1])   # 进入长度2的list，转化为tms
#     lst_xpt = []
#     dtm_bdt = lst_mpt[0]                                                                # 时间段初次赋值
#     dtm_edt = lst_mpt[0] + typ_dt_timedelta(days=(flt_len-(1 if bln_day is True else 0)))
#     while (lst_mpt[1] - dtm_bdt).days >= 0:
#         stm_bdt = fnc_dtt_frm_tdy(0, lst_typ[1], lst_fmt[1], dtm_bdt)                   # 时间段输出格式转换
#         stm_edt = fnc_dtt_frm_tdy(0, lst_typ[1], lst_fmt[1], dtm_edt if dtm_edt < lst_mpt[1] else lst_mpt[1])
#         lst_xpt.append([stm_bdt, stm_edt])                                              # 格式转换后时间段追加入输出列表
#         dtm_bdt = dtm_bdt + typ_dt_timedelta(days=flt_len) if bln_day else dtm_edt + typ_dt_timedelta(seconds=1)    # 时间段递增
#         dtm_edt += typ_dt_timedelta(days=flt_len)                                                               # 时间段递增
#     return fnc_lst_to_thr(lst_xpt, str_typ, lst_dct)
