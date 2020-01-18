#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 22 16:30:38 2019
input/output operation.
@author: zoharslong

@alters:
2020-01-16 zoharslong
"""
from abc import ABCMeta

from numpy import array as np_array, ndarray as typ_np_ndarray
from pandas.core.series import Series as typ_pd_Series                  # 定义series类型
from pandas.core.frame import DataFrame as typ_pd_DataFrame             # 定义dataframe类型
from pandas.core.indexes.base import Index as typ_pd_Index              # 定义dataframe.columns类型
from pandas.core.indexes.range import RangeIndex as typ_pd_RangeIndex   # 定义dataframe.index类型
from pandas import DataFrame as pd_DataFrame, read_csv
from os import path
from bsc import stz, lsz, dcz, dtz
from pandas import DataFrame as pd_DataFrame


class ioBsc(pd_DataFrame, metaclass=ABCMeta):
    """
    I/O basic
    """
    lst_typ_dts = [
        str,
        stz,
        list,
        lsz,
        dict,
        dcz,
        tuple,
        typ_np_ndarray,
        typ_pd_DataFrame,
        typ_pd_Series,
        typ_pd_Index,
        typ_pd_RangeIndex,
    ]   # data sets' type
    lst_typ_lcn = [list, lsz, dict, dcz]   # io methods' type

    def __init__(self, dts=None, lcn=None, *, spr=False):
        # all the i/o operations have the same attributes for locate target data: location and collection
        super().__init__()                  # 不将dts预传入DataFrame
        self.__dts, self.typ = None, None   # 接受数据
        self.__lcn, self.iot = None, None   # 连接信息
        self.__init_rst(dts, lcn, spr=spr)

    def __init_rst(self, dts=None, lcn=None, *, spr=False):
        """
        private reset initiation.
        :param dts: a data set to input or output
        :return: None
        """
        self.dts = dts if self.dts is None and dts is not None else []
        self.lcn = lcn if self.lcn is None and lcn is not None else {}
        if spr:
            self.spr_nit()

    def spr_nit(self, rtn=False):
        try:
            super(ioBsc, self).__init__(self.__dts)
        except ValueError:
            print('info: %s cannot convert to DataFrame.' % (str(self.__dts)[:8]+'..'))
        if rtn:
            return self

    def __str__(self):
        """
        print(io path).
        :return: None
        """
        return '<io: %s; ds: %s>' % (str(self.lcn)[1:-1], self.typ)
    __repr__ = __str__  # 调用类名的输出与print(className)相同

    @property
    def dts(self):
        """
        @property get & set lsz.seq.
        :return: lsz.seq
        """
        return self.__dts

    @dts.setter
    def dts(self, dts):
        """
        lsz.seq = seq.
        :param seq: a sequence to import.
        :return: None
        """
        if dts is None or type(dts) in self.lst_typ_dts:
            self.__dts = dts
            self.__attr_rst('dts')
        else:
            raise TypeError('info: dts\'s type %s is not available.' % type(dts))

    @property
    def lcn(self):
        return self.__lcn

    @lcn.setter
    def lcn(self, lcn):
        if lcn is None or type(lcn) in self.lst_typ_lcn:
            self.__lcn = lcn
            self.__attr_rst('lcn')
        else:
            raise TypeError('info: lcn\'s type %s is not available.' % type(lcn))

    def __attr_rst(self, str_typ=None):
        """
        reset attributes lsz.typ.
        :param str_typ: type of attributes resets, in ['dts','lcn'] for data set reset and location reset
        :return: None
        """
        if str_typ in ['dts', None]:
            try:
                self.typ = type(self.__dts)
            except TypeError:
                raise TypeError('info: %s is not available for type().' % (str(self.__dts)[:8]+'..'))
        if str_typ in ['lcn', None]:    # in ['lcz','mnz','sqz','apz'] for [local, mongodb, sql, api]
            if [True for i in self.lcn.keys() if i in ['fld', 'fls']]:
                self.iot = 'lcz'
            elif [True for i in self.lcn.keys() if i in ['sql', 'tbl']]:
                self.iot = 'sqz'
            elif [True for i in self.lcn.keys() if i in ['mng', 'cln']]:
                self.iot = 'mnz'
            elif [True for i in self.lcn.keys() if i in ['url', 'dts']]:
                self.iot = 'apz'
            else:
                raise KeyError('stop: %s is not available.' % self.lcn)


class lczMixin(ioBsc, metaclass=ABCMeta):
    """
    local files input and output operations.
    """
    def __init__(self, dts=None, lcn=None, *, spr=False):
        super(lczMixin, self).__init__(dts, lcn, spr=spr)

    def mpt_csv(self, str_sep=None, *, rtn=False):
        if str_sep is None:
            self.dts = read_csv(path.join(*self.lcn.values()))
        else:
            self.dts = read_csv(path.join(*self.lcn.values()), sep=str_sep)
        if rtn:
            return self.dts

    def mpt(self, rtn=False):
        pass

    def xpt(self, rtn=False):
        pass


class mngMixin(ioBsc, metaclass=ABCMeta):
    """
    local files input and output operations.
    """
    def __init__(self, dts=None, lcn=None, *, spr=False):
        super(mngMixin, self).__init__(dts, lcn, spr=spr)


class ioz(mngMixin, lczMixin, metaclass=ABCMeta):
    def __init__(self, dts=None, lcn=None, *, spr=False):
        super(ioz, self).__init__(dts, lcn, spr=spr)


print('io\'s alteration ready.')
