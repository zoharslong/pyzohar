#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 22 16:30:38 2019
input/output operation.
@author: zoharslong

@alters:
2020-01-16 zoharslong
"""
from numpy import array as np_array, ndarray as typ_np_ndarray
from pandas.core.series import Series as typ_pd_Series                  # 定义series类型
from pandas.core.frame import DataFrame as typ_pd_DataFrame             # 定义dataframe类型
from pandas.core.indexes.base import Index as typ_pd_Index              # 定义dataframe.columns类型
from pandas.core.indexes.range import RangeIndex as typ_pd_RangeIndex   # 定义dataframe.index类型
from pandas import DataFrame as pd_DataFrame
from bsc import stz, lsz, dcz
from pandas import DataFrame as pd_DataFrame


class ioBsc(object):
    """
    I/O basic
    """
    lst_typ_iob = [
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
    ]   # lsz.seq's type

    def __init__(self, dts=None, lcn=None, cln=None):
        # all the i/o operations have the same attributes for locate target data: location and collection
        self.__dts, self.lcn, self.cln = None, None, None
        self.typ_dts, self.len = None, None
        self.__init_rst(dts, lcn, cln)

    def __init_rst(self, dts=None, lcn=None, cln=None):
        """
        private reset initiation.
        :param dts: a data set to input or output
        :return: None
        """
        self.dts = dts if dts is not None and self.dts is None else None
        self.lcn = lcn if lcn is not None and self.lcn is None else None
        self.cln = cln if cln is not None and self.cln is None else None
        self.__attr_rst()

    def __attr_rst(self):
        """
        reset attributes lsz.typ.
        :return: None
        """
        self.typ = type(self.__dts)
        try:
            self.len = len(self.__dts)
        except TypeError:
            raise TypeError('info: type %s is not available.' % self.typ)


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
        if dts is None or type(dts) in self.lst_typ_iob:
            self.__dts = dts
            self.__attr_rst()
        else:
            raise TypeError('info: dts\'s type %s is not available.' % type(dts))


class lczMixIn(ioBsc):
    """
    local files input and output operations.
    """
    def __init__(self):
        super()

    def mpt_csv(self):
        pass

    def mpt(self, rtn=False):
        pass

    def xpt(self, rtn=False):
        pass


print('basic types\' alteration ready.')
