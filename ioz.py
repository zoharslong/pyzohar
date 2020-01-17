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
from bsc import stz, lsz, dcz, dtz
from pandas import DataFrame as pd_DataFrame


class ioBsc(object):
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
    lst_typ_lcn = [list, lsz]   # io methods' type

    def __init__(self, dts=None, lcn=None):
        # all the i/o operations have the same attributes for locate target data: location and collection
        self.__dts, self.typ, self.len = None, None, None   # 接受数据
        self.__iot, self.__lcn = None, None   # 连接信息
        self.__init_rst(dts, lcn)

    def __init_rst(self, dts=None, lcn=None):
        """
        private reset initiation.
        :param dts: a data set to input or output
        :return: None
        """
        self.dts = dts if dts is not None else []
        self.lcn = lcn if lcn is not None else []

    def __attr_rst(self, str_typ=None):
        """
        reset attributes lsz.typ.
        :return: None
        """
        if str_typ in ['dts', None]:
            self.typ = type(self.__dts)
            try:
                self.len = len(self.__dts)
            except TypeError:
                raise TypeError('info: type %s is not available.' % self.typ)
        if str_typ in ['lcn', None]:
            pass

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
            raise TypeError('info: dts\'s type %s is not available.' % type(lcn))


class lczMixIn(ioBsc):
    """
    local files input and output operations.
    """
    def __init__(self, dts=None, lcn=None):
        super(lczMixIn, self).__init__(dts, lcn)

    def mpt_csv(self):
        pass

    def mpt(self, rtn=False):
        pass

    def xpt(self, rtn=False):
        pass


class mngMixIn(ioBsc):
    """
    local files input and output operations.
    """
    def __init__(self, dts=None, lcn=None):
        super(mngMixIn, self).__init__(dts, lcn)

    def mpt_csv(self):
        pass

    def mpt(self, rtn=False):
        pass

    def xpt(self, rtn=False):
        pass


class ioz(mngMixIn, lczMixIn):
    def __init__(self, dts=None, lcn=None):
        super(ioz, self).__init__(dts, lcn)


print('io\'s alteration ready.')
