#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 22 16:30:38 2019
@author: zoharslong

@alters:
2020-01-14 zoharslong
"""
from datetime import datetime as dt_datetime
from pandas._libs.tslib import Timestamp as typ_pd_Timestamp
from datetime import date as typ_dt_date, time as typ_dt_time
from time import struct_time as typ_tm_structtime, strftime as tm_strftime
dct_tms_fTr = {
    '%Y-%m-%d %H:%M:%S ': '^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2} [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2} $',
    '%Y-%m-%d %H:%M:%S': '^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2} [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}$',
    '%Y-%m-%d': '^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}$',
    '%Y/%m/%d %H:%M:%S': '^[0-9]{4}/[0-9]{1,2}/[0-9]{1,2} [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}$',
    '%Y/%m/%d': '^[0-9]{4}/[0-9]{1,2}/[0-9]{1,2}$',
    '%Y.%m.%d %H:%M:%S': '^[0-9]{4}[.][0-9]{1,2}[.][0-9]{1,2} [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}$',
    '%Y.%m.%d': '^[0-9]{4}[.][0-9]{1,2}[.][0-9]{1,2}$',
    '%Y年%m月%d日 %H:%M:%S': '^[0-9]{2,4}年[0-9]{1,2}月[0-9]{1,2}日 [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}$',
    '%Y年%m月%d日': '^[0-9]{2,4}年[0-9]{1,2}月[0-9]{1,2}日$',
    'int': '^[0-9]+[.]{0}[0-9]*$',
    'float': '^[0-9]+[.]{1}[0-9]*$',
}


class stz(str):
    """
    type str altered by zoharslong.
    >>> stz('1989-02-14').mch_rgx(rtn=True)     # generate regex of the string content
    ['^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}$']
    >>> stz('1989-02-14').mch_fmt(rtn=True)     # generate format of the string content
    ['%Y-%m-%d']
    """
    def __init__(self, val=''):
        """
        create a new string object from the given object.
        :param val: 指定的值
        """
        super().__init__()
        self.__val = val, self.rgx, self.fmt = val, [], []

    def mch_rgx(self, lst_rgx=None, rtn=False):
        """
        match regex
        从传入的正则列表中筛选出符合类的正则表达式组成列表
        :param lst_rgx: 参与匹配的正则表达式列表
        :param rtn: return or not, default False
        :return: None
        """
        from re import search as re_search
        lst_rgx = list(dct_tms_fTr.values()) if lst_rgx is None else lst_rgx
        rgx_xpt = [i_rgx for i_rgx in lst_rgx if re_search(i_rgx, self)]
        self.rgx = rgx_xpt
        if rtn:
            return self.rgx

    def mch_fmt(self, dct_rgx=None, rtn=False):
        """
        match format
        从传入的格式-正则字典中筛选出符合类的格式组成列表
        :param dct_rgx: 参与匹配的格式-正则表达式字典
        :param rtn: return or not, default False
        :return: None
        """
        if len(self.rgx) <= 0:
            self.mch_rgx()
        dct_rgx = dct_tms_fTr if dct_rgx is None else dct_rgx
        for i in range(len(self.rgx)):
            self.fmt.append(list(dct_rgx.keys())[list(dct_rgx.values()).index(self.rgx[i])])
        if rtn:
            return self.fmt


class dtz(object):
    """
    type datetime altered by zoharslong.
    >>> dtz(15212421.1).typ_to_dtt(rtn=True)    # from any type to datetime
    datetime.datetime(1970, 6, 26, 9, 40, 21)
    """
    __slots__ = ('__val', 'typ', 'fmt',)
    lst_typ_dtz = [
        str,
        int,
        float,
        dt_datetime,
        typ_dt_date,
        typ_pd_Timestamp,
        typ_tm_structtime,
    ]   # dtz.val's type

    def __init__(self, val=None):
        """"""
        self.__val = None
        self.__init_rst(val)

    def __init_rst(self, val=None):
        """private reset initiation"""
        if self.val is None:
            self.val = val
        self.typ = type(self.val)

    @property
    def val(self):
        """
        :return: dtz.val
        """
        return self.__val

    @val.setter
    def val(self, val=None):
        """
        dtz.val = val
        :param val: a value to import.
        :return: None
        """
        if type(val) not in self.lst_typ_dtz:
            raise TypeError('type of val is not available')
        else:
            self.__val = val

    def __str__(self):
        """
        print(className)
        :return: None
        """
        return '%s' % self.__val
    __repr__ = __str__  # 调用类名的输出与print(className)相同

    def typ_to_dtt(self, rtn=False):
        """
        alter type to datetime.datetime
        tmz(15212421.1).typ_to_dtt(rtn=True)
        """
        if self.typ in [dt_datetime]:
            pass
        elif self.typ in [typ_pd_Timestamp]:
            self.val = dt_datetime.combine(self.val.date(), self.val.time())
        elif self.typ in [typ_dt_date]:
            self.val = dt_datetime.combine(self.val, typ_dt_time())
        elif self.typ in [typ_tm_structtime]:
            self.val = dt_datetime.strptime(tm_strftime('%Y-%m-%d %H:%M:%S', self.val), '%Y-%m-%d %H:%M:%S')
        elif self.typ in [int, float]:
            self.val = dt_datetime.fromtimestamp(int(str(self.val).rsplit('.')[0]))
        elif self.typ in [str]:
            slf_fmt = stz(self.val).mch_fmt(rtn=True)
            if [True for i in slf_fmt if i in ['float', 'int']]:
                self.val = dt_datetime.fromtimestamp(int(self.val.rsplit('.')[0]))
            else:
                self.val = dt_datetime.strptime(self.val, slf_fmt[0])
        else:
            raise TypeError('type of value not in [dt_datetime, dt_date, tm_structtime, int, float, str]')
        self.__init_rst()
        if rtn:
            return self.val

    def dtt_to_typ(self):
        pass


print('ready.')
