#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 22 16:30:38 2019
@author: zoharslong

@alters:
2020-01-14 zoharslong
"""
from datetime import datetime as dt_datetime
from pandas import to_datetime as pd_to_datetime
from pandas._libs.tslib import Timestamp as typ_pd_Timestamp
from datetime import date as typ_dt_date, time as typ_dt_time, timedelta as typ_dt_timedelta
from time import struct_time as typ_tm_structtime, strftime as tm_strftime, mktime as tm_mktime
from re import search as re_search
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
    """
    def __init__(self, val=''):
        """
        create a new string object from the given object.
        :param val: 指定的值
        """
        super().__init__()
        self.__val = val, self.rgx, self.fmt = val, [], []

    def add_rgx(self, lst_rgx=None, rtn=False):
        """
        match regex
        从传入的正则列表中筛选出符合类的正则表达式组成列表
        :param lst_rgx: 参与匹配的正则表达式列表
        :param rtn: return or not, default False
        :return: None
        """
        lst_rgx = list(dct_tms_fTr.values()) if lst_rgx is None else lst_rgx
        rgx_xpt = [i_rgx for i_rgx in lst_rgx if re_search(i_rgx, self)]
        self.rgx = rgx_xpt
        if rtn:
            return self.rgx

    def add_fmt(self, dct_rgx=None, rtn=False):
        """
        match format
        从传入的格式-正则字典中筛选出符合类的格式组成列表
        :param dct_rgx: 参与匹配的格式-正则表达式字典
        :param rtn: return or not, default False
        :return: None
        """
        if len(self.rgx) <= 0:
            self.add_rgx()
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
    >>> dtz('2019m03').prd_to_dtt(-1, rtn=True)
    datetime.datetime(2019, 3, 31, 0, 0)
    """
    __slots__ = ('__val', 'typ', 'fmt',)
    lst_typ_dtz = [
        str,
        stz,
        int,
        float,
        dt_datetime,
        typ_dt_date,
        typ_dt_time,
        typ_pd_Timestamp,
        typ_tm_structtime,
    ]   # dtz.val's type

    def __init__(self, val=None):
        """"""
        self.__val, self.typ = None, None
        self.__init_rst(val)

    def __init_rst(self, val=None):
        """private reset initiation"""
        if self.val is None:
            self.val = val if val is not None else dt_datetime.now()
        self.typ = type(self.val)

    @property
    def val(self):
        """
        @property get & set dtz.val
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
        if val is None or type(val) in self.lst_typ_dtz:
            self.__val = val
        else:
            raise TypeError('val\'s type %s is not available.' % type(val))

    def __str__(self):
        """
        print(className)
        :return: None
        """
        if type(self.__val) == typ_tm_structtime:
            str_xpt = 'time.struct_time'+str(tuple(self.__val))
        else:
            str_xpt = self.__val
        return '%s' % str_xpt
    __repr__ = __str__  # 调用类名的输出与print(className)相同

    def typ_to_dtt(self, rtn=False, *args):
        """
        alter other type to datetime.datetime type
        fit for datetime.datetime, pd.Timestamp, tm.structtime, float, int and str
        :param rtn: return the result or not, default False
        :return: if rtn is True, return dtz.val
        """
        if self.typ in [dt_datetime]:
            pass
        elif self.typ in [typ_dt_date]:
            self.val = dt_datetime.combine(self.val, typ_dt_time())
        elif self.typ in [typ_pd_Timestamp]:
            self.val = dt_datetime.combine(self.val.date(), self.val.time())
        elif self.typ in [typ_tm_structtime]:
            self.val = dt_datetime.strptime(tm_strftime('%Y-%m-%d %H:%M:%S', self.val), '%Y-%m-%d %H:%M:%S')
        elif self.typ in [int, float]:
            self.val = dt_datetime.fromtimestamp(int(str(self.val).rsplit('.')[0]))
        elif self.typ in [str, stz]:
            slf_fmt = stz(self.val).add_fmt(rtn=True)   # self.format
            if [True for i in slf_fmt if i in ['float', 'int']]:
                self.val = dt_datetime.fromtimestamp(int(self.val.rsplit('.')[0]))
            else:
                self.val = dt_datetime.strptime(self.val, slf_fmt[0])
        else:
            raise TypeError('type of value not in [dt_datetime, dt_date, tm_structtime, pd_Timestamp, int, float, str]')
        self.__init_rst()
        if rtn:
            return self.val

    def dtt_to_typ(self, str_typ='str', str_fmt='%Y-%m-%d', rtn=False):
        """
        alter datetime.datetime type to other type
        fit for datetime.datetime, pd.Timestamp, tm.structtime, float, int and str
        :param str_typ: target type
        :param str_fmt: if str_typ in ['str'],
        :param rtn: return the result or not, default False
        :return: if rtn is True, return self.val in type str_typ, format str_fmt
        """
        if self.typ not in [dt_datetime]:
            self.typ_to_dtt()
        if str_typ.lower() in ['datetime', 'dt_datetime', 'dt']:
            pass
        if str_typ.lower() in ['date', 'dt_date']:
            self.val = self.val.date()
        elif str_typ.lower() in ['pd_structtime', 'pd_timestamp']:
            self.val = pd_to_datetime(dt_datetime.strftime(self.val, '%Y-%m-%d %H:%M:%S'), format='%Y-%m-%d %H:%M:%S')
        elif str_typ.lower() in ['timetuple', 'structtime', 'tm_structtime']:
            self.val = self.val.timetuple()
        elif str_typ.lower() in ['float', 'flt', 'int']:
            self.val = int(tm_mktime(self.val.timetuple()))
        elif str_typ.lower() in ['string', 'str', 'stz']:
            if str_fmt in ['%Y年%m月%d日']:
                self.val = dt_datetime.strftime(self.val, '%Y{y}%m{m}%d{d}').format(y='年', m='月', d='日')
            else:
                self.val = dt_datetime.strftime(self.val, str_fmt)
        else:
            raise KeyError("str_typ needs ['str','int','float','datetime','pd_timestamp','tm_structtime']")
        self.__init_rst()
        if rtn:
            return self.val

    def shf(self, flt_dlt=0, rtn=False):
        """
        shift days from dtz.val, type of dtz.val will be datetime.datetime
        :param flt_dlt: how many days to shift, delta in type float
        :param rtn: return or not, default False
        :return: if rtn is True, return the result
        """
        if self.typ not in [dt_datetime]:
            self.typ_to_dtt()
        self.val += typ_dt_timedelta(days=flt_dlt)
        if rtn:
            return self.val

    def dtt_to_prd(self, str_kwd='w', rtn=False):
        """
        alter datetime.datetime type to period string in format ['%Yw%w','%Ym%m']
        :param str_kwd: in ['w','m']
        :param rtn: return or not
        :return: result in format ['%Ym%m','%Yw%w']
        """
        if self.typ not in [dt_datetime]:
            self.typ_to_dtt()
        slf_dwk = self.val.isocalendar()                        # 得到tuple(year, week, weekday)
        slf_dmh = self.val.timetuple().tm_mon                   # 得到month
        int_kwd = slf_dwk[1] if str_kwd == 'w' else slf_dmh     # 根据str_kwd判断标志字符
        self.val = "%s%s%s" % (str(slf_dwk[0]), str_kwd, str(int_kwd).zfill(2))
        self.__init_rst()
        if rtn:
            return self.val

    def dwk_to_dtt(self, flt_dlt=1, rtn=False):
        """
        :param flt_dlt: in range(1,7) for '%yw%w', range(-7,-1) also equals to range(1,7)
        :param rtn:
        :return:
        """
        slf_fmt = stz(self.val).add_fmt(rtn=True)
        if '%Yw%w' not in slf_fmt:
            raise AttributeError('%s not in [\'%Yw%w\']' % slf_fmt)
        else:
            int_dyr = int(self.val.rsplit('w')[0])
            int_dwk = int(self.val.rsplit('w')[1])
            dtt_jan = typ_dt_date(int_dyr, 1, 4)
            dtt_dlt = typ_dt_timedelta(dtt_jan.isoweekday() - 1)
            bgn_dyr = dtt_jan - dtt_dlt
            flt_dlt = 8 + flt_dlt if flt_dlt < 0 else flt_dlt
            self.val = bgn_dyr + typ_dt_timedelta(days=flt_dlt-1, weeks=int_dwk-1)
            self.__init_rst()
        if rtn:
            return self.val

    def dmh_to_dtt(self, flt_dlt=1, rtn=False):
        slf_fmt = stz(self.val).add_fmt(rtn=True)
        if '%Ym%m' not in slf_fmt:
            raise AttributeError('%s not in [\'%Ym%m\']' % slf_fmt)
        else:
            from calendar import monthrange
            int_dyr = int(self.val.rsplit('m')[0])
            int_dmh = int(self.val.rsplit('m')[1])
            int_max = monthrange(int_dyr, int_dmh)[1]
            if int_max - flt_dlt < 0:
                raise KeyError('this month has only %s days' % str(int_max))
            else:
                int_day = flt_dlt if flt_dlt > 0 else int_max + 1 + flt_dlt
                self.val = dt_datetime(int_dyr, int_dmh, int_day)
                self.__init_rst()
                if rtn:
                    return self.val

    def prd_to_dtt(self, flt_dlt=1, rtn=False):
        """
        alter period string in format ['%Yw%w','%Ym%m'] to datetime.datetime type
        :param flt_dlt: which day in the period to export, in range(1,7) for week and range(1,28-31) for month
        :param rtn: return the result or not
        :return: if rtn is True, return
        """
        if re_search('w', self.val):
            self.dwk_to_dtt(flt_dlt, rtn)
        elif re_search('m', self.val):
            self.dmh_to_dtt(flt_dlt, rtn)
        else:
            raise AttributeError('%s is not in format [\'%Yw%w\',\'%Ym%m\']')


print('ready.')
