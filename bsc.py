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
from calendar import monthrange     # how many days in any month
from numpy import array as np_array, ndarray as typ_np_ndarray
from pandas.core.series import Series as typ_pd_Series                  # 定义series类型
from pandas.core.frame import DataFrame as typ_pd_DataFrame             # 定义dataframe类型
from pandas.core.indexes.base import Index as typ_pd_Index              # 定义dataframe.columns类型
from pandas.core.indexes.range import RangeIndex as typ_pd_RangeIndex   # 定义dataframe.index类型
from pandas import DataFrame as pd_DataFrame
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

    def add_fmt(self, dct_rgx=None, rtn=False):
        """
        match format.
        从传入的格式-正则字典中筛选出符合类的格式组成列表.
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
    >>> dtz(dtz(15212421.1).typ_to_dtt(rtn=True)).dtt_to_typ('str',rtn=True)    # from datetime to any type
    '1970-06-26'
    >>> dtz(dtz(15212421.1).typ_to_dtt(rtn=True)).shf(5, rtn=True)    # 5 days after 1970-06-26 in type datetime
    datetime.datetime(1970, 7, 1, 9, 40, 21)
    >>> dtz('2019m03').prd_to_dtt(-1, rtn=True) # get the last day in 2019 March
    datetime.datetime(2019, 3, 31, 0, 0)
    >>> dtz('2019w03').prd_to_dtt(-1, rtn=True) # get the last day in 2019 3th week
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
        """
        initiating dtz.val, dtz.typ.
        :param val: a datetime content in any type, None for datetime.datetime.now()
        """
        self.__val, self.typ, self.fmt = None, None, None
        self.__init_rst(val)

    def __init_rst(self, val=None):
        """
        private reset initiation.
        :param val: a datetime content in any type, None for datetime.datetime.now()
        :return: None
        """
        if self.val is None:
            self.val = val if val is not None else dt_datetime.now()
        self.__attr_rst()

    def __attr_rst(self):
        """
        reset attributes dtz.typ, dtz.fmt.
        :return: None
        """
        self.typ = type(self.val)
        self.fmt = stz(self.val).add_fmt(rtn=True) if self.typ == str else self.fmt

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
            raise TypeError('val\'s type %s is not available.' % type(val))

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
        elif self.typ in [typ_dt_date]:
            self.val = dt_datetime.combine(self.val, typ_dt_time())
        elif self.typ in [typ_pd_Timestamp]:
            self.val = dt_datetime.combine(self.val.date(), self.val.time())
        elif self.typ in [typ_tm_structtime]:
            self.val = dt_datetime.strptime(tm_strftime('%Y-%m-%d %H:%M:%S', self.val), '%Y-%m-%d %H:%M:%S')
        elif self.typ in [int, float]:
            self.val = dt_datetime.fromtimestamp(int(str(self.val).rsplit('.')[0]))
        elif self.typ in [str, stz]:
            if [True for i in self.fmt if i in ['float', 'int']]:
                self.val = dt_datetime.fromtimestamp(int(self.val.rsplit('.')[0]))
            else:
                self.val = dt_datetime.strptime(self.val, self.fmt[0])
        else:
            raise TypeError('type of value not in [dt_datetime, dt_date, tm_structtime, pd_Timestamp, int, float, str]')
        self.__init_rst()
        if rtn:
            return self.val

    def dtt_to_typ(self, str_typ='str', str_fmt='%Y-%m-%d', rtn=False):
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
        shift days from dtz.val, type of dtz.val will be datetime.datetime.
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
        alter datetime.datetime type to period string in format ['%Yw%w','%Ym%m'].
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
            raise AttributeError('%s not in [\'%Yw%w\',\'%yw%w\']' % self.fmt)
        self.__init_rst()
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
            raise AttributeError('%s not in [\'%Ym%m\',\'%ym%m\']' % self.fmt)
        self.__init_rst()
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


class lsz(list):
    """
    type list altered by zoharslong.
    >>> lsz({'A':1,'B':2,'C':3}).typ_to_lst(rtn=True)
    [('A', 1), ('B', 2), ('C', 3)]
    >>> lsz([1,2,3]).lst_to_typ('dict', rtn=True, prm=['A','B','C'])
    [{'A': 1, 'B': 2, 'C': 3}]
    >>> lsz([1,2,3]).cpy_to_tal(5,rtn=True)
    [1, 2, 3, 3, 3]
    >>> lsz([1,2,3]).mrg_to_cll(['_A','_B','_C'],rtn=True)
    ['1_A', '2_B', '3_C']
    """
    __slots__ = ('__seq', 'typ', 'len', 'len_min', 'len_max')
    lst_typ_lsz = [
        str,
        stz,
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
        :param seq: first, save target into lsz.seq
        :param spr: let lsz = lsz.seq or not, default False
        """
        super().__init__()
        self.__seq, self.typ, self.len, self.len_max, self.len_min = None, None, None, None, None
        self.__init_rst(seq, spr)

    def __init_rst(self, seq=None, spr=False):
        """
        private reset initiation.
        :param seq: a list content in any type, None for []
        :param spr: let lsz = lsz.seq or not, default False
        :return: None
        """
        if self.seq is None:
            self.seq = seq if seq is not None else []
        self.__attr_rst()
        if spr:
            self.spr_nit()

    def __attr_rst(self):
        """
        reset attributes lsz.typ.
        :return: None
        """
        self.typ = type(self.__seq)
        self.len = len(self.__seq)
        self.edg_of_len()

    def spr_nit(self, rtn=False):
        """
        super initiation. let lsz = lsz.seq.
        :param rtn: return lsz or not, default False
        :return: if rtn is True, return lsz
        """
        super(lsz, self).__init__(self.__seq)
        if rtn:
            return self

    def edg_of_len(self, rtn=False):
        """
        get the max and min length of cells in lsz.seq.
        :param rtn: return the result or not, default False
        :return: if rtn is True, return the result in format [min, max]
        """
        lst_len = [len(i) for i in self.seq if type(i) in [self.lst_typ_lsz]]
        self.len_max = max(lst_len) if lst_len != [] else None
        self.len_min = min(lst_len) if lst_len != [] else None
        if rtn:
            return [self.len_min, self.len_max]

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

    def typ_to_lst(self, spr=False, rtn=False, prm='record'):
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

    def lst_to_typ(self, str_typ='list', spr=False, rtn=False, prm=None):
        """"""
        self.typ_to_lst()
        if str_typ.lower() in ['list', 'lst']:
            pass
        elif str_typ.lower() in ['listt', 'lstt', 't']:
            self.seq = np_array(self.seq).T.tolist()
        elif str_typ.lower() in ['dict', 'dct', '[dict]', '[dct]', 'listdict', 'lstdct']:  # [a,b]+[1,1]->[{a:1},{b:1}]
            self.seq = pd_DataFrame([self.seq], columns=lsz(prm).typ_to_lst(rtn=True)).to_dict(orient='record')
        elif str_typ.lower() in ['listtuple', 'lsttpl', 'list_tuple', 'lst_tpl']:   # [a,b,..]+[1,1,..] ->[(a,1),..]
            lst_prm = lsz(prm).cpy_to_tal(self.len, rtn=True)
            self.seq = [(i, lst_prm[self.seq.index(i)]) for i in self.seq]
        elif str_typ.lower() in ['str']:  # 用于将list转化为不带引号的字符串:[1,'a',...] -> "1,a,..."
            self.seq = str(self.seq).replace("'", '')[1:-1]
        if spr:
            self.spr_nit()
        if rtn:
            return self.seq

    def cpy_to_tal(self, flt_len, spr=False, rtn=False):
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

    def mrg_to_cll(self, lst_mrg, spr=False, rtn=False):
        """
        in lsz.seq, cell to cell merging.
        structuring: ['a','b',...] -> ['ax','bx',...].
        :param lst_mrg: a list to merge in method cell by cell
        :param spr: let lsz = lsz.seq or not, default False
        :param rtn: return the result or not, default False
        :return: if rtn is True, return the final result
        """
        self.typ_to_lst()
        lst_mrg = lsz(lst_mrg).cpy_to_tal(self.len, rtn=True)
        self.seq = [str(self.seq[i]) + str(lst_mrg[i]) for i in range(self.len)]
        if spr:
            self.spr_nit()
        if rtn:
            return self.seq


print('ready.')
