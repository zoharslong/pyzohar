#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 22 16:30:38 2019
dataframe operation.
@author: zoharslong

@alters:
2020-02-16 zoharslong
"""
from numpy import nan as np_nan
from pandas import merge, concat
from re import search as re_search, findall as re_findall, sub as re_sub, match as re_match
from pandas.core.indexes.base import Index as typ_pd_Index              # 定义dataframe.columns类型
from math import isnan as math_isnan
from .bsc import stz, lsz, dtz
from .ioz import ioz


class dfBsc(ioz):

    def __init__(self, dts=None, lcn=None, *, spr=False):
        super(dfBsc, self).__init__(dts, lcn, spr=spr)


class clmMixin(dfBsc):

    def __init__(self, dts=None, lcn=None, *, spr=False):
        super(clmMixin, self).__init__(dts, lcn, spr=spr)

    def srt_clm(self, lst_clm, *, spr=False, rtn=False, drp=True):
        """
        sort columns.
        >>> from pandas import DataFrame
        >>> tst = clmMixin(DataFrame([{'A':1,'B':1}]),spr=True)
        >>> tst.srt_clm('A', rtn=True)
           A
        0  1
        :param lst_clm: columns to be sorted
        :param spr: let self = self.dts
        :param rtn: default False, return None
        :param drp: drop columns which not mentioned in lst_clm, default True
        :return: if rtn is Ture, return self.dts
        """
        lst_fnl = []
        lst_clm = lsz(lst_clm).typ_to_lst(rtn=True)
        for i_cll in lst_clm:  # 对self.dts中存在的columns, 按照lst_clm_srt排序
            if list({i_cll} & set(self.clm)):
                lst_fnl.append(i_cll)
        if drp:  # bln_drp为True，则舍弃self.clm中，未出现再lst_clm_srt中的列
            pass
        else:  # bln_drp为False，则将self.clm中有而lst_clm_srt未提及的列附加在后面
            x = lsz(lst_fnl).mrg('differ', self.clm, rtn=True)
            lst_fnl.extend(x)
        self.dts = self.dts[lst_fnl]
        if spr:
            self.spr_nit()
        if rtn:
            return self.dts

    def drp_clm(self, lst_clm, *, spr=False, rtn=False):
        """
        drop columns.
        >>> from pandas import DataFrame
        >>> tst = clmMixin(DataFrame([{'A':1,'B':1}]),spr=True)
        >>> tst.drp_clm('A', rtn=True)
           B
        0  1
        :param lst_clm: a list of columns to drop
        :param spr: let self = self.dts
        :param rtn: default False, return None
        :return: if rtn is True, return self.dts
        """
        lst_clm = lsz(lst_clm).typ_to_lst(rtn=True)
        self.dts = self.dts.drop(lst_clm, axis=1)
        if spr:
            self.spr_nit()
        if rtn:
            return self.dts

    def rnm_clm(self, *args, spr=False, rtn=False):
        """
        rename columns.
        >>> from pandas import DataFrame
        >>> tst = clmMixin(DataFrame([{'A':1,'B':1}]),spr=True)
        >>> tst.rnm_clm({'A':'a'}, rtn=True) # tst.rnm_clm('A','a',rtn=True)
           a  B
        0  1  1
        :param args: old columns' name and the new. 填入两个列表或一个字典，表达重命名对应关系
        :param spr: let self = self.dts
        :param rtn: default False, return None
        :return: if rtn is True, return self.dts, default False
        """
        if len(args) == 1 and type(args[0]) in [dict]:
            self.dts = self.dts.rename(columns=args[0])
        else:
            self.dts = self.dts.rename(columns=lsz(args[1]).lst_to_typ('dct', args[0], rtn=True)[0])
        if spr:
            self.spr_nit()
        if rtn:
            return self.dts

    def fll_clm_na(self, *args, spr=False, rtn=False, prm=None):
        """
        fill columns' na cells.
        >>> from pandas import DataFrame
        >>> from numpy import nan as np_nan
        >>> tst = clmMixin(DataFrame([{'A':np_nan,'B':1}]),spr=True)
        >>> tst.fll_clm_na({'A':1}, rtn=True)
             A  B
        0  1.0  1
        :param args: columns and values to be filled. 填入两个列表或一个字典，表达对列中空缺值的填充关系
        :param spr: let self = self.dts
        :param rtn: default False, return None
        :param prm: method of fill na, in ['backfill', 'bfill', 'pad', 'ffill', None]
        :return: if rtn is True, return self.dts
        """
        lst_clm = lsz(args[0])
        if len(args) > 1 and type(args[0]) in [list, str]:
            lst_clm.typ_to_lst(spr=True)
            lst_clm_fll = lsz(args[1])
            lst_clm_fll.typ_to_lst(spr=True)
            flt_len = lsz([lst_clm, lst_clm_fll]).edg_of_len(rtn=True)[1]
            lst_clm.cpy_tal(flt_len, spr=True)
            lst_clm_fll.cpy_tal(flt_len, spr=True)
            lst_clm.lst_to_typ('dct', lst_clm_fll)
        self.dts = self.dts.fillna(value=lst_clm.seq, method=prm)
        if spr:
            self.spr_nit()
        if rtn:
            return self.dts

    def drp_clm_na(self, *, spr=False, rtn=False, prm=None):
        """
        drop columns for nan.
        >>> from pandas import DataFrame
        >>> from numpy import nan as np_nan
        >>> tst = clmMixin(DataFrame([{"A":np_nan,'B':1}]))
        >>> tst.drp_clm_na(rtn=True)
           B
        0  1
        :param spr: let self = self.dts
        :param rtn: default False, return None
        :param prm: if na numbers in one columns bigger than prm, delete the columns
        :return: if rtn is True, return self.dts
        """
        self.dts = self.dts.dropna(axis=1,          # 0 for index and 1 for columns, if 0, subset is available
                                   how='any',       # any and all
                                   thresh=prm,      # optional drop the column if it has at least thresh nan
                                   subset=None,)    # optional columns in target
        if spr:
            self.spr_nit()
        if rtn:
            return self.dts

    def stt_clm(self, lst_clm, *args, spr=False, rtn=False, prm=None):
        """
        static columns.
        >>> from pandas import DataFrame
        >>> from numpy import sum as np_sum
        >>> tst = clmMixin(DataFrame([{'A':1,'B':2},{'A':1,'B':3},{'A':2,'B':4},{'A':2,'B':5},]))
        >>> tst.stt_clm(['A'], ['B'], [np_sum], rtn=True, prm='C')  # tst.stt_clm(['A'],{'B':np_sum},rtn=True,prm='C')
           A  C
        0  1  5
        1  2  9
        :param lst_clm: group by columns
        :param args: static columns and methods, in format ({columns:method}) or ([columns], [method])
        :param spr: let self = self.dts
        :param rtn: default False, return None
        :param prm: new name for static columns, default remain old names if None
        :return: if rtn is True, return self.dts
        """
        if type(args[0]) in [dict] and len(args) == 1:
            dct_stt = args[0]
            lst_stt = list(args[0].keys())
        else:
            lst_stt = lsz(args[0]).typ_to_lst(rtn=True)
            lst_mtd = lsz(args[1]).cpy_tal(len(lst_stt), rtn=True)
            dct_stt = lsz(lst_mtd).lst_to_typ('dct', lst_stt, rtn=True)[0]
        self.dts = self.dts.groupby(lsz(lst_clm).typ_to_lst(rtn=True))
        self.dts = self.dts.aggregate(dct_stt).reset_index(drop=False)
        if type(self.clm) is typ_pd_Index:
            lst_rnm = lsz(prm).typ_to_lst(rtn=True) if prm else lst_stt
            self.rnm_clm(lst_stt, lst_rnm)
        else:
            print("info: columns' multi-index cannot be altered.")
        if spr:
            self.spr_nit()
        if rtn:
            return self.dts

    def add_clm_rgx(self, *args, spr=False, rtn=False, prm=None):
        """
        add new columns from old columns' some part.
        >>> tst = dfz([{"A":'1=a;'},{"A":'2=b;'}])
        >>> tst.typ_to_dtf()
        >>> tst.add_clm_rgx('A', 'B', prm='1=(.*?);', rtn=True)
              A     B
        0  1=a;     a
        1  2=b;  None
        :param args: ({oldColumns: newColumns}) or ([oldColumns], [newColumns])
        :param spr: let self = self.dts
        :param rtn: default False, return None
        :param prm: a list of regex to be found in old columns
        :return: if rtn is True, return self.dts
        """
        if type(args[0]) is dict and len(args) == 1:
            lst_clm = lsz(list(args[0].keys())).typ_to_lst(rtn=True)
            lst_new = lsz(list(args[0].values())).typ_to_lst(rtn=True)
        else:
            lst_clm = lsz(args[0]).typ_to_lst(rtn=True)
            lst_new = lsz(args[0]).typ_to_lst(rtn=True) if len(args) == 1 else lsz(args[1]).typ_to_lst(rtn=True)
        prm = lsz(prm)
        prm.typ_to_lst()
        prm.cpy_tal(len(lst_clm), spr=True)
        for i in range(len(lst_clm)):
            self.dts[lst_new[i]] = [re_findall(prm[i], j)[0] if re_search(prm[i], j) else None for
                                    j in self.dts[lst_clm[i]]]
        self.dts_nit()
        if spr:
            self.spr_nit()
        if rtn:
            return self.dts

    def ltr_clm_rpc(self, lst_clm, *args, spr=False, rtn=False, prm=None):
        """
        alter columns by replacing contents.
        >>> tst = clmMixin([{'A':'1'},{'A':'abc'},{'A':None}])
        >>> tst.typ_to_dtf()
        >>> tst.ltr_clm_rpc('A','1','2', rtn=True)
              A
        0     2
        1   abc
        2  None
        >>> tst.ltr_clm_rpc('A','b','B',prm='part', rtn=True)
              A
        0     2
        1   aBc
        2  None
        >>> from math import isnan
        >>> tst = clmMixin([{'A':1},{'A':None}])
        >>> tst.typ_to_dtf()
        >>> tst.ltr_clm_rpc('A',{'':'thisIsNa'},prm=isnan, rtn=True)
                  A
        0         1
        1  thisIsNa
        :param lst_clm: target columns, all the alteration are used on each column
        :param args: ({old_content:new_content}) or (old_content, new_content)
        :param spr: let self = self.dts
        :param rtn: default False, return None
        :param prm: in [None, 'part', function]
        :return: if rtn is True, return self.dts
        """
        if type(args[0]) is dict and len(args) == 1:
            lst_old = lsz(list(args[0].keys())).typ_to_lst(rtn=True)
            lst_new = lsz(list(args[0].values())).typ_to_lst(rtn=True)
        else:
            lst_old = lsz(args[0]).typ_to_lst(rtn=True)
            lst_new = lsz(args[1])
            lst_new.typ_to_lst()
            lst_new.cpy_tal(len(lst_old), spr=True)
        lst_clm = lsz(lst_clm).typ_to_lst(rtn=True)
        for i in range(len(lst_clm)):
            for j in range(len(lst_old)):
                if prm is None:                     # 当prm为None时运行完全替换
                    self.dts[lst_clm[i]] = [lst_new[j] if k == lst_old[j] else k for k in self.dts[lst_clm[i]]]
                elif prm in ['part', 'prt']:    # 当prm为string类型且内容为'part'时运行部分替换
                    self.dts[lst_clm[i]] = [re_sub(lst_old[j], lst_new[j], k) if
                                            type(k) is str and re_search(lst_old[j], k) else
                                            k for k in self.dts[lst_clm[i]]]
                else:                           # 当prm为函数时判断满足prm()则运行替换
                    self.dts[lst_clm[i]] = [lst_new[j] if prm(k) else k for k in self.dts[lst_clm[i]]]
        self.dts_nit(False)
        if spr:
            self.spr_nit()
        if rtn:
            return self.dts

    def ltr_clm_unl(self, *args, spr=False, rtn=False):
        """
        atler columns in type str English words to upper or lower.
        >>> tst = clmMixin([{'A':1},{'A':'abC'},{'A':'ABc'}])
        >>> tst.typ_to_dtf()
        >>> tst.ltr_clm_unl('A','ppr', rtn=True)
             A
        0    1
        1  ABC
        2  ABC
        :param args: ({columns:upperNLower}) or ([columns],[upperNLower]), unl in ['upper','ppr','lower','lwr']
        :param spr: let self = self.dts
        :param rtn: default False, return None
        :return: if rtn is True, return self.dts
        """
        if type(args[0]) is dict and len(args) == 1:
            dct_typ = args[0]
        else:
            lst_clm = lsz(args[0]).typ_to_lst(rtn=True)
            lst_typ = lsz(args[1])
            lst_typ.typ_to_lst()
            lst_typ.cpy_tal(len(lst_clm), spr=True)
            dct_typ = lst_typ.lst_to_typ('dct', lst_clm, rtn=True)[0]   # args转字典
        for i_clm in [i for i in dct_typ.keys() if dct_typ[i] in ['lower', 'lwr']]:
            self.dts[i_clm] = self.dts.apply(lambda x: x[i_clm].lower() if type(x[i_clm]) is str else x[i_clm], axis=1)
        for i_clm in [i for i in dct_typ.keys() if dct_typ[i] in ['upper', 'ppr']]:
            self.dts[i_clm] = self.dts.apply(lambda x: x[i_clm].upper() if type(x[i_clm]) is str else x[i_clm], axis=1)
        self.dts_nit(False)
        if spr:
            self.spr_nit()
        if rtn:
            return self.dts

    def ltr_clm_flt(self, *args, spr=False, rtn=False, prn=True):
        """
        check columns in float, if cannot convert into float, fill None.
        >>> tst = clmMixin([{'A':1.21},{'A':2},{'A':'a'}])
        >>> tst.typ_to_dtf()
        >>> tst.ltr_clm_flt('A')
        [{'A': ['a']}]  # 返回格式[{column: [unusual cells, ...]},{},...]
        :param args: target columns, alter if len == 1, create new columns if len == 2
        :param spr: let self = self.dts
        :param rtn: default False, return None
        :param prn: print cells that cannot convert into float
        :return: if prn is True, return unusual cells; if not prn and rtn, return self.dts
        """
        lst_old = lsz(args[0]).typ_to_lst(rtn=True)
        if len(args) == 1:
            lst_clm = lsz(args[0]).typ_to_lst(rtn=True)
        else:
            lst_clm = lsz(args[1]).typ_to_lst(rtn=True)
        lst_ttl = []
        for i in range(len(lst_clm)):
            if lst_old[i] in self.clm:
                lst_err = [i for i in self.dts[lst_old[i]] if
                           (not re_match("^\d+?$", str(i))) & (not re_match("^\d+?\.\d+?$", str(i)))]
                self.dts[lst_clm[i]] = self.dts.apply(lambda x: None if
                                                      (not re_match("^\d+?$", str(x[lst_old[i]]))) &
                                                      (not re_match("^\d+?\.\d+?$", str(x[lst_old[i]]))) else
                                                      str(x[lst_old[i]]), axis=1)
                lst_ttl.append({lst_old[i]: lst_err})
        if prn:
            print('info: unusual cells %s.' % lst_ttl)
            return lst_ttl
        self.dts_nit(False)
        if spr:
            self.spr_nit()
        if rtn:
            return self.dts

    def ltr_clm_rnd(self, *args, spr=False, rtn=False, prm=2):
        """
        alter columns' decimal scales.
        >>> tst = dfz([{'A':1.111},{'A':1.1},{'A':None}])
        >>> tst.typ_to_dtf()
        >>> tst.ltr_clm_rnd('A',prm=1, rtn=True)
             A
        0  1.1
        1  1.1
        2  NaN
        :param args: ([oldColumns]) or ([oldColumns],[newColumns])
        :param spr: let self = self.dts
        :param rtn: default False, return None
        :param prm: decimal scales. default 2
        :return: if rtn is True, return self.dts
        """
        lst_old = lsz(args[0]).typ_to_lst(rtn=True)
        if len(args) == 1:
            lst_clm = lsz(args[0]).typ_to_lst(rtn=True)
        else:
            lst_clm = lsz(args[1]).typ_to_lst(rtn=True)
        prm = lsz(prm)
        prm.typ_to_lst()
        prm.cpy_tal(len(lst_clm), spr=True)
        for i in range(len(lst_clm)):
            self.dts[lst_clm[i]] = self.dts.apply(lambda x: np_nan if math_isnan(x[lst_old[i]]) else
                                                  round(x[lst_old[i]], prm[i]), axis=1)
        self.dts_nit(False)
        if spr:
            self.spr_nit()
        if rtn:
            return self.dts

    def ltr_clm_typ(self, *args, spr=False, rtn=False):
        """
        alter columns' data type.
        >>> tst = clmMixin([{'A':1,'B':'a'},{'A':2,'B': 3},{'A':'', 'B':None}])
        >>> tst.typ_to_dtf()
        >>> tst.ltr_clm_typ({'A':float,'B':str}, rtn=True)  # 空类值在转float时会转为np.nan, 转str时会转为''
        >>> # 空值类形如['NA', 'nan', 'null', 'None', '', None, np.nan]
             A  B
        0  1.0  a
        1  2.0  3
        2  NaN
        :param args: ({columns: types}) or ([columns],[types]), types in [str, float, int]
        :param spr: let self = self.dts
        :param rtn: default False, return None
        :return: if rtn is True, return self.dts
        """
        if type(args[0]) is dict and len(args) == 1:
            dct_typ = args[0]
        else:
            lst_clm = lsz(args[0]).typ_to_lst(rtn=True)
            lst_typ = lsz(args[1])
            lst_typ.typ_to_lst()
            lst_typ.cpy_tal(len(lst_clm), spr=True)
            dct_typ = lst_typ.lst_to_typ('dct', lst_clm, rtn=True)[0]                   # args转字典
        lst_flt = [i for i in dct_typ.keys() if dct_typ[i] in [int, 'int', float, 'float']]
        self.ltr_clm_rpc(lst_flt, ['NA', 'N/A', 'nan', 'null', 'None', None, ''], [np_nan])    # 转float列的空值处理
        self.dts = self.dts.astype(dct_typ)  # 核心转换
        lst_str = [i for i in dct_typ.keys() if dct_typ[i] in [str, 'str']]
        self.ltr_clm_rpc(lst_str, ['NA', 'N/A', 'nan', 'null', 'None', None], [''])            # 转str列的空值处理
        self.dts_nit(False)
        if spr:
            self.spr_nit()
        if rtn:
            return self.dts

    def cod_clm_int(self, lst_clm, *, spr=False, rtn=False, prm='encode'):
        lst_clm = lsz(lst_clm).typ_to_lst(rtn=True)
        if prm in ['encode', 'ncd']:
            self.ltr_clm_typ(lst_clm, 'str')
            self.ltr_clm_rpc(lst_clm, {'\.\d+': ''}, prm='prt')
            for i in lst_clm:
                self.dts[i] = self.dts.apply(lambda x: stz(x[i]).ncd_int(rtn=True), axis=1)
        elif prm in ['decode', 'dcd']:
            for i in lst_clm:
                self.dts[i] = self.dts.apply(lambda x: stz(x[i]).dcd_int(rtn=True), axis=1)
        self.dts_nit()
        if spr:
            self.spr_nit()
        if rtn:
            return self.dts

    def cod_clm_cvr(self, lst_clm, *, spr=False, rtn=False, prm=None):
        prm = [1, -1] if prm is None else prm
        lst_clm = lsz(lst_clm).typ_to_lst(rtn=True)
        self.ltr_clm_typ(lst_clm, 'str')
        for i in lst_clm:
            self.dts[i] = self.dts.apply(lambda x: stz(x[i]).ncd_cvr(prm), axis=1)
        self.dts_nit()
        if spr:
            self.spr_nit()
        if rtn:
            return self.dts

    def cod_clm_md5(self, *args, spr=False, rtn=False, prm=None):
        lst_old = lsz(args[0])
        lst_old.typ_to_lst(spr=True)
        if len(args) == 1:
            lst_clm = lsz(args[0]).typ_to_lst(rtn=True)
        else:
            lst_clm = lsz(args[1]).typ_to_lst(rtn=True)
        lst_old.cpy_tal(len(lst_clm), spr=True)
        self.ltr_clm_typ(lst_old.seq, 'str')
        for i in range(len(lst_clm)):
            self.dts[lst_clm[i]] = self.dts.apply(lambda x: stz(x[lst_old[i]]).ncd_md5(rtn=True, prm=prm), axis=1)
        self.dts_nit()
        if spr:
            self.spr_nit()
        if rtn:
            return self.dts


class dcmMixin(dfBsc):

    def __init__(self, dts=None, lcn=None, *, spr=False):
        super(dcmMixin, self).__init__(dts, lcn, spr=spr)

    def srt_dcm(self, lst_clm=None, scd=True, *, spr=False, rtn=False, prm='last'):
        """
        sort documents.
        :param lst_clm: target columns
        :param scd: in [True, false] for [ascending, descending], default ascending
        :param spr: let self = self.dts
        :param rtn: default False, return None
        :param prm: in ['first','last'], default 'last'
        :return: if rtn is True, return self.dts
        """
        self.dts = self.dts.sort_values(by=lst_clm, ascending=scd, na_position=prm)
        if spr:
            self.spr_nit()
        if rtn:
            return self.dts

    def drp_dcm(self, lst_ndx, *, spr=False, rtn=False):
        """
        drop documents by index numbers.
        :param lst_ndx: a list of index
        :param spr: let self = self.dts
        :param rtn: default False, return None
        :return: if rtn is True, return self.dts
        """
        if lst_ndx is None:
            pass
        else:
            lst_ndx = lsz(lst_ndx).typ_to_lst(rtn=True)
            self.dts = self.dts.drop(lst_ndx, axis=0)
        if spr:
            self.spr_nit()
        if rtn:
            return self.dts

    def drp_dcm_na(self, lst_clm=None, *, spr=False, rtn=False, prm=None):
        """
        drop documents for na cells.
        >>> import pandas as pd
        >>> tst = dfz([{'A':1,'B':'a','C':'a'},{'A':pd.NaT,'B':'b','C':'a'},{'A':pd.NaT,'B':pd.NaT,'C':'a'}])
        >>> tst.typ_to_dtf()
        >>> tst.drp_dcm_na(prm=2, rtn=True)     # 当行中至少有两单元格非空时保留
               A  B  C
        0      1  a  a
        1    NaT  b  a
        >>> tst.drp_dcm_na('B', rtn=True)
               A  B  C
        0      1  a  a
        1    NaT  b  a
        :param lst_clm: default None means do nothing
        :param spr: let self = self.dts
        :param rtn: default False, return None
        :param prm:  remain documents at least <prm> cells not nan, default None
        :return: if rtn is True, return self.dts
        """
        flt_bgn = self.len
        lst_clm = lsz(lst_clm).typ_to_lst(rtn=True) if not prm else lst_clm
        self.dts = self.dts.dropna(axis=0,          # 0 for index and 1 for columns
                                   how='any',       # any and all
                                   thresh=prm,      # optional Keep only the rows with at least 2 non-NA values
                                   subset=lst_clm)  # optional columns in target
        print('info: %i remains from %i by dropping.' % (self.len, flt_bgn))
        if spr:
            self.spr_nit()
        if rtn:
            return self.dts

    def drp_dcm_dpl(self, *args, spr=False, rtn=False, prm='first', scd=True, prn=True):
        """
        drop duplicate documents.
        :param args: ([columnsDropping]) or ([columnsDropping], [columnsSorting])
        :param scd: if len(args) == 2, define the sorting method
        :param spr: let self = self.dts
        :param rtn: default False, return None
        :param prm: dropping method, in ['first','last',None], default first
        :param prn: print detail of dropping duplicates or not, default True
        :return: if rtn is True, return self.dts
        """
        lst_dpl = lsz(args[0]).typ_to_lst(rtn=True)
        if len(args) > 1:
            lst_srt = lsz(args[1]).typ_to_lst(rtn=True)
            self.srt_dcm(lst_srt, scd)
        len_bfr = self.len
        self.dts = self.dts.drop_duplicates(subset=lst_dpl, keep=prm)
        if prn:
            print("info: drop duplicate documents, %.i in total %.i left, from <%s>." % (len_bfr, self.len, self))
        if spr:
            self.spr_nit()
        if rtn:
            return self.dts

    def drp_dcm_ctt(self, *args, spr=False, rtn=False, prm=True):
        """
        drop documents for contents.
        >>> tst = dfz([{'A':'a','B':'1'},{'A':'b','B':'2'},{'A':'c','B':'3'},{'A':'d','B':'4'},])
        >>> tst.typ_to_dtf()
        >>> tst.drp_dcm_ctt(['A','B'],[['a','b'],['2','3']],rtn=True)
           A  B
        0  d  4
        :param args: ({columns:[content,..]}) or ([column,..],[[content,..],..])
        :param spr: let self = self.dts
        :param rtn: default False, return None
        :param prm: in [(True,'drop'),(False,'keep'),'partDrop','partKeep'], default True for dropping documents.
        :return: if rtn is True, return self.dts
        """
        if type(args[0]) is dict and len(args) == 1:
            lst_clm = lsz(list(args[0].keys()))
            lst_ctt = lsz(list(args[0].values()))
        else:
            lst_clm = lsz(args[0])
            lst_ctt = lsz(args[1])
        lst_clm.typ_to_lst(spr=True)
        lst_ctt.typ_to_lst(spr=True)
        if len(lst_clm) == 1 and type(lst_ctt[0]) not in [list, lsz]:
            lst_ctt = lsz([lst_ctt.seq])            # 当仅检查单列时，需要控制其待识别内容为唯一个列表
        lst_clm.cpy_tal(lsz([lst_clm.seq, lst_ctt.seq]).edg_of_len(rtn=True)[1], spr=True)
        lst_ctt.cpy_tal(lsz([lst_clm.seq, lst_ctt.seq]).edg_of_len(rtn=True)[1], spr=True)
        for i in range(len(lst_clm)):
            if prm is False or prm in ['keep', 'kep']:   # 当not drop 或prm='keep'时，保留完全匹配的行
                self.dts = self.dts.loc[self.dts[lst_clm[i]].isin(lsz(lst_ctt[i]).typ_to_lst(rtn=True)), :]
            elif prm is True or prm in ['drop', 'drp']:     # 当drop 或prm='drop'时，删除完全匹配的行
                self.dts = self.dts.loc[~self.dts[lst_clm[i]].isin(lsz(lst_ctt[i]).typ_to_lst(rtn=True)), :]
            elif prm in ['partDrop', 'prtDrp']:     # 当prm='partDrop'时，删除正则匹配的行
                lst_ndx = [k for j in lst_ctt[i] for k in self.dts.index if
                           re_search(j, str(self.dts.loc[k, lst_clm[i]]))]
                self.dts = self.dts.drop(lst_ndx, axis=0)
            elif prm in ['partKeep', 'prtKep']:     # 当prm='partKeep'时，保留正则匹配的行
                lst_ndx = [k for j in lst_ctt[i] for k in self.dts.index if
                           re_search(j, str(self.dts.loc[k, lst_clm[i]]))]
                lst_ndx = [i for i in self.dts.index if i not in lst_ndx]
                self.dts = self.dts.drop(lst_ndx, axis=0)
        if spr:
            self.spr_nit()
        if rtn:
            return self.dts


class cllMixin(dcmMixin, clmMixin):

    def __init__(self, dts=None, lcn=None, *, spr=False):
        super(cllMixin, self).__init__(dts, lcn, spr=spr)

    def mrg_dtf(self, dtf_mrg, *args, spr=False, rtn=False, prm='outer'):
        """merge dataframe horizontally or vertically.
        >>> import pandas as pd
        >>> tst = dfz()
        >>> tst.mrg_dtf_hrl(pd.DataFrame([{"A":1}]), pd.DataFrame([{"A":2}]), prm='vrl', rtn=True)
           A
        0  1
        1  2
        >>> tst = dfz([{'A':1,'B':'a'},{'A':2,'B':'b'}])
        >>> tst.typ_to_dtf()
        >>> tst.mrg_dtf_hrl(pd.DataFrame([{'A':2,'C':'x'}]), 'A', prm='outer', rtn=True)
           A  B    C
        0  1  a  NaN
        1  2  b    x
        :param dtf_mrg: if prm is 'vrl': merge[self.dts,dtf_mrg,*args]; else: name of the dataframe merged on the right side
        :param args: the column names for fitting in self.dts and dtf_mrg, len(lst_str_clm_on) in [1,2]
        :param spr: let self = self.dts
        :param rtn: default False, return None
        :param prm: pandas.merge(how=str_how), in ['inner', 'outer', 'left', 'right','dpl'];
            str_how in ['dpl']: drop documents in self.dts matched in dtf_mrg
        :return: if rtn is True, return self.dts
        """
        if prm in ['vertical', 'vrl']:              # merging vertically
            lst_dtf = []
            if self.len > 0:
                lst_dtf.extend([self.dts])
            lst_dtf.extend([dtf_mrg])
            if args:
                lst_dtf.extend(lsz(args).typ_to_lst(rtn=True))
            self.dts = concat(lst_dtf, ignore_index=True, keys=None, axis=0, sort=False)
        elif prm in ['outer_index', 'ndx']:         # merging by indexes
            self.dts = merge(self.dts, dtf_mrg, how='outer', left_index=True, right_index=True)
        else:
            lst_clm_on = lsz(args)
            lst_clm_on.typ_to_lst()
            lst_clm_on.cpy_tal(2, spr=True)         # 扩充用于匹配的字段名以满足left/right_on的需求
            if prm in ['inner', 'outer', 'left', 'right']:                      # traditional merging
                self.dts = merge(self.dts, dtf_mrg, how=prm,
                                 left_on=lsz(lst_clm_on[0]).typ_to_lst(rtn=True),
                                 right_on=lsz(lst_clm_on[1]).typ_to_lst(rtn=True))
            elif prm in ['dpl']:                    # merge and drop duplicates
                dtf_mrg_drp = dtf_mrg[lsz(lst_clm_on[1]).typ_to_lst(rtn=True)]  # 仅保留用于匹配的列，避免对self.dts的污染
                self.dts = merge(self.dts, dtf_mrg_drp, how='left',
                                 left_on=lsz(lst_clm_on[0]).typ_to_lst(rtn=True),
                                 right_on=lsz(lst_clm_on[1]).typ_to_lst(rtn=True),
                                 indicator=True)    # 使用自动定义的列'_merge'标记来源
                self.dts = self.dts.loc[self.dts['_merge'] != 'both', :].drop(['_merge'], axis=1)
        if spr:
            self.spr_nit()
        if rtn:
            return self.dts


class tmsMixin(cllMixin):

    def __init__(self, dts=None, lcn=None, *, spr=False):
        super(tmsMixin, self).__init__(dts, lcn, spr=spr)

    def typ_to_tms(self, *args, spr=False, rtn=False):
        """
        columns' type from others to datetime.datetime
        >>> tst = dfz([{'A':'1989-04-21'},{'A':'1989/4/21'}])
        >>> tst.typ_to_dtf()
        >>> tst.typ_to_tms('A','B')
                   A          B
        0 1989-04-21 1989-04-21
        1 1989-04-21 1989-04-21
        :param args: (columns) or (columns, newColumns) or ({columns: newColumns})
        :param spr: let self = self.dts
        :param rtn: default False, return None
        :return: if rtn is True, return self.dts
        """
        if type(args[0]) is dict and len(args) == 1:
            lst_old = lsz(list(args[0].keys())).typ_to_lst(rtn=True)
            lst_new = lsz(list(args[0].values())).typ_to_lst(rtn=True)
        elif len(args) == 1:
            lst_old = lsz(args[0]).typ_to_lst(rtn=True)
            lst_new = lsz(args[0]).typ_to_lst(rtn=True)
        else:
            lst_old = lsz(args[0])
            lst_new = lsz(args[1]).typ_to_lst(rtn=True)
            lst_old.typ_to_lst()
            lst_old.cpy_tal(len(lst_new), spr=True)
        for i in range(len(lst_old)):
            self.dts[lst_new[i]] = self.dts.apply(lambda x: dtz(x[lst_old[i]]).typ_to_dtt(rtn=True), axis=1)
        self.dts_nit()
        if spr:
            self.spr_nit()
        if rtn:
            return self.dts

    def tms_to_typ(self, *args, spr=False, rtn=False, prm='%Y-%m-%d'):
        """
        columns from timestamp to other types.
        >>> tst = dfz([{'A':'2019-04-21'},{'A':'2019/4/21'}])
        >>> tst.typ_to_dtf()
        >>> tst.tms_to_typ('A','yday',rtn=True)
             A
        0  111
        1  111
        :param args: ([columns],[types]) or ({column:type,...})
        :param spr: let self = self.dts
        :param rtn: default False, return None
        :param prm: if type in 'str', define the format of the column
        :return: if rtn is True, return self.dts
        """
        if type(args[0]) is dict and len(args) == 1:
            lst_clm = lsz(list(args[0].keys())).typ_to_lst(rtn=True)
            lst_typ = lsz(list(args[0].values())).typ_to_lst(rtn=True)
        else:
            lst_clm = lsz(args[0]).typ_to_lst(rtn=True)
            lst_typ = lsz(args[1])
            lst_typ.typ_to_lst()
            lst_typ.cpy_tal(len(lst_clm), spr=True)
        for i in range(len(lst_clm)):
            if lst_typ[i] in ['w', 'm']:
                self.dts[lst_clm[i]] = self.dts.apply(
                    lambda x: dtz(x[lst_clm[i]]).dtt_to_prd(lst_typ[i], rtn=True), axis=1)
            else:
                self.dts[lst_clm[i]] = self.dts.apply(
                    lambda x: dtz(x[lst_clm[i]]).dtt_to_typ(lst_typ[i], prm, rtn=True), axis=1)
        self.dts_nit()
        if spr:
            self.spr_nit()
        if rtn:
            return self.dts


class pltMixin(cllMixin):

    def __init__(self, dts=None, lcn=None, *, spr=False):
        super(pltMixin, self).__init__(dts, lcn, spr=spr)


class dfz(tmsMixin, pltMixin):

    def __init__(self, dts=None, lcn=None, *, spr=False):
        super(dfz, self).__init__(dts, lcn, spr=spr)
