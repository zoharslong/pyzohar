#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 22 16:30:38 2019
dataframe operation.
@author: zoharslong

@alters:
2020-02-16 zoharslong
"""
from pandas.core.indexes.base import Index as typ_pd_Index              # 定义dataframe.columns类型
from bsc import lsz
from ioz import ioz


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


print('info: multiple io\'s alteration ready.')
