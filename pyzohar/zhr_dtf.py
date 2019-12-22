#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 22 16:30:38 2019

@author: sl
@alters: 19-03-26 sl
"""
from os import path
from numpy import min as np_min, max as np_max, sum as np_sum, nan as np_nan
from pandas import DataFrame, merge, concat, cut, read_excel, Timestamp as pd_Timestamp     # 可执行的操作
from pandas.core.series import Series as typ_pd_Series          # 定义pd.series类型
from pandas.core.frame import DataFrame as typ_pd_DataFrame     # 定义pd.dataframe类型
from pandas._libs.tslib import Timestamp as typ_pd_Timestamp    # 定义pandas.Timestamp类型
from pandas.core.indexes.multi import MultiIndex as typ_pd_multiindex
from pandas.core.indexes.base import Index as typ_pd_index
from time import mktime, strftime
from datetime import date as typ_dt_date, time as typ_dt_time, timedelta as typ_dt_dlt, datetime as dt_datetime
from progressbar import ProgressBar, UnknownLength
from re import search, findall, sub, match
from warnings import warn
from .zhr_bsc import fnc_lst_cpy_cll, fnc_lst_frm_all, fnc_dct_frm_lst, fnc_rgx_frm_cll, \
    fnc_lst_cll_mrg, fnc_lst_frm_cll_mrg, fnc_lst_frm_dtf_dtp, fnc_flt_for_lst_len, dtt_fmt_rgx
from .zhr_dts import PrcDts
from .zhr_ncd import fnc_ncd_mbl_str, fnc_dcd_mbl_str, fnc_ncd_str_hsh

class PrcDtfClm(PrcDts):
    """
    processing on columns in dataframe
    """
    def __init__(self, dts_mpt=None, bln_rst_ndx=False):
        """initiate
        define self.dts, self.len, self.typ
        :param dts_mpt: var name of target dataset
        :return: None
        """
        super(PrcDtfClm, self).__init__(dts_mpt, bln_rst_ndx)

    def srt_clm(self, lst_clm_srt, bln_drp=True, bln_rst_ndx=True):
        """alter columns for sort
        :param lst_clm_srt: list of columns' name in type str, columns not mentioned will put at the end
        :param bln_drp: if lst_clm_srt does not mention, then drop the columns from self.dts
        :param bln_rst_ndx: reset the final output's index or not
        :return: None
        """
        lst_fnl = []
        lst_clm_srt = fnc_lst_frm_all(lst_clm_srt)
        for i_cll in lst_clm_srt:  # 对self.dts中存在的columns, 按照lst_clm_srt排序
            if list({i_cll} & set(self.clm)):
                lst_fnl.append(i_cll)
        if bln_drp:  # bln_drp为True，则舍弃self.clm中，未出现再lst_clm_srt中的列
            pass
        else:  # bln_drp为False，则将self.clm中有而lst_clm_srt未提及的列附加在后面
            x = fnc_lst_frm_cll_mrg('differ', lst_fnl, self.clm)
            lst_fnl.extend(x)
        self.dts = self.dts[lst_fnl]
        self.init_rst(bln_rst_ndx)

    def rnm_clm(self, lst_clm, lst_clm_xpt=None, bln_rst_ndx=True):
        """rename columns
        :param lst_clm:
        :param lst_clm_xpt:
        :param bln_rst_ndx:
        :return:
        """
        if lst_clm_xpt is None and type(lst_clm) in [dict]:
            self.dts.rename(columns=lst_clm, inplace=True)
        else:
            self.dts.rename(columns=fnc_dct_frm_lst(lst_clm, lst_clm_xpt), inplace=True)
        self.init_rst(bln_rst_ndx)

    def fll_clm_na(self, dct_clm=None, str_method=None, lst_clm_fll=None):
        """fill na cells(pd.fillna)
            :param dct_clm: a dict of column and how to fill na, format in {'column name':(what to fill)}
            :param str_method: in ['backfill', 'bfill', 'pad', 'ffill', None]
            :param lst_clm_fll: if dct_clm is a list, then needs to build a dict with values
            :return: None
            """
        if type(dct_clm) in [list,str] and lst_clm_fll is not None:
            lst_clm = fnc_lst_frm_all(dct_clm)
            lst_clm_fll = fnc_lst_frm_all(lst_clm_fll)
            flt_len = fnc_flt_for_lst_len([lst_clm, lst_clm_fll])
            lst_clm = fnc_lst_cpy_cll(lst_clm, flt_len)
            lst_clm_fll = fnc_lst_cpy_cll(lst_clm_fll, flt_len)
            dct_clm = fnc_dct_frm_lst(lst_clm, lst_clm_fll)
        self.dts.fillna(value=dct_clm, method=str_method, inplace=True)
        self.init_rst()

    def drp_clm(self, lst_clm, bln_rst_ndx=True):
        """drop columns by names
        :param lst_clm:
        :param bln_rst_ndx:
        :return:
        """
        lst_clm = fnc_lst_frm_all(lst_clm)
        self.dts.drop(lst_clm, axis=1, inplace=True)
        self.init_rst(bln_rst_ndx)

    def drp_clm_na(self, flt_thresh=None, bln_rst_ndx=True):
        """drop column which has a certain percent of na()
        :param flt_thresh: the percent of na, default None, in [0,1]
        :param bln_rst_ndx:
        :return: None
        # dropna: http://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.dropna.html
        # optional function: https://blog.csdn.net/zhuiqiuuuu/article/details/72858835
        """
        if flt_thresh is None:
            flt_thresh = int(1)
        else:
            flt_thresh = int(flt_thresh * self.len)
        self.dts.dropna(axis=1,  # 0 for index and 1 for columns
                        how='any',  # any and all
                        thresh=flt_thresh,  # optional drop the column if it has at least flt_thresh percent of non-NA
                        # subset=["tmp"],   # optional columns in target
                        inplace=True)  # optional inplace dtf_input
        self.init_rst(bln_rst_ndx)

    def add_clm_frm_dcm(self, str_clm, lst_clm_rmn=None, lst_clm_grp=None, bln_long_name=True):
        """
        根据某列进行分类，不同分类的行转列，实现缩行扩列，如
        self.add_clm_frm_dcm('est',['date','vls'],'date'):
        DataFrame([{'est':'A','date':'1','vls':1},{'est':'B','date':'1','vls':2}]) ->
        DataFrame([{'date':1,'A':1},'date':1,'B':2])
        :param str_clm: the target column to set its unique cells in columns
        :param lst_clm_rmn: 除扩列以外希望保留的全部列名
        :param lst_clm_grp: 用于横向拼接合并的key列名
        :param bln_long_name: 转换而来的字段名是拼接长名或直接使用列str_clm中内容的短名
        :return: None
        """
        dtf_fnl = DataFrame([])
        lst_clm_cll = self.dts[str_clm].unique()
        if lst_clm_rmn is None:
            lst_clm_rmn = self.clm.copy()
        lst_clm_rmn = fnc_lst_frm_all(lst_clm_rmn)
        for i in range(lst_clm_cll.shape[0]):
            lst_clm_tmp = lst_clm_rmn.copy()
            if lst_clm_grp is not None:
                lst_clm_grp = fnc_lst_frm_all(lst_clm_grp)
                for i_clm in lst_clm_grp:
                    lst_clm_tmp.remove(i_clm)
            if bln_long_name:
                lst_clm_rnm = lst_clm_cll[i]                                    # 直接使用变量名作为新字段名
            else:
                lst_clm_rnm = fnc_lst_cll_mrg(lst_clm_tmp, lst_clm_cll[i])      # 拼接字段名和变量名作为新字段名
            dct_clm_rnm = fnc_dct_frm_lst(lst_clm_tmp, lst_clm_rnm)
            dtf_tmp = self.dts.loc[self.dts[str_clm] == lst_clm_cll[i], :][lst_clm_rmn]
            dtf_tmp.rename(columns=dct_clm_rnm, inplace=True)
            dtf_fnl = dtf_tmp.copy() if dtf_fnl.shape[0] == 0 else merge(dtf_fnl, dtf_tmp, how='outer', on=lst_clm_grp)
        self.dts = dtf_fnl
        self.init_rst()

    def add_clm_frm_stt(self, lst_clm_grb, dct_clm, lst_mtd=None, lst_clm_add=None):
        """add columns from stat other columns
        :param lst_clm_grb: 汇总统计列名组成的列表
        :param lst_clm_add: 新加列名
        :param dct_clm: 此处既可以是已经拼好的计算方法字典，也可以是需要计算的列名列表
        :param lst_mtd: 当dct_clm已经是完整计算方法字典的时候，此处为None，default None
        :return: None
        """
        lst_clm_grb = fnc_lst_frm_all(lst_clm_grb)
        if type(dct_clm) in [dict] and lst_mtd is None:
            lst_clm = fnc_lst_frm_all(list(dct_clm.keys()))
        else:
            lst_clm = fnc_lst_frm_all(dct_clm)
            lst_mtd = fnc_lst_cpy_cll(lst_mtd, len(lst_clm))
            dct_clm = fnc_dct_frm_lst(lst_clm, lst_mtd)
        self.dts = self.dts.groupby(lst_clm_grb)
        self.dts = self.dts.aggregate(dct_clm)
        self.init_rst(True, False)
        if type(self.clm) is typ_pd_index:
            lst_clm_add = fnc_lst_frm_all(lst_clm_add) if lst_clm_add is not None else lst_clm
            self.rnm_clm(lst_clm, lst_clm_add)
        elif type(self.clm) is typ_pd_multiindex and lst_clm_add is not None:
            self.dts.columns = [i[0]+'_'+i[1] for i in self.dts.columns.to_list()]
            print('info: multiindex cannot rename')
        elif type(self.clm) is typ_pd_multiindex and lst_clm_add is None:
            print('info: multiindex remained.')

    def add_clm_frm_clm_rgx(self, lst_clm, lst_clm_add, lst_rgx, bln_rst_ndx=True):
        """
        add columns from column in type str by regex
        :param lst_clm: original columns
        :param lst_clm_add: new columns
        :param lst_rgx: a regex to find same part in original columns
        :param bln_rst_ndx: reset index or not, default True
        :return: None
        """
        lst_clm = fnc_lst_frm_all(lst_clm)
        lst_clm_add = fnc_lst_frm_all(lst_clm_add)
        lst_rgx = fnc_lst_frm_all(lst_rgx)
        flt_len = fnc_flt_for_lst_len([lst_clm, lst_rgx, lst_clm_add])
        lst_clm = fnc_lst_cpy_cll(lst_clm, flt_len)
        lst_rgx = fnc_lst_cpy_cll(lst_rgx, flt_len)
        lst_clm_add = fnc_lst_cpy_cll(lst_clm_add, flt_len)
        for i in range(len(lst_clm_add)):
            if lst_clm_add[i] not in self.clm:
                self.dts[lst_clm_add[i]] = self.dts.apply(
                    lambda x: str(findall(lst_rgx[i], str(x[lst_clm[i]]))[0]) if
                    search(lst_rgx[i], str(x[lst_clm[i]])) else None, axis=1)
            else:
                self.dts[lst_clm_add[i]] = self.dts.apply(
                    lambda x: str(findall(lst_rgx[i], str(x[lst_clm[i]]))[0]) if
                    search(lst_rgx[i], str(x[lst_clm[i]])) else x[lst_clm_add[i]], axis=1)
            self.init_rst(bln_rst_ndx)

    def add_clm_frm_clm_dtf(self, str_clm, bln_rst_ndx=True):
        """
        若dataframe中某列中为形如[{},{},..]的可转化为dataframe的结构，则解出拼接至原dataframe，导致原dataframe的行列均扩增
        :param str_clm: 列单元格内容为[{},{},..]格式的列名
        :param bln_rst_ndx: 是否重置索引，默认是
        :return: None
        """
        dtf_dtl = DataFrame([])
        for i in range(self.len):
            len_tmp = len(self.dts[str_clm][i]) if type(self.dts[str_clm][i]) is list else 1  # 兼容{}, [{},{}]两种输入
            dtf_i = DataFrame(self.dts[str_clm][i], index=[int(i)]*len_tmp)
            dtf_dtl = concat([dtf_dtl, dtf_i], axis=0,sort=False)
        self.dts.drop([str_clm], axis=1, inplace=True)
        self.dts = concat([dtf_dtl, self.dts], axis=1, join='outer', join_axes=[dtf_dtl.index], sort=False)
        self.init_rst(bln_rst_ndx)

    def add_clm_dff(self, lst_clm_srt, bln_ascending, lst_clm_grb, lst_clm_mpt, lst_clm_xpt, bln_chk=True, bln_bar=False):
        """add columns from other columns difference
        :param lst_clm_srt: sort before calculation
        :param bln_ascending: sort ascending or descending
        :param lst_clm_grb: sum calculation under groups of a list of columns
        :param lst_clm_mpt: name of the columns to calculate
        :param lst_clm_xpt: the results of calculation
        :param bln_chk: some documents may be error(type n - 0 - n), dealing with this case automatically, default True
        :param bln_bar: use progressbar or not, default False
        :return: None
        """
        bar = ProgressBar(max_value=UnknownLength) if bln_bar else None
        self.init_dts(self.dts.sort_values(by=lst_clm_srt, ascending=bln_ascending, na_position='last'))
        self.init_rst()
        self.typ_dts_to_dtf()
        for k in range(len(lst_clm_mpt)):
            for i in range(self.len - 1):
                if bln_bar:
                    bar.update(i)
                if bln_chk is True and i < self.len - 2 and \
                        (((self.dts.loc[i, lst_clm_mpt[k]] + self.dts.loc[i+2, lst_clm_mpt[k]])/2 -
                          abs(self.dts.loc[i, lst_clm_mpt[k]] - self.dts.loc[i+2, lst_clm_mpt[k]]) >
                          self.dts.loc[i+1, lst_clm_mpt[k]]) or
                         ((self.dts.loc[i, lst_clm_mpt[k]] + self.dts.loc[i+2, lst_clm_mpt[k]])/2 +
                          abs(self.dts.loc[i, lst_clm_mpt[k]] - self.dts.loc[i+2, lst_clm_mpt[k]]) <
                          self.dts.loc[i+1, lst_clm_mpt[k]])):
                    self.dts.loc[i+1, lst_clm_mpt[k]] = \
                        (self.dts.loc[i, lst_clm_mpt[k]] + self.dts.loc[i+2, lst_clm_mpt[k]])/2
                # process for diff calculation
                cnt_lst_clm_grb = 0
                if lst_clm_grb is not None:
                    for j in lst_clm_grb:
                        if self.dts.loc[i, j] != self.dts.loc[i+1, j]:
                            cnt_lst_clm_grb += 1
                        else:
                            pass
                else:
                    pass
                if cnt_lst_clm_grb == 0:
                    self.dts.loc[i, lst_clm_xpt[k]] = \
                        self.dts.loc[i, lst_clm_mpt[k]] - self.dts.loc[i+1, lst_clm_mpt[k]]
                else:
                    self.dts.loc[i, lst_clm_xpt[k]] = None
                if bln_chk is True and i > 0 and\
                        self.dts.loc[i, lst_clm_xpt[k]] + self.dts.loc[i-1, lst_clm_xpt[k]] == 0:
                    self.dts.loc[i, lst_clm_xpt[k]] = 0
                    self.dts.loc[i-1, lst_clm_xpt[k]] = 0
                else:
                    pass
        self.init_rst()

    def add_clm_frm_vls_to_cts(self, str_clm_flt, str_clm_xpt, str_cts_typ, flt_cts_num,
                               lst_clm_xpt_rnm=None, bln_include_lowest=True, *flt_key_point):
        """
        add columns from cutting float to categories
        :param str_clm_flt: the original float column to be cut
        :param str_clm_xpt: the final category column
        :param str_cts_typ: define a method for cutting, in ['num','width']
        :param flt_cts_num: how many pieces to cut in type 'num', or the width of each cut in 'width'
        :param lst_clm_xpt_rnm: each piece's name of categories
        :param bln_include_lowest: if the min num contains in the first category or not
        :param flt_key_point: the min and max of categories, cells out of range will set to first and last categories
        :return: None
        """
        flt_clm_min = np_min(self.dts[str_clm_flt])  # 自动产生分箱的上下界
        flt_clm_max = np_max(self.dts[str_clm_flt])
        if len(flt_key_point) in [1, 2]:             # 在flt_key_point有输入的情况下手工指定分箱的上下界
            bln_include_lowest = False               # 手工指定上下限情况下需要关闭自动包含下界选项
            flt_clm_min = flt_key_point[0]
            try:
                flt_clm_max = flt_key_point[1]
            except IndexError:
                pass
        # 构建分箱规则列表
        lst_bns = [flt_clm_min]
        flt_clm_cts = (flt_clm_max - flt_clm_min) / flt_cts_num
        if str_cts_typ is 'num':        # option: 确定分箱数量
            for i in range(int(flt_cts_num)):
                lst_bns.append(flt_clm_min+(i+1)*flt_clm_cts)
        elif str_cts_typ is 'width':    # option: 确定分箱宽度
            from math import ceil
            for i in range(ceil(flt_clm_cts)):
                lst_bns.append(flt_clm_min+(i+1)*flt_cts_num)
        # 基础分箱
        self.dts[str_clm_xpt] = cut(self.dts[str_clm_flt], lst_bns,
                                    labels=lst_clm_xpt_rnm, include_lowest=bln_include_lowest)
        self.init_rst()
        # 当手工指定分箱上下界之一时，读取第一个和最后一个分类的值，用于手动赋值给手动设置的范围以外的行
        if len(flt_key_point) >= 1:
            cll_min = self.dts.loc[(self.dts[str_clm_flt] > lst_bns[0]) & (self.dts[str_clm_flt] <= lst_bns[1]),
                                   str_clm_xpt]
            cll_min = cll_min[cll_min.index[0]]
            cll_max = self.dts.loc[(self.dts[str_clm_flt] > lst_bns[-2]) & (self.dts[str_clm_flt] <= lst_bns[-1]),
                                   str_clm_xpt]
            cll_max = cll_max[cll_max.index[0]]
            self.dts[str_clm_xpt] = self.dts.apply(lambda x:
                                                   cll_min if x[str_clm_flt] <= lst_bns[0] else x[str_clm_xpt], axis=1)
        if len(flt_key_point) >= 2:
            self.dts[str_clm_xpt] = self.dts.apply(lambda x:
                                                   cll_max if x[str_clm_flt] > lst_bns[-1] else x[str_clm_xpt], axis=1)
        self.init_rst()

    def ltr_clm_rpc(self, lst_clm, lst_cll, lst_cll_rpc, bln_print=True):
        """
        replace cells in target columns from lst_cll to lst_cll_rpc
        :param lst_clm: the columns' name you wanna replace some cells
        :param lst_cll: cells' contents originally
        :param lst_cll_rpc: the contents of cells after replacing
        :param bln_print: 是否打印提示
        :return: None
        """
        # match the longest params in type list
        lst_clm = fnc_lst_frm_all(lst_clm)
        lst_cll = fnc_lst_frm_all(lst_cll)
        lst_cll_rpc = fnc_lst_frm_all(lst_cll_rpc)
        flt_len = fnc_flt_for_lst_len([lst_clm, lst_cll, lst_cll_rpc])
        lst_cll = fnc_lst_cpy_cll(lst_cll, flt_len)
        lst_clm = fnc_lst_cpy_cll(lst_clm, flt_len)
        lst_cll_rpc = fnc_lst_cpy_cll(lst_cll_rpc, flt_len)
        # alter columns one by one
        for i in range(len(lst_cll)):
            if lst_clm[i] in self.clm:
                self.dts[lst_clm[i]] = self.dts.apply(
                    lambda x: lst_cll_rpc[i] if x[lst_clm[i]] == lst_cll[i] else x[lst_clm[i]], axis=1)
                self.init_rst()
            elif bln_print:
                print("info: %s not in columns" % lst_clm[i])
            else:
                pass

    def ltr_clm_rpc_prt(self, lst_clm, lst_cll_rgx, lst_cll_rpc=None, bln_ndx_rst=True):
        """
        alter columns' contents by replacing str partially
        :param lst_clm: target columns' names
        :param lst_cll_rgx: list of regex to replace, or a dict in format {lst_cll_rgx: lst_cll_rpc, ...}
        :param lst_cll_rpc: replace regex with cells in this list, default None(兼容lst_cll_rgx为字典直接指明替换关系的方法)
        :param bln_ndx_rst: reset indexes or not, default True
        :return: None
        """
        lst_clm = fnc_lst_frm_all(lst_clm)
        if type(lst_cll_rgx) in [dict] and lst_cll_rpc is None:
            lst_cll_rpc = fnc_lst_frm_all(list(lst_cll_rgx.values()))
            lst_cll_rgx = fnc_lst_frm_all(list(lst_cll_rgx.keys()))
        else:
            lst_cll_rgx = fnc_lst_frm_all(lst_cll_rgx)
            lst_cll_rpc = fnc_lst_frm_all(lst_cll_rpc)
            lst_cll_rgx = fnc_lst_cpy_cll(lst_cll_rgx, max(len(lst_clm), len(lst_cll_rgx), len(lst_cll_rpc)))
            lst_cll_rpc = fnc_lst_cpy_cll(lst_cll_rpc, max(len(lst_clm), len(lst_cll_rgx), len(lst_cll_rpc)))
        lst_clm = fnc_lst_cpy_cll(lst_clm, max(len(lst_clm), len(lst_cll_rgx), len(lst_cll_rpc)))
        for i in range(len(lst_clm)):
            if self.typ is typ_pd_DataFrame and lst_clm[i] in self.clm:
                self.dts[lst_clm[i]] = self.dts.apply(
                    lambda x: x[lst_clm[i]] if (x[lst_clm[i]] is None) | (type(x[lst_clm[i]]) is not str) else
                    sub(lst_cll_rgx[i], lst_cll_rpc[i], x[lst_clm[i]]), axis=1)
            elif self.typ is typ_pd_Series and lst_clm[i] in self.clm:
                self.dts = self.dts.apply(
                    lambda x: x if (x is None) | (type(x) is not str) else sub(lst_cll_rgx[i], lst_cll_rpc[i], x))
            elif lst_clm[i] not in self.clm:
                print("info: %s not in columns" % lst_clm[i])
            self.init_rst(bln_ndx_rst)

    def ltr_clm_typ(self, lst_clm, lst_typ=None):
        """
        alter columns' types, cannot deal with type dict.
        :param lst_clm: a list of columns waiting for alter, also can be retype dict like {column: type,..}
        :param lst_typ: a list of types matched each columns in lst_clm, need [float, str, int]
        :return: None
        """
        if type(lst_clm) in [dict] and lst_typ is None:
            dct_typ = lst_clm.copy()
        else:
            lst_clm = fnc_lst_frm_all(lst_clm)
            lst_typ = fnc_lst_cpy_cll(lst_typ, len(lst_clm))
            dct_typ = fnc_dct_frm_lst(lst_clm, lst_typ).copy()
        for i in list(dct_typ.keys()):
            if dct_typ[i] in ['dict','dct',dict]: dct_typ.pop(i)    # 类型为dict的话只能听天由命，此处无法调整
        for i in list((set(dct_typ.keys()) ^ set(self.clm)) & set(dct_typ.keys())):
            dct_typ.pop(i)
        self.dts = self.dts.astype(dct_typ)
        lst_clm_o = fnc_lst_frm_dtf_dtp(self.dts, 'O')
        for i_clm in lst_clm_o:
            self.ltr_clm_rpc(i_clm, ['NA', 'nan', 'null', 'None', None], np_nan)

    def ltr_clm_typ_unl(self, lst_clm, lst_typ='lower'):
        """
        atler columns in type str English words to lower or upper
        :param lst_clm: columns' name in list
        :param lst_typ: in ['lower','upper]
        :return: None
        """
        lst_clm = fnc_lst_frm_all(lst_clm)
        lst_typ = fnc_lst_frm_all(lst_typ)
        flt_len = fnc_flt_for_lst_len([lst_clm, lst_typ])
        lst_clm = fnc_lst_cpy_cll(lst_clm, flt_len)
        lst_typ = fnc_lst_cpy_cll(lst_typ, flt_len)
        lst_clm_lwr = [lst_clm[i] for i in range(len(lst_clm)) if lst_typ[i]=='lower']
        lst_clm_ppr = [lst_clm[i] for i in range(len(lst_clm)) if lst_typ[i]=='upper']
        for i_clm in lst_clm_lwr:
            self.dts[i_clm] = self.dts.apply(lambda x: x[i_clm].lower() if type(x[i_clm]) is str else x[i_clm],axis=1)
        for i_clm in lst_clm_ppr:
            self.dts[i_clm] = self.dts.apply(lambda x: x[i_clm].upper() if type(x[i_clm]) is str else x[i_clm],axis=1)
        self.init_rst()

    def ltr_clm_typ_for_flt(self, lst_clm, lst_typ):
        """alter columns' type for the columsn in format [0-9]+[.]*[0-9]*
        强制将format不符合数值的cell转换为None
        :param lst_clm: a list of columns waiting for alter
        :param lst_typ: a list of types matched each columns in lst_clm
        :return: None
        """
        warn("warning: maybe drop some cells useful")
        for i_clm in lst_clm:
            if i_clm in self.clm:
                self.dts[i_clm] = \
                    self.dts.apply(lambda x: None if (match("^\d+?$", str(x[i_clm])) is None) &
                                                     (match("^\d+?\.\d+?$", str(x[i_clm])) is None) else
                                   str(x[i_clm]), axis=1)
        self.ltr_clm_typ(lst_clm, lst_typ)

    def ltr_clm_rnd(self,lst_clm, lst_rnd):
        """
        对数值列的小数位数进行规范
        :param lst_clm: 需要处理的列名列表
        :param lst_rnd: 想要实现的小数位数列表
        :return: None
        """
        lst_clm = fnc_lst_frm_all(lst_clm)
        lst_rnd = fnc_lst_frm_all(lst_rnd)
        lst_clm = fnc_lst_cpy_cll(lst_clm, fnc_flt_for_lst_len([lst_clm,lst_rnd]))
        lst_rnd = fnc_lst_cpy_cll(lst_rnd, fnc_flt_for_lst_len([lst_clm,lst_rnd]))
        for i in range(len(lst_clm)):
            self.dts[lst_clm[i]] = self.dts.apply(lambda x: round(x[lst_clm[i]],lst_rnd[i]),axis=1)
        self.init_rst()

class PrcDtfDcm(PrcDtfClm, PrcDts):
    """
    processing on documents in dataframe
    """
    def __init__(self, dts_mpt=None, bln_rst_ndx=False):
        """initiate
        define self.dts, self.len, self.typ
        :param dts_mpt: var name of target dataset
        :return: None
        """
        super(PrcDtfDcm, self).__init__(dts_mpt, bln_rst_ndx)

    def srt_dcm(self, lst_clm, bln_ascending=True, bln_rst_ndx=True):
        """sort document by certain columns(pd.sort_values), then reindex the documents by ltr_dcm_rndx
        :param lst_clm: a list of columns for sorting documents
        :param bln_ascending: descending or ascending, default True, ascending
        :param bln_rst_ndx: make sure if the index of dataframe needs reindex or not, default True
        :return: None
        """
        self.dts = self.dts.sort_values(by=lst_clm, ascending=bln_ascending, na_position='last')
        self.init_rst(bln_rst_ndx)

    def drp_dcm(self, lst_ndx, bln_rst_ndx=True):
        """drop documents by index
        :param lst_ndx: index's name in a list which will be removed
        :param bln_rst_ndx: reindex the result or not, default True
        :return: None
        """
        if lst_ndx is None:
            pass
        else:
            lst_ndx = fnc_lst_frm_all(lst_ndx)
            self.dts.drop(lst_ndx, axis=0, inplace=True)
            self.init_rst(bln_rst_ndx)

    def drp_dcm_dpl(self, lst_clm_dpl=None, lst_clm_srt=None, bln_ascending=True, bln_rst_ndx=True, bln_print=True):
        """drop documents duplicated
        :param lst_clm_dpl: columns to check duplicates
        :param lst_clm_srt: columns to sort the whole dataframe
        :param bln_ascending: ascending or descending, default True, ascending
        :param bln_rst_ndx: reset index or not, default True
        :param bln_print: print a short message for this proc or not, default True
        :return: None
        """
        len_bfr = self.len
        if lst_clm_srt is not None:
            self.srt_dcm(lst_clm_srt, bln_ascending, bln_rst_ndx)
        self.dts.drop_duplicates(subset=lst_clm_dpl, inplace=True)
        self.init_rst(bln_rst_ndx)
        if bln_print:
            print("info: drop duplicate documents, %.i in total %.i left." % (len_bfr, self.len))

    def drp_dcm_na(self, lst_clm_subset=None, bln_rst_ndx=True):
        """drop documents with na cells
        :param lst_clm_subset: a list of column names for limit the scape of na searching, default None
        :param bln_rst_ndx: reset index or not, default True
        :return: None
        """
        lst_clm_subset = fnc_lst_frm_all(lst_clm_subset) if lst_clm_subset is not None else None
        self.dts.dropna(axis=0,  # 0 for index and 1 for columns
                        how='any',  # any and all
                        # thresh=2,    # optional Keep only the rows with at least 2 non-NA values
                        subset=lst_clm_subset,  # optional columns in target
                        inplace=True)  # optional inplace dtf_input
        self.init_rst(bln_rst_ndx)

    def drp_dcm_for_ctt(self, lst_clm, lst_clm_ctt, bln_drp=True, bln_rst_ndx=True):
        """alter document, select the documents satisfied with some requirements
        :param lst_clm: a list of columns needed to be satisfied with requirements below
        :param lst_clm_ctt: a nd list of requirements for the list of columns one by one
        :param bln_drp: if select out or drop lst_clm_cct
        :param bln_rst_ndx: reset index, default True
        :return: None
        """
        lst_clm = fnc_lst_frm_all(lst_clm)
        lst_clm_ctt = fnc_lst_frm_all(lst_clm_ctt)
        lst_clm = fnc_lst_cpy_cll(lst_clm, max(len(lst_clm), len(lst_clm_ctt)))
        lst_clm_ctt = fnc_lst_cpy_cll(lst_clm_ctt, max(len(lst_clm), len(lst_clm_ctt)))
        for i in range(len(lst_clm)):
            if bln_drp is False:
                self.dts = self.dts.loc[self.dts[lst_clm[i]].isin(lst_clm_ctt), :]
            elif bln_drp is True:
                lst_clm_ctt[i] = fnc_lst_frm_all(lst_clm_ctt[i])
                self.dts = self.dts.loc[~self.dts[lst_clm[i]].isin(lst_clm_ctt[i]), :]
        self.init_rst(bln_rst_ndx)

    def drp_dcm_for_ctt_rgx(self, lst_clm, lst_rgx, bln_drp=True, bln_rst_ndx=True):
        """drop documents for contents regex
        :param lst_clm: target columns
        :param lst_rgx: a list of regex to be found in target columns
        :param bln_drp: drop or remain, default drop, False means remain matched ones and drop the others
        :param bln_rst_ndx: reset index or not, default True
        :return: None
        """
        lst_clm = fnc_lst_frm_all(lst_clm)
        lst_rgx = fnc_lst_frm_all(lst_rgx)
        lst_clm = fnc_lst_cpy_cll(lst_clm, max(len(lst_clm), len(lst_rgx)))
        lst_rgx = fnc_lst_cpy_cll(lst_rgx, max(len(lst_clm), len(lst_rgx)))
        for i in range(len(lst_rgx)):
            self.dts.index = self.dts.apply(lambda x: 1 if search(lst_rgx[i], x[lst_clm[i]]) else 0, axis=1)
            if bln_drp:
                try:
                    self.dts.drop([1], axis=0, inplace=True)  # 找到了, bln_drp is True, drop
                except (ValueError, KeyError):                  # 当上一步未能赋予相应的index的时候未ValueError
                    pass
            else:
                try:
                    self.dts.drop([0], axis=0, inplace=True)  # 没找到, bln_drp is False, drop
                except ValueError:                            # 当上一步未能赋予相应的index的时候未ValueError
                    pass
            self.init_rst(bln_rst_ndx)

    def add_dcm_frm_clm(self, lst_clm_mpt, lst_clm_grp, str_clm_xpt='_vls', str_clm_cls='_cty', bln_rst_ndx=True):
        """add documents from columns in the same categories
        group by lst_clm_grp，lst_clm_mpt中的同性质列转行，实现缩列扩行，新增str_clm_cls作为列来源的标志
        :param lst_clm_mpt: 同性质的将要换位同一列的多列名组成的列表
        :param lst_clm_grp: 用于汇总分组的列，即 grouping by and aggregating
        :param str_clm_xpt: 缩列后用于存放多列数据值的唯一列名，default _vls
        :param str_clm_cls: 缩列后用于存放标志多列来源的分类列，列中内容为lst_clm_mpt，default _cty
        :param bln_rst_ndx: reset index, default True
        :return: None
        """
        lst_dtf = []
        for i_clm in fnc_lst_frm_all(lst_clm_mpt):
            if i_clm in self.clm:
                lst_clm_grp_tmp = fnc_lst_frm_all(lst_clm_grp).copy()
                lst_clm_grp_tmp.append(i_clm)
                dtmp = DataFrame(self.dts.values, columns=self.clm, index=fnc_lst_cpy_cll(i_clm, self.len))
                dtmp.index.name = str_clm_cls  # 设定index name 用于在reset_index环节直接导出成为column name
                dtmp = dtmp[lst_clm_grp_tmp]
                dtmp.rename(columns={i_clm: str_clm_xpt}, inplace=True)
                lst_dtf.append(dtmp)  # 拼接用于for循环结束后的concat环节
        self.dts = concat(lst_dtf, axis=0)
        self.init_rst(bln_rst_ndx, False, str_clm_cls)

    def add_dcm_frm_clm_split(self, str_clm_mpt, str_clm_xpt, str_split, bln_rst_ndx=True):
        """add document like transpose in SAS, from one column splitting
        利用某个分隔符进行分列并转变为相同结构的多行
        :param str_clm_mpt: the column name which will be transposed
        :param str_clm_xpt: the new column name from str_clm_mpt
        :param str_split: a regex of
        :param bln_rst_ndx: reindex, defaul True
        :return: None
        """
        if 0 in self.clm:
            raise KeyError('a necessary column name 0 has been occupied, column 0 needs to be renamed')
        else:
            dff = self.dts[str_clm_mpt].str.split(str_split, expand=True).stack()  # 分离为series形式
            dff.index = dff.index.codes[0]  # 保证index与self.ndx相符, 旧版本使用 = dff.index.labels[0]
            self.dts = concat([self.dts, dff], axis=1)
            self.init_rst(bln_rst_ndx)
            self.drp_clm(str_clm_mpt)
            self.rnm_clm({0: str_clm_xpt})
            self.drp_dcm_for_ctt(str_clm_xpt, '', True, bln_rst_ndx)

class PrcDtfCll(PrcDtfDcm, PrcDtfClm):
    """
    processing on cells in dataframe
    """
    def __init__(self, dts_mpt=None, bln_rst_ndx=False):
        """initiate
        define self.dts, self.len, self.typ
        :param dts_mpt: var name of target dataset
        :return: None
        """
        super(PrcDtfCll, self).__init__(dts_mpt, bln_rst_ndx)

    def mrg_dtf_hrl(self, dtf_mrg, str_how, *lst_clm_on):
        """merge dataframe horizontally on certain columns by pd.merge
        :param dtf_mrg: name of the dataframe merged on the right side
        :param str_how: pandas.merge(how=str_how), in ['inner', 'outer', 'left', 'right','dpl','outer_index];
            str_how in ['dpl']: drop documents in self.dts matched in dtf_mrg
        :param lst_clm_on: the column names for fitting in self.dts and dtf_mrg, len(lst_str_clm_on) in [1,2]
        :return: None
        """
        if str_how in ['outer_index']:  # merging by indexes
            bln_rst_ndx = False  # 仅在此情形下不重置索引
            self.dts = merge(self.dts, dtf_mrg, how='outer', left_index=True, right_index=True)
        else:
            bln_rst_ndx = True  # 默认重置索引
            lst_clm_on = fnc_lst_cpy_cll(lst_clm_on, 2)  # 扩充用于匹配的字段名以满足left/right_on的需求
        if str_how in ['inner', 'outer', 'left', 'right']:  # traditional merging
            self.dts = merge(self.dts, dtf_mrg, how=str_how,
                             left_on=fnc_lst_frm_all(lst_clm_on[0]),
                             right_on=fnc_lst_frm_all(lst_clm_on[1]))
        elif str_how in ['dpl']:  # merge and drop duplicates
            dtf_mrg_drp = dtf_mrg[fnc_lst_frm_all(lst_clm_on[1])]  # 仅保留用于匹配的列，避免对self.dts的污染
            self.dts = merge(self.dts, dtf_mrg_drp, how='left',
                             left_on=fnc_lst_frm_all(lst_clm_on[0]),
                             right_on=fnc_lst_frm_all(lst_clm_on[1]),
                             indicator=True)  # 使用自动定义的列'_merge'标记来源
            self.dts = self.dts.loc[self.dts['_merge'] != 'both', :].drop(['_merge'], axis=1)
        self.init_rst(bln_rst_ndx)

    def mrg_dtf_vrl(self, lst_key_ndx=None, *dtf_mrg):
        """merge dataframe vertically by pd.concat
        :param lst_key_ndx: a key index for marking the resource of documents, len(lst_key) needs len(dtf_mrg)+1
        :param dtf_mrg: a sort of dataframes will merged on self.dts
        :return: None
        """
        bln_rst_ndx = True if lst_key_ndx is None else False  # lst_key有值则代表不忽略索引
        lst_dtf_mrg = [self.dts]
        lst_dtf_mrg.extend(fnc_lst_frm_all(dtf_mrg))  # 构成一组数据框的嵌套列表
#        self.dts = concat(lst_dtf_mrg, ignore_index=bln_rst_ndx, keys=lst_key_ndx, axis=0, sort=False)
        self.dts = concat(lst_dtf_mrg, ignore_index=bln_rst_ndx, keys=lst_key_ndx, axis=0)
        self.init_rst(bln_rst_ndx)

    def ltr_clm_ncd_mbl_cvr(self, str_clm, lst_cvr=None):
        """
        alter column to encode moblie str to 136****1234
        :param str_clm:
        :param lst_cvr: in ['head','middle','tail'], default 'middle'
        :return: None
        """
        lst_cvr = fnc_lst_frm_all(lst_cvr) if lst_cvr is not None else [3,4]
        for i in range(self.len):
            if type(self.dts.loc[i,str_clm]) is str:
                tgt = self.dts.loc[i,str_clm]
                len_cvr = len(tgt) - lst_cvr[0] - lst_cvr[1] if len(tgt)>(lst_cvr[0] - lst_cvr[1]) else 4
                self.dts.loc[i,str_clm] = tgt[:lst_cvr[0]]+''.join(['*' for k in range(len_cvr)])
                if len(lst_cvr)>1 and lst_cvr[1]!=0:
                    self.dts.loc[i,str_clm] += tgt[-lst_cvr[1]:]
            else: pass
        self.init_rst()

    def ltr_clm_ncd_mbl_str(self, lst_clm):
        """
        alter column to encode moblie str
        :param lst_clm:
        :return: None
        """
        lst_clm = fnc_lst_frm_all(lst_clm)
        for i_clm in lst_clm:
            self.dts[i_clm] = self.dts.apply(lambda x: fnc_ncd_mbl_str(x[i_clm]), axis=1)
            self.init_rst()

    def ltr_clm_dcd_mbl_str(self, lst_clm):
        """
        alter column to decode moblie str
        :param lst_clm:
        :return: None
        """
        lst_clm = fnc_lst_frm_all(lst_clm)
        for i_clm in lst_clm:
            self.dts[i_clm] = self.dts.apply(lambda x: fnc_dcd_mbl_str(x[i_clm]), axis=1)
            self.init_rst()

    def ltr_clm_ncd_str_hsh(self, lst_clm, lst_clm_add=None):
        """
        alter column to decode moblie str
        :param lst_clm: the target columns for hashing
        :param lst_clm_add: create a new column for hashed lst_clm
        :return: None
        """
        lst_clm = fnc_lst_frm_all(lst_clm)
        lst_clm_add = fnc_lst_frm_all(lst_clm_add) if lst_clm_add is not None else None
        for i in range(len(lst_clm)):
            if lst_clm_add is not None and len(lst_clm_add) == len(lst_clm):
                self.dts[lst_clm_add[i]] = self.dts.apply(lambda x: fnc_ncd_str_hsh(x[lst_clm[i]]), axis=1)
            else:
                self.dts[lst_clm[i]] = self.dts.apply(lambda x: fnc_ncd_str_hsh(x[lst_clm[i]]), axis=1)
            self.init_rst()

    def ppc_stt_tsp(self, lst_grb, dct_grb, dct_kep=None, dct_drp=None, bln_T=True):
        """
        pre-processing on statistic and transposition
        :param lst_grb: a list for grouping by, and the last cell will transpose in columns if bln_T is True
        :param dct_grb: the method of grouping by, in format {'targetColumn':[grbMethod, 'newColumnName']}
        :param dct_kep: keep documents
        :param dct_drp: drop documents
        :param bln_T: if transposing or not
        :return: None
        """
        if dct_kep is not None:
            for i_key in list(dct_kep.keys()):
                self.drp_dcm_for_ctt(i_key, dct_kep[i_key], False)
        if dct_drp is not None:
            for i_key in list(dct_drp.keys()):
                self.drp_dcm_for_ctt(i_key, dct_drp[i_key], True)
        self.add_clm_frm_stt(lst_grb,list(dct_grb.keys())[0],list(dct_grb.values())[0][0],list(dct_grb.values())[0][1])
        if bln_T:
            lst_tsp = lst_grb.copy()
            lst_tsp.remove(lst_grb[-1])
            self.srt_dcm(lst_tsp+[list(dct_grb.values())[0][1]],False)
            self.add_clm_frm_dcm(lst_grb[-1], lst_tsp+[list(dct_grb.values())[0][1]], lst_tsp)
            self.srt_dcm(lst_tsp)

    def T(self):
        """
        transpose: column to row one, row one to column
        :return: None
        """
        str_clm_tgt = self.clm[0]
        dtmp = self.dts.T
        dtmp.columns = list(dtmp.iloc[0, :])
        dtmp = dtmp.iloc[1:, :]
        dtmp[str_clm_tgt] = dtmp.index
        self.init_dts(dtmp)
        self.init_rst()
        lst_clm_srt = list(self.clm)
        lst_clm_srt.remove(str_clm_tgt)
        lst_clm_srt = [str_clm_tgt] + lst_clm_srt
        self.srt_clm(lst_clm_srt, False)

class PrcDtfTms(PrcDtfCll):
    """
    processing on timestamp columns in dataframe
    """
    def __init__(self, dts_mpt=None, bln_rst_ndx=False):
        """initiate
        define self.dts, self.len, self.typ
        :param dts_mpt: var name of target dataset
        :return: None
        """
        super(PrcDtfTms, self).__init__(dts_mpt, bln_rst_ndx)

    def add_clm_typ_to_tms(self, lst_clm, lst_clm_tms=None, lst_clm_typ=None, lst_clm_fmt=None):
        """add a column in type timestamp from another column in other dtypes
        :param lst_clm: original columns' names
        :param lst_clm_tms: new columns' names in type timestamp
        :param lst_clm_typ: original columns' types
        :param lst_clm_fmt: if original columns' types are in str, make sure the formats of them
        :return: None
        """
        lst_clm = fnc_lst_frm_all(lst_clm)
        lst_clm_tms = lst_clm.copy() if lst_clm_tms is None else lst_clm_tms  # 对params进行预处理
        lst_clm_tms = fnc_lst_frm_all(lst_clm_tms)
        lst_clm_typ = fnc_lst_cpy_cll(lst_clm_typ, len(lst_clm))
        lst_clm_fmt = fnc_lst_cpy_cll(lst_clm_fmt, len(lst_clm))
        dct_dtt = fnc_dct_frm_lst(list(dtt_fmt_rgx.values()), list(dtt_fmt_rgx.keys()))
        for i in range(len(lst_clm)):
            try:
                rgx_tmp = fnc_rgx_frm_cll(self.dts.loc[0, lst_clm[i]], list(dtt_fmt_rgx.values()))  # 识别列内容的正则表达式类型
            except TypeError:
                rgx_tmp = False
            if self.dts.dtypes[lst_clm[i]] in ['<M8[ns]'] or lst_clm_typ[i] in ['tms']:  # 略过已经满足类型要求的列
                self.dts[lst_clm_tms[i]] = self.dts.apply(lambda x: x[lst_clm[i]], axis=1)
            elif (self.dts.dtypes[lst_clm[i]] in ['O'] and rgx_tmp) or lst_clm_typ[i] in ['str']:
                lst_clm_fmt[i] = dct_dtt[rgx_tmp] if lst_clm_fmt[i] is None else lst_clm_fmt[i]  # 用正则匹配时间format
                rgx_tmp = '' if rgx_tmp is False or rgx_tmp is None else rgx_tmp
                self.dts[lst_clm_tms[i]] = self.dts.apply(
                    lambda x: pd_Timestamp.strptime(x[lst_clm[i]], lst_clm_fmt[i]) if
                    search(rgx_tmp, (x[lst_clm[i]] if type(x[lst_clm[i]]) is str else '')) else None, axis=1)
            elif self.dts.dtypes[lst_clm[i]] in ['O'] and lst_clm_typ[i] in ['date','day','dt_date']:
                self.dts[lst_clm_tms[i]] = self.dts.apply(
                    lambda x: dt_datetime.combine(x[lst_clm[i]], typ_dt_time(0, 0, 0)), axis=1)
            elif self.dts.dtypes[lst_clm[i]] in ['int64', 'int32', 'float64', 'float32'] or \
                    lst_clm_typ[i] in ['int', 'flt', 'float']:
                self.dts[lst_clm_tms[i]] = self.dts.apply(
                    lambda x: dt_datetime.fromtimestamp(float(x[lst_clm[i]])) if
                    (search('^[0-9]+[.]?[0-9]*?', str(x[lst_clm[i]])) is not None) &
                    ((float(x[lst_clm[i]]) if x[lst_clm[i]] is not None else 84000) > 84000) else
                    None, axis=1)
            elif self.dts.dtypes[lst_clm[i]] in ['O'] and lst_clm_typ[i] in ['timtuple', 'struct_time']:
                self.dts[lst_clm_tms[i]] = self.dts.apply(
                    lambda x: pd_Timestamp.strptime(strftime('%Y-%m-%d %H:%M:%S', x[lst_clm[i]]), '%Y-%m-%d %H:%M:%S'),
                    axis=1)
        self.init_rst()

    def add_clm_tms_to_typ(self, lst_clm, lst_clm_dtt=None, lst_typ=None, lst_clm_fmt=None):
        """alter column type from timestamp to other types on datetime
        :param lst_clm: a list of columns in type timestamp
        :param lst_clm_dtt: new columns' name after alteration
        :param lst_typ: new columns' type after alteration, default 'datetime'
        :param lst_clm_fmt: if new columns' type is str, a str format is needed, default None
        :return: None
        """
        lst_typ_tms = [typ_pd_Timestamp, dt_datetime]  # 支持不同变化的数据类型定义
        lst_typ_tmd, lst_typ_tmt = lst_typ_tms + [typ_dt_date], lst_typ_tms + [typ_dt_time]
        lst_clm = fnc_lst_frm_all(lst_clm)
        lst_clm_dtt = lst_clm.copy() if lst_clm_dtt is None else fnc_lst_frm_all(lst_clm_dtt)  # 对params进行预处理
        lst_typ = fnc_lst_frm_all('datetime') if lst_typ is None else fnc_lst_frm_all(lst_typ)
        lst_clm_fmt = fnc_lst_frm_all(lst_clm_fmt)
        flt_len_max = fnc_flt_for_lst_len([lst_clm, lst_clm_dtt, lst_typ, lst_clm_fmt])
        lst_clm = fnc_lst_cpy_cll(lst_clm, flt_len_max)
        lst_clm_dtt = fnc_lst_cpy_cll(lst_clm_dtt, flt_len_max)
        lst_typ = fnc_lst_cpy_cll(lst_typ, flt_len_max)
        lst_clm_fmt = fnc_lst_cpy_cll(lst_clm_fmt, flt_len_max)
        for i in range(len(lst_clm)):
            if lst_typ[i] is 'datetime':
                self.dts[lst_clm_dtt[i]] = self.dts.apply(
                    lambda x: x[lst_clm[i]] if type(x[lst_clm[i]]) in lst_typ_tms else None, axis=1)
            elif lst_typ[i] is 'date':
                self.dts[lst_clm_dtt[i]] = self.dts.apply(
                    lambda x: x[lst_clm[i]].date() if type(x[lst_clm[i]]) in lst_typ_tms else None, axis=1)
            elif lst_typ[i] is 'time':
                self.dts[lst_clm_dtt[i]] = self.dts.apply(
                    lambda x: x[lst_clm[i]].time() if type(x[lst_clm[i]]) in lst_typ_tms else None, axis=1)
            elif lst_typ[i] is 'hour':
                self.dts[lst_clm_dtt[i]] = self.dts.apply(
                    lambda x: x[lst_clm[i]].hour if type(x[lst_clm[i]]) in lst_typ_tmt else None, axis=1)
            elif lst_typ[i] is 'week':
                self.dts[lst_clm_dtt[i]] = self.dts.apply(
                    lambda x: x[lst_clm[i]].isocalendar()[1] if type(x[lst_clm[i]]) in lst_typ_tmd else None, axis=1)
            elif lst_typ[i] is 'weekday':
                self.dts[lst_clm_dtt[i]] = self.dts.apply(
                    lambda x: x[lst_clm[i]].isocalendar()[2] if type(x[lst_clm[i]]) in lst_typ_tmd else None, axis=1)
            elif lst_typ[i] is 'day':
                self.dts[lst_clm_dtt[i]] = self.dts.apply(
                    lambda x: x[lst_clm[i]].day if type(x[lst_clm[i]]) in lst_typ_tmd else None, axis=1)
            elif lst_typ[i] is 'month':
                self.dts[lst_clm_dtt[i]] = self.dts.apply(
                    lambda x: x[lst_clm[i]].month if type(x[lst_clm[i]]) in lst_typ_tmd else None, axis=1)
            elif lst_typ[i] is 'year':
                self.dts[lst_clm_dtt[i]] = self.dts.apply(
                    lambda x: x[lst_clm[i]].year if type(x[lst_clm[i]]) in lst_typ_tmd else None, axis=1)
            elif lst_typ[i] is 'timetuple':
                self.dts[lst_clm_dtt[i]] = self.dts.apply(
                    lambda x: x[lst_clm[i]].timetuple() if type(x[lst_clm[i]]) in lst_typ_tms else None, axis=1)
            elif lst_typ[i] is 'mday':
                self.dts[lst_clm_dtt[i]] = self.dts.apply(
                    lambda x: x[lst_clm[i]].timetuple().tm_mday if type(x[lst_clm[i]]) in lst_typ_tms else None, axis=1)
            elif lst_typ[i] is 'yday':
                self.dts[lst_clm_dtt[i]] = self.dts.apply(
                    lambda x: x[lst_clm[i]].timetuple().tm_yday if type(x[lst_clm[i]]) in lst_typ_tms else None, axis=1)
            elif lst_typ[i] is 'str':
                self.dts[lst_clm_dtt[i]] = self.dts.apply(
                    lambda x: pd_Timestamp.strftime(x[lst_clm[i]], lst_clm_fmt[i]) if
                    type(x[lst_clm[i]]) in lst_typ_tmd else None, axis=1)
            elif lst_typ[i].lower() in ['flt', 'float','int']:
                self.dts[lst_clm_dtt[i]] = self.dts.apply(
                    lambda x: mktime(x[lst_clm[i]].timetuple()) if
                    type(x[lst_clm[i]]) in lst_typ_tmd else None, axis=1)
            else:
                raise KeyError('lst_typ is not available')
        self.init_rst()

    def add_clm_typ_to_dtt(self, lst_clm, lst_clm_tms=None, lst_clm_typ=None, lst_clm_fmt=None):
        """add column from any type to datetime
        :param lst_clm: a list of columns' name for alteration
        :param lst_clm_tms: a list of new columns' name after alteration
        :param lst_clm_typ: a list of types in format [None, 'datetime']
        :param lst_clm_fmt: a list of format, default [None, None]
        :return: None
        """
        if lst_clm_typ is None:
            lst_clm_typ = [None, 'datetime']
        if lst_clm_fmt is None:
            lst_clm_fmt = [None, None]
        self.add_clm_typ_to_tms(lst_clm, lst_clm_tms, lst_clm_typ[0], lst_clm_fmt[0])  # 将不是时间变量的转化成日期时间变量
        lst_clm_tms = lst_clm if lst_clm_tms is None else lst_clm_tms  # 对params进行预处理
        lst_clm_tms, lst_clm_fmt[1] = fnc_lst_frm_all(lst_clm_tms), fnc_lst_frm_all(lst_clm_fmt[1])
        self.add_clm_tms_to_typ(lst_clm_tms, None, lst_clm_typ[1], lst_clm_fmt[1])

    def drp_dcm_by_tms(self, str_clm_tms, str_typ='>', cll_tms=None, flt_bias=None):
        """
        drop documents by a timestamp column's values
        :param str_clm_tms: a column's name which is essentially timestamp
        :param str_typ: drop documents not in this area, in ['>','>=','<','<=']
        :param cll_tms: a certain time point to be compared
        :param flt_bias: documents str_typ(>) cll_tmp + flt_bias
        :return: None
        """
        if cll_tms is None:
            cll_tms = dt_datetime.now().date()
        if flt_bias is None:
            flt_bias = 0
        self.add_clm_typ_to_tms(str_clm_tms, '_tms')
        if str_typ not in ['>', '<', '>=', '<=']:
            raise KeyError('str_typ needs [>, <, >=, <=]')
        elif str_typ == '>':
            self.dts = self.dts.loc[self.dts['_tms'] > cll_tms + typ_dt_dlt(flt_bias), :]
            self.drp_clm('_tms')
        elif str_typ == '<':
            self.dts = self.dts.loc[self.dts['_tms'] < cll_tms + typ_dt_dlt(flt_bias), :]
            self.drp_clm('_tms')
        elif str_typ == '>=':
            self.dts = self.dts.loc[self.dts['_tms'] >= cll_tms + typ_dt_dlt(flt_bias), :]
            self.drp_clm('_tms')
        else:
            self.dts = self.dts.loc[self.dts['_tms'] <= cll_tms + typ_dt_dlt(flt_bias), :]
            self.drp_clm('_tms')
    # drop documents if period gap is smaller than flt_dff
    def drp_dcm_for_dtt_dff(self, str_clm, flt_dff=1):
        """
        drop documents which datetime columns' diff is
        :param str_clm: 时间变量名
        :param flt_dff: 间隔时长大于该值则保留输出
        :return: None
        """
        self.add_clm_typ_to_dtt(str_clm, '_str_clm', [None, 'datetime'])
        self.srt_dcm('_str_clm')
        for i in range(self.len - 1):
            self.dts.loc[i, 'flag'] = self.dts.loc[i, '_str_clm'] - self.dts.loc[i + 1, '_str_clm']
        self.dts = self.dts.loc[self.dts.flag < typ_dt_dlt(days=-flt_dff), :]
        self.drp_clm('_str_clm')
    # add datetime column from mongodb _id
    def add_clm_oid_to_dtt(self, str_clm_dtt, str_format='%Y-%m-%d'):
        """add column from mongodb's _id in type objectID to datetype in type str
        knowledge from http://www.sharejs.com/codes/python/9028
        :param str_clm_dtt: the name of new columns for the date message from _id in mongodb
        :param str_format: the final format of date message in str_clm_dtt
        :return: None
        """
        if '_id' in self.clm:
            from bson.objectid import ObjectId as typ_ObjectId  # 用于识别mongodb中'_id'的dtype
            if str_clm_dtt in self.clm:
                self.dts[str_clm_dtt] = self.dts.apply(
                    lambda x: strftime(str_format, x['_id'].generation_time.timetuple()) if
                    type(x['_id']) is typ_ObjectId else x[str_clm_dtt], axis=1)  # bson.objectid.ObjectId
            else:
                self.dts[str_clm_dtt] = self.dts.apply(
                    lambda x: strftime(str_format, x['_id'].generation_time.timetuple()) if
                    type(x['_id']) is typ_ObjectId else None, axis=1)  # bson.objectid.ObjectId
        else:
            raise KeyError('_id not in self.clm')
        self.init_rst()
    # 按照某种分组规则填充时间行
    def ppc_dct_day(self, do_day_min, do_day_max, str_clm='do_date', str_typ='str',str_fmt='%Y-%m-%d',pth_day=None):
        """
        产生一列do_day，规定起止日期
        :param do_day_min: 规定的开始日期
        :param do_day_max: 规定的结束日期
        :param str_clm:
        :param str_typ:
        :param str_fmt:
        :param pth_day: 日期目录文件地址
        :return: 包含唯一列do_day的PrcDtf
        """
        pth_dct = 'C:/Users/Administrator/Programs/prg_pyzohar/doc_dct'
        pth_day = path.join(pth_dct,"DCT_DAY.xlsx") if pth_day is None else pth_day
        self.init_dts(read_excel(pth_day))
        self.init_rst()
        self.add_clm_typ_to_dtt('do_day', str_clm, [None, str_typ], [None, str_fmt])
        self.dts = self.dts.loc[(self.dts.do_day >= do_day_min) & (self.dts.do_day <= do_day_max), :]
        self.init_rst()
        self.srt_clm(str_clm)
    # # run: 填充
    def fll_dcm_by_dct_day(self, str_grb, str_tms, str_typ='str',str_fmt='%Y-%m-%d', lst_prd=None):
        """
        对某列进行分类后对类中所有可能的取值进行时间列的行补足
        :param str_grb: 用于分类的列
        :param str_tms: 需要填充的时间列
        :param str_typ:
        :param str_fmt:
        :param lst_prd: 补足的时间窗口
        :return: None
        """
        prcdtf = PrcDtfCll(self.dts)
        lst_prd = fnc_lst_cpy_cll(fnc_lst_frm_all(lst_prd),2)
        lst_prd[0] = prcdtf.dts[str_tms].min() if lst_prd[0] is None else lst_prd[0]
        lst_prd[1] = prcdtf.dts[str_tms].max() if lst_prd[1] is None else lst_prd[1]

        self.ppc_dct_day(lst_prd[0], lst_prd[1], str_tms, str_typ, str_fmt)
        for i_cll in list(prcdtf.dts[str_grb].unique()):
            dtf_cll = self.dts.copy()
            dtf_cll[str_grb] = i_cll
            dtf_cll = PrcDtf(dtf_cll)
            dtf_cll.drp_dcm_for_ctt(str_tms, list(prcdtf.dts.loc[prcdtf.dts[str_grb]==i_cll,:][str_tms].unique()))
            prcdtf.mrg_dtf_vrl(None, dtf_cll.dts)
        self.init_dts(prcdtf.dts)
        self.init_rst()
    # generate x for do_day, do_week, do_mnth, do_year
    def add_clm_x_hour(self, lst_dtt=None, vnm_x='x_hour', str_format='%Y-%m-%d'):
        """
        add column in format date-hours, especially for wechat article reading in hours
        :param lst_dtt: default ['ref_date', 'ref_hour']
        :param vnm_x: name of the new column
        :param str_format: default '%Y-%m-%d'
        :return: None
        """
        warn('deprecated')
        lst_dtt = ['ref_date', 'ref_hour'] if lst_dtt is None else lst_dtt
        self.add_clm_typ_to_dtt(lst_dtt[0], lst_dtt[1], [None, 'str'], [None, str_format])
        self.ltr_clm_typ([lst_dtt[1]], ['str'])
        for i in range(self.len):
            if len(self.dts.loc[i, lst_dtt[1]]) == 1:
                self.dts.loc[i, lst_dtt[1]] = '000' + self.dts.loc[i, lst_dtt[1]]
            elif len(self.dts.loc[i, lst_dtt[1]]) == 2:
                self.dts.loc[i, lst_dtt[1]] = '00' + self.dts.loc[i, lst_dtt[1]]
            elif len(self.dts.loc[i, lst_dtt[1]]) == 3:
                self.dts.loc[i, lst_dtt[1]] = '0' + self.dts.loc[i, lst_dtt[1]]
        self.dts[vnm_x] = self.dts.apply(lambda x: x[lst_dtt[0]][2:]+"_"+x[lst_dtt[1]][:2], axis=1)
        self.init_rst()

    def add_clm_x_date(self, vnm_dtt='do_day', vnm_x='x_date', str_format='%Y-%m-%d'):
        """
        :param vnm_dtt:
        :param vnm_x:
        :param str_format:
        :return:
        """
        self.add_clm_typ_to_dtt(vnm_dtt, vnm_x, [None, 'str'], [None, str_format])
        self.dts[vnm_x] = self.dts.apply(lambda x: x[vnm_x][2:], axis=1)
        self.init_rst()

    def add_clm_x_week(self, vnm_dtt='do_day', lst_dtt=None, vnm_x='x_week'):
        """
        :param vnm_dtt:
        :param lst_dtt:
        :param vnm_x:
        :return:
        """
        lst_dtt = ['do_year', 'do_mnth', 'do_week'] if lst_dtt is None else lst_dtt
        # 对年初年末的截断周进行所属年份的调整
        self.dts['week_year'] = self.dts.apply(lambda x:
                                               x[lst_dtt[0]] - 1 if
                                               (x[vnm_dtt].timetuple().tm_yday < 10) & (x[lst_dtt[2]] > 50)
                                               else x[lst_dtt[0]], axis=1)
        self.dts['week_year'] = self.dts.apply(lambda x:
                                               x[lst_dtt[0]] + 1 if
                                               (x[vnm_dtt].timetuple().tm_yday > 355) & (x[lst_dtt[2]] < 2)
                                               else x['week_year'], axis=1)
        self.dts[vnm_x] = self.dts.apply(lambda x: str(x['week_year'])[2:] + 'w' + "%02d" % x[lst_dtt[2]], axis=1)
        self.init_rst()
        self.drp_clm(['week_year'])

    def add_clm_x_mnth(self, lst_dtt=None, vnm_x='x_mnth'):
        """add column x_mnth for echarts
        :param lst_dtt:
        :param vnm_x:
        :return:
        """
        if lst_dtt is None:
            lst_dtt = ['do_year', 'do_mnth', 'do_week']
        self.dts[vnm_x] = self.dts.apply(lambda x: str(x[lst_dtt[0]])[2:] + 'm' + "%02d" % x[lst_dtt[1]], axis=1)
        self.init_rst()

    def add_clm_x_dtt(self, str_clm_dtt, lst_clm_dtt_tmp=None):
        """add columns x_date, x_week, x_mnth
        :param str_clm_dtt: the main column of datetime in type datetime.datetime
        :param lst_clm_dtt_tmp: a list of year/mnth/week name, default None - ['do_year','do_mnth','do_week']
        :return: PrcDtf with x_date, x_week, x_mnth
        """
        lst_clm_dtt_tmp = ['do_year', 'do_mnth', 'do_week'] if lst_clm_dtt_tmp is None else lst_clm_dtt_tmp
        if str_clm_dtt in self.clm and \
                len(list((set(lst_clm_dtt_tmp).union(set(self.clm))) ^ (set(lst_clm_dtt_tmp) ^ set(self.clm)))) == 0:
            self.add_clm_typ_to_tms(str_clm_dtt)
            self.add_clm_tms_to_typ(str_clm_dtt, lst_clm_dtt_tmp, ['year', 'month', 'week'])
        else:
            raise KeyError('the datetime column does not exist, or do_week/mnth/year already exist')
        self.add_clm_x_date(str_clm_dtt)
        self.add_clm_x_week(str_clm_dtt, lst_clm_dtt_tmp)
        self.add_clm_x_mnth(lst_clm_dtt_tmp)
        self.drp_clm(lst_clm_dtt_tmp)

class PrcDtfGrp(PrcDtfCll):
    """
    Process on Graphing
    """
    def __init__(self, dts_mpt=None, bln_rst_ndx=False):
        """initiate
        define self.dts, self.len, self.typ
        :param dts_mpt: var name of target dataset
        :return: None
        """
        super(PrcDtfGrp, self).__init__(dts_mpt, bln_rst_ndx)
    # 合并规则下的分组求和，对合并消除的列进行手工赋值
    def fnc_mrg_vrl_ttl(self, lst_grb, grb_mtd=np_sum, new_clm=None, *dct_spc):
        """
        求按lst_grb的合计并纵向拼接到原数据集
        :param lst_grb: 汇总列
        :param grb_mtd: 汇总方法,默认为np.sum
        :param new_clm: 生成的新列名，默认维持不变
        :param dct_spc: 对合并后制定列的值进行手工定制,比如汇总列改名"合计"
        :return: None
        """
        lst_grb = fnc_lst_frm_all(lst_grb)
        prcttl = PrcDtfClm(self.dts)
        prcttl.add_clm_frm_stt(lst_grb, [i for i in self.clm if i not in lst_grb], grb_mtd, new_clm)
        if dct_spc is not None:
            for i in range(len(dct_spc)):
                for j in (dct_spc[i].keys()):
                    prcttl.dts[j] = dct_spc[i][j]
        prcttl.init_rst()
        self.mrg_dtf_vrl(None, prcttl.dts)
    # 多列下行转多列
    def ppc_dcm_tsp_clm(self,lst_grb,str_tsp):
        """
        多列下行转多列
        :param lst_grb: 固定不转的列名
        :param str_tsp: 为需要转置的分类变量
        :return: None
        """
        dfnl = PrcDtf()
        for i_dpt in self.dts[str_tsp].unique():
            dprt = PrcDtf(self.dts)
            dprt.drp_dcm_for_ctt(str_tsp, i_dpt, False)
            dprt.drp_clm(str_tsp)
            lst_clm_rnm = fnc_lst_frm_cll_mrg('differ', list(dprt.clm), lst_grb)
            lst_clm_new = [str(i_dpt) + '_' + str(i) for i in lst_clm_rnm]
            dprt.rnm_clm(lst_clm_rnm, lst_clm_new)
            if self.dts[str_tsp].unique()[0] == i_dpt:
                dfnl = PrcDtf(dprt.dts)
            else:
                dfnl.mrg_dtf_hrl(dprt.dts, 'outer', lst_grb)
        self.add_dts(dfnl.dts)
    # 整体转置
    def ppc_nwo_T(self):
        """
        对第一行第一列为坐标轴的dataframe进行转置
        """
        str_clm_tgt = self.clm[0]
        dtmp = self.dts.T
        dtmp.columns = list(dtmp.iloc[0, :])
        dtmp = dtmp.iloc[1:, :]
        dtmp[str_clm_tgt] = dtmp.index
        self.init_dts(dtmp)
        self.init_rst()
        self.typ_dts_to_dtf()
        lst_clm_srt = list(self.clm)
        lst_clm_srt.remove(str_clm_tgt)
        lst_clm_srt = [str_clm_tgt] + lst_clm_srt
        self.srt_clm(lst_clm_srt, False)
    # 环比变化率
    def fnc_ppc_per(self, lst_clm=None,str_tls='_pop'):
        """求环比变化率"""
        if lst_clm is None:
            lst_clm = list(self.clm).copy()
            for i_prd in ['x_week', 'x_date', 'x_day', 'x_mnth', 'do_day', 'do_date']:
                try: lst_clm.remove(i_prd)
                except ValueError: pass
        for i_clm in lst_clm:
            for i in range(1, self.len):
                if self.dts.loc[i - 1, i_clm] > 0:
                    self.dts.loc[i, i_clm + str_tls] = round(
                        100 * (self.dts.loc[i, i_clm] - self.dts.loc[i - 1, i_clm]) / self.dts.loc[i - 1, i_clm], 1)
            self.fll_clm_na({i_clm + str_tls: 0})
            self.ltr_clm_rpc([i_clm + str_tls], -100, 0)
    # 数值转百分比
    def ppc_clm_per(self,lst_grb,dct_grb=None):
        """
        alter count to percent,需要非常严格的表头格式，如self.clm=[时间列, 分类列, 值列]
        :param lst_grb: self.clm[0], 类似时间列
        :param dct_grb: self.clm[-1], 类似值列, default {'cnt':np.sum}
        :return: None
        """
        dct_grb = {'cnt':np_sum} if dct_grb is None else dct_grb
        if type(dct_grb) is dict:
            dprt = PrcDtf(self.dts)
            dprt.add_clm_frm_stt(lst_grb,dct_grb,None,'_sum')
            self.fll_clm_na({'cnt':0})
            self.mrg_dtf_hrl(dprt.dts,'left',lst_grb)
            self.dts[list(dct_grb.keys())[0]] = self.dts.apply(lambda x: round(100*x[list(dct_grb.keys())[0]]/x['_sum'],1),axis=1)
            self.init_rst()
        else: raise KeyError('stop: type of dct_grb needs dict, like {"cnt":np.sum}')
    # 移动平均综合
    def ppc_clm_rll_stt(self,lst_srt,flt_rll,dct_mtd):
        """
        pre-processing columns rolling stat
        :param lst_srt: 按汇总变量和时间变量进行排序，例如['empid','x_week]
        :param flt_rll: rolling window
        :param dct_mtd: rolling method, like {'sum':['cll','cht']}
        :param flt_rnd: 对rolling的结果进行round()规范小数位数
        :return: None
        """
        self.srt_dcm(lst_srt)
        # 对排序列表的首个元素进行标号，便于后期rolling无效行的删除
        n = 1
        for i in range(1, self.len):
            if self.dts.loc[i, lst_srt[0]] != self.dts.loc[i - 1, lst_srt[0]]: n = 0
            self.dts.loc[i, '_id'] = n
            n += 1
        for i_key in dct_mtd.keys():
            if i_key.lower() in 'mean':
                for i_clm in dct_mtd[i_key]:
                    self.dts[i_clm+str(flt_rll).zfill(2)] = self.dts[i_clm].rolling(window=flt_rll,center=False).mean()
            if i_key.lower() in 'sum':
                for i_clm in dct_mtd[i_key]:
                    self.dts[i_clm+str(flt_rll).zfill(2)] = self.dts[i_clm].rolling(window=flt_rll,center=False).sum()
            if i_key.lower() in 'std':
                for i_clm in dct_mtd[i_key]:
                    self.dts[i_clm+str(flt_rll).zfill(2)] = self.dts[i_clm].rolling(window=flt_rll,center=False).std()
        # 根据预留的_id进行无效行删除
        self.init_dts(self.dts.loc[self.dts._id>=flt_rll-1])
        self.init_rst()
        self.drp_dcm_na('_id')
        self.drp_clm('_id')
    # echarts exporting preparation
    def ppc_typ_dts_for_ech_bsc(self):
        """
        将columns=[x,legend1,legend2,...]转换为[[x,1,2],[x,3,4],...]
        :return: None
        """
        dts_ctt = self.dts.to_dict('split')['data']
        self.dts = [self.dts.to_dict('split')['columns']]
        self.dts.extend(dts_ctt)
        self.init_rst()

    def ppc_typ_dts_for_ech_bar(self,str_hdr=None):
        """
        将columns=[x,legend1,legend2,...]转换为[[x,1,2],[x,3,4],...]
        :param str_hdr: echarts表头标题
        :return: None
        """
        self.ppc_typ_dts_for_ech_bsc()
        self.dts = {'dts': self.dts}
        if str_hdr is not None:
            self.dts.update({'hdr':str_hdr})

    def ppc_typ_dts_for_ech_flw(self,str_hdr=None,x_clm='x_hour'):
        """
        将columns=[x,legend1,legend2,...]转换为[[1,_vls,legend1],[2,vls,legend1],...]
        :param x_clm: 原数据集中的横轴数据所在列
        :param str_hdr: echarts表头标题
        :return: None
        """
        lst_x = self.dts[x_clm].unique().tolist()
        self.dts[x_clm] = self.dts.index
        self.init_rst()
        self.add_dcm_frm_clm([i for i in self.clm if i not in [x_clm]], [x_clm])
        self.srt_clm([x_clm, '_vls', '_cty'])
        self.ppc_typ_dts_for_ech_bsc()
        self.dts = {'clm':lst_x,'dts':self.dts[1:]}
        if str_hdr is not None:
            self.dts.update({'hdr':str_hdr})

    def ppc_typ_dts_for_ech(self,str_typ='echarts_bar',str_hdr=None,x_clm='x_hour',):
        """
        generate self.dts format type for echarts
        将dataframe转化为[[clmA,clmB,..],[cellA1,cellB1,..],[cellA2,cellB2,..],...]
        需要起点format为columns=[x_clm,legend1,legend2,...]
        :return: None
        """
        if str_typ =='echarts_bar':
            self.ppc_typ_dts_for_ech_bar(str_hdr)
        if str_typ =='echarts_flow':
            self.ppc_typ_dts_for_ech_flw(str_hdr,x_clm)

class PrcDtf(PrcDtfTms, PrcDtfGrp, PrcDtfCll):
    """
    processing on dataframes
    """
    def __init__(self, dts_mpt=None, bln_rst_ndx=False):
        """initiate
        define self.dts, self.len, self.typ
        :param dts_mpt: var name of target dataset
        :return: None
        """
        super(PrcDtf, self).__init__(dts_mpt, bln_rst_ndx)  # https://blog.csdn.net/brucewong0516/article/details/79121179
        self.typ_dts_to_dtf()

    def add_dts(self, dts_mpt):
        """
        import data set into self.dts
        :param dts_mpt: the target data set
        :return: None
        """
        self.init_dts(dts_mpt)
        self.init_rst()
        self.typ_dts_to_dtf()

    def bch_fnc(self, fnc, *lst_prm):
        """
        批量运行PrcDtf.func
        :param fnc: self.func
        :param lst_prm: params in a list for self.func
        :return: None
        """
        for i_prm in lst_prm:
            if len(i_prm) == 0:
                fnc()
            elif len(i_prm) ==1:
                fnc(i_prm[0])
            elif len(i_prm) ==2:
                fnc(i_prm[0],i_prm[1])
            elif len(i_prm) ==3:
                fnc(i_prm[0],i_prm[1],i_prm[2])
            elif len(i_prm) ==4:
                fnc(i_prm[0],i_prm[1],i_prm[2],i_prm[3])
            elif len(i_prm) ==5:
                fnc(i_prm[0],i_prm[1],i_prm[2],i_prm[3],i_prm[4])
            elif len(i_prm) ==6:
                fnc(i_prm[0],i_prm[1],i_prm[2],i_prm[3],i_prm[4],i_prm[5])
            elif len(i_prm) ==7:
                fnc(i_prm[0],i_prm[1],i_prm[2],i_prm[3],i_prm[4],i_prm[5],i_prm[6])
            elif len(i_prm) ==8:
                fnc(i_prm[0],i_prm[1],i_prm[2],i_prm[3],i_prm[4],i_prm[5],i_prm[6],i_prm[7])
            elif len(i_prm) ==9:
                fnc(i_prm[0],i_prm[1],i_prm[2],i_prm[3],i_prm[4],i_prm[5],i_prm[6],i_prm[7],i_prm[8])
            else: raise KeyError('stop: lst_prm is too long for func batch')
        self.init_rst()
