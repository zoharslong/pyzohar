#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 22 16:30:38 2019
dataframe operation.
@author: zoharslong
"""
from numpy import nan as np_nan, max as np_max, min as np_min, sum as np_sum
from pandas import merge, concat, DataFrame, cut
from re import search as re_search, findall as re_findall, sub as re_sub, match as re_match
from pandas.core.indexes.base import Index as typ_pd_Index              # 定义dataframe.columns类型
from math import isnan as math_isnan
from bsc import stz, lsz, dtz
from ioz import ioz


class dfBsc(ioz):
    """
    dataFrame Basic.
    """
    def __init__(self, dts=None, lcn=None, *, spr=False):
        super(dfBsc, self).__init__(dts, lcn, spr=spr)


class clmMixin(dfBsc):
    """
    methods Mixin for columns.
    """
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

    def drp_clm(self, lst_clm, *, spr=False, rtn=False, prn=True):
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
        :param prn: print info or not
        :return: if rtn is True, return self.dts
        """
        lst_clm = lsz(lst_clm)
        lst_clm.typ_to_lst(rtn=True)
        lst_clm = lst_clm.mrg('inter', list(self.clm), rtn=True)
        self.dts = self.dts.drop(lst_clm, axis=1)
        if prn:
            print('info: found columns %s in target dataFrame, dropping.' % str(lst_clm))
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
        >>> tst = clmMixin(DataFrame([{'A':np_nan,'B':1}]),spr=True)
        >>> tst.fll_clm_na({'A':1}, rtn=True)
             A  B
        0  1.0  1
        >>> tst = dfz(DataFrame([{'A':1,'B':1},{'A':np_nan,'B':1}]),spr=True)
        >>> tst.fll_clm_na(prm='ffill')
             A  B
        0  1.0  1
        1  1.0  1
        :param args: columns and values to be filled. 填入两个列表或一个字典，表达对列中空缺值的填充关系
        :param spr: let self = self.dts
        :param rtn: default False, return None
        :param prm: method of fill na, in ['backfill', 'bfill', 'pad', 'ffill', None]
        :return: if rtn is True, return self.dts
        """
        if len(args) == 0:
            dct_fll = None
        elif len(args) == 1:
            dct_fll = args[0]
        else:
            dct_fll = lsz(args).rgs_to_typ(prm='dct', rtn=True)
        self.dts = self.dts.fillna(value=dct_fll, method=prm)
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
        dct_stt = lsz(args).rgs_to_typ(prm='dct', rtn=True)
        lst_stt = list(dct_stt.keys())
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
        dct_rgs = lsz(args).rgs_to_typ(prm='dct', rtn=True)
        lst_clm, lst_new = list(dct_rgs.keys()), list(dct_rgs.values())
        prm = lsz(prm)
        prm.typ_to_lst()
        prm.cpy_tal(len(lst_clm), spr=True)
        for i in range(len(lst_clm)):
            self.dts[lst_new[i]] = [re_findall(prm[i], j)[0] if type(j) in [str] and re_search(prm[i], j) else None for
                                    j in self.dts[lst_clm[i]]]
        self.dts_nit()
        if spr:
            self.spr_nit()
        if rtn:
            return self.dts

    def add_clm_dtf(self, str_clm, *, spr=False, rtn=False):
        """
        若dataframe中某列中为形如[{},{},..]的可转化为dataframe的结构，则解出拼接至原dataframe，导致原dataframe的行列均扩增
        :param str_clm: 列单元格内容为[{},{},..]格式的列名
        :param spr: let self = self.dts
        :param rtn: default False, return None
        :return: None
        """
        dtf_dtl = DataFrame([])
        for i in range(self.len):
            len_tmp = len(self.dts[str_clm][i]) if type(self.dts[str_clm][i]) is list else 1  # 兼容{}, [{},{}]两种输入
            dtf_i = DataFrame(self.dts[str_clm][i], index=[int(i)]*len_tmp)
            dtf_dtl = concat([dtf_dtl, dtf_i])  # default prm axis=0, sort=False
        self.dts.drop([str_clm], axis=1, inplace=True)
        self.dts = concat([dtf_dtl, self.dts], axis=1)  # default prm join='outer', sort=False
        if spr:
            self.spr_nit()
        if rtn:
            return self.dts

    def add_clm_spl(self, clm_spl, clm_vls, clm_rmn=None, *, spr=False, rtn=False):
        """
        add columns from spliting an column.
        >>> self = dfz([{'est':'A','date':'1','vls':1},{'est':'B','date':'1','vls':2},
        >>>             {'est':'A','date':'2','vls':1},{'est':'B','date':'3','vls':2},])
        >>> self.typ_to_dtf()
        >>> self.add_clm_spl('est','vls', rtn=True)  # 按照est列，将vls列的值扩张成多个新的列
          date  vls_A  vls_B
        0    1    1.0    2.0
        1    3    1.0    NaN
        2    2    NaN    2.0
        :param clm_spl: 用于分出多列的列，列中的每一种唯一元素都会成为新的列
        :param clm_vls: 用于分到多列的值
        :param clm_rmn: 保留到新表中的用作拼接的列
        :param spr: let self = self.dts
        :param rtn: default False, return None
        :return: if rtn, return self.dts
        """
        dtf_fnl = DataFrame([])
        clm_spl_cll = self.dts[clm_spl].unique()
        # 对clm_rmn进行预处理
        if clm_rmn is None:
            clm_ttl = [i for i in lsz(self.clm.copy()).typ_to_lst(rtn=True) if i not in [clm_spl]]
            clm_rmn = [i for i in clm_ttl if i not in [clm_vls, clm_spl]]
        else:
            clm_rmn = [i for i in lsz(clm_rmn).typ_to_lst(rtn=True) if i not in [clm_vls, clm_spl]]
            clm_ttl = lsz(clm_rmn + [clm_vls]).typ_to_lst(rtn=True)
        # 如果存在标志列重复的行，则终止本次列转行操作，因为会在拼接过程中导致虚增重复的行数
        len_bfr = self.len
        chk_dpl = self.dts.drop_duplicates(subset=[clm_spl] + clm_rmn, keep='first')
        if len_bfr != chk_dpl.shape[0]:
            raise AttributeError("stop: duplicate documents exist, %.i / %.i." % (chk_dpl.shape[0], len_bfr))
        # clm_spl中每个唯一的cell进行一次clm_vls列的值转新列的操作
        for i in range(clm_spl_cll.shape[0]):
            dtf_tmp = self.dts.loc[self.dts[clm_spl] == clm_spl_cll[i], :][clm_ttl]
            dtf_tmp.rename(columns={clm_vls: clm_vls+'_'+clm_spl_cll[i]}, inplace=True)
            dtf_fnl = dtf_tmp.copy() if dtf_fnl.shape[0] == 0 else merge(dtf_fnl, dtf_tmp, how='outer', on=clm_rmn)
        self.dts = dtf_fnl
        if spr:
            self.spr_nit()
        if rtn:
            return self.dts

    def add_clm_ctg(self, *args, prm='num', ctg_num=2, ctg_edg=None, ctg_lbl=None, bln_lwr=False, spr=False, rtn=False):
        """
        add new columns of certain columns in category.
        >>> tst = dfz([{'A':1},{'A':2},{'A':3},{'A':4},{'A':5},])
        >>> tst.typ_to_dtf()
        >>> tst.add_clm_ctg('A',ctg_lbl=['a','b','c','d','e'],bln_lwr=True)
           A
        0  a
        1  a
        2  a
        3  b
        4  b
        :param args:
        :param prm: in ['number', 'num', 'width']
        :param ctg_num: the number of boxes if prm in ['num', 'number'], the width of each box if prm in ['width']
        :param ctg_edg: the min and max limitation of th category
        :param ctg_lbl: label name of each boxes
        :param bln_lwr: method in (a, b] or [a, b], default (,]
        :param spr: let self = self.dts
        :param rtn: default False, return None
        :return: if rtn, return self.dts
        """
        lst_rgs = lsz(args).rgs_to_typ(rtn=True)
        str_mpt, str_xpt = lst_rgs[0], lst_rgs[1]
        flt_min = np_min(self.dts[str_mpt])     # 自动产生分箱的上下界
        flt_max = np_max(self.dts[str_mpt])
        if ctg_edg is not None and ctg_edg[0] <= flt_min and ctg_edg[1] >= flt_max:
            flt_min = ctg_edg[0]
            flt_max = ctg_edg[1]
        elif ctg_edg is None:
            pass
        else:
            raise AttributeError('stop: edge of the category is not available.')
        # 构建分箱规则列表
        lst_bns = [flt_min]
        flt_cts = (flt_max - flt_min) / ctg_num
        if prm is 'num':        # option: 确定分箱数量
            lst_bns = [flt_min] + [flt_min+(i+1)*flt_cts for i in range(int(ctg_num))]
        elif prm is 'width':    # option: 确定分箱宽度
            from math import ceil
            lst_bns = [flt_min] + [flt_min+(i+1)*ctg_num for i in range(ceil(flt_cts))]
        # 基础分箱
        ctg_lbl = ctg_lbl[:len(lst_bns) - 1] if ctg_lbl is not None else ctg_lbl
        self.dts[str_xpt] = cut(self.dts[str_mpt], lst_bns, labels=ctg_lbl, include_lowest=bln_lwr)
        self.dts_nit()
        print('info: transform column <%s> in category %s.' % (str_mpt, str(ctg_lbl)))
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
        dct_rgs = lsz(args).rgs_to_typ(prm='dct', rtn=True)
        lst_old, lst_new = list(dct_rgs.keys()), list(dct_rgs.values())
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
        # *args预处理
        dct_typ = lsz(args).rgs_to_typ(prm='dct', rtn=True)
        # 转化部分
        for i_clm in [i for i in dct_typ.keys() if dct_typ[i] in ['lower', 'lwr']]:
            self.dts[i_clm] = self.dts.apply(lambda x: x[i_clm].lower() if type(x[i_clm]) is str else x[i_clm], axis=1)
        for i_clm in [i for i in dct_typ.keys() if dct_typ[i] in ['upper', 'ppr']]:
            self.dts[i_clm] = self.dts.apply(lambda x: x[i_clm].upper() if type(x[i_clm]) is str else x[i_clm], axis=1)
        self.dts_nit(False)
        if spr:
            self.spr_nit()
        if rtn:
            return self.dts

    def ltr_clm_flt(self, *args, spr=False, rtn=False, prn=False):
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
        dct_rgs = lsz(args).rgs_to_typ(prm='dct', rtn=True)
        lst_old, lst_clm = list(dct_rgs.keys()), list(dct_rgs.values())
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
        dct_rgs = lsz(args).rgs_to_typ(prm='dct', rtn=True)
        lst_old, lst_clm = list(dct_rgs.keys()), list(dct_rgs.values())
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
        dct_typ = lsz(args).rgs_to_typ(prm='dct', rtn=True)                # args转字典
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
        """
        encode or decode int columns with special coding dict.
        :param lst_clm:
        :param spr:
        :param rtn:
        :param prm: in ['encode','decode'], default 'encode'
        :return:
        """
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
        """
        encode columns contents with covering by '*'.
        :param lst_clm:
        :param spr:
        :param rtn:
        :param prm:
        :return:
        """
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
        """
        endocd column contents by md5.
        :param args: columns' name in a list, format in (targetColumn,), ([targetColumns]), (targetColumn, newColumn)
        :param spr:
        :param rtn:
        :param prm:
        :return:
        """
        dct_rgs = lsz(args).rgs_to_typ(prm='dct', rtn=True)
        lst_old, lst_clm = list(dct_rgs.keys()), list(dct_rgs.values())
        self.ltr_clm_typ(lst_old, 'str')
        for i in range(len(lst_clm)):
            self.dts[lst_clm[i]] = self.dts.apply(lambda x: stz(x[lst_old[i]]).ncd_md5(rtn=True, prm=prm), axis=1)
        self.dts_nit()
        if spr:
            self.spr_nit()
        if rtn:
            return self.dts


class dcmMixin(dfBsc):
    """
    documents' Mix in
    """
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
        >>> tst.drp_dm_na('A', prm='keep', rtn=True)
               A  B  C
        0    NaT  b  a
        :param lst_clm: default None means do nothing
        :param spr: let self = self.dts
        :param rtn: default False, return None
        :param prm:  remain documents at least <prm> cells not nan, default None
        :return: if rtn is True, return self.dts
        """
        flt_trs = None if prm in ['keep', 'kep', 'kp'] else prm
        flt_bgn = self.len
        lst_clm = lsz(lst_clm).typ_to_lst(rtn=True) if prm in ['keep', 'kep', 'kp', None] else lst_clm
        dts_drp = self.dts.dropna(axis=0,           # 0 for index and 1 for columns
                                  how='any',        # any and all
                                  thresh=flt_trs,   # optional Keep only the rows with at least 2 non-NA values
                                  subset=lst_clm)   # optional columns in target
        if prm in ['keep', 'kep', 'kp']:            # 当prm为'保留'时，反向删除不为空的行
            self.drp_dcm(list(dts_drp.index))
            print('info: %i remains from %i by keeping na.' % (self.len, flt_bgn))
        else:                                       # 当prm未经过特别定义时，对dfz.dts沿用去空操作
            self.dts = dts_drp.copy()
            print('info: %i remains from %i by dropping na.' % (self.len, flt_bgn))
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
        :param prm: dropping method, in ['first','last', 'keep'], default first, keep means keep duplicates in self.dts
        :param prn: print detail of dropping duplicates or not, default True
        :return: if rtn is True, return self.dts
        """
        lst_dpl = lsz(args[0]).typ_to_lst(rtn=True)
        if len(args) > 1:
            lst_srt = lsz(args[1]).typ_to_lst(rtn=True)
            self.srt_dcm(lst_srt, scd)
        len_bfr = self.len
        if prm.lower() in ['keep', 'kep']:
            self.drp_dcm(list(self.dts.drop_duplicates(subset=lst_dpl, keep=False).index))
        else:
            self.dts = self.dts.drop_duplicates(subset=lst_dpl, keep=prm)
        if prn:
            print("info: drop duplicate documents, %.i in total %.i left, from <%s>." % (len_bfr, self.len, self))
        if spr:
            self.spr_nit()
        if rtn:
            return self.dts

    def drp_dcm_ctt(self, *args, spr=False, rtn=False, prm='drop'):
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
        lst_rgs = lsz(args).rgs_to_typ(rtn=True)
        lst_clm, lst_ctt = lsz(lst_rgs[0], lst=True), lsz(lst_rgs[1], lst=True)
        if lst_clm.len == 1 and type(lst_ctt[0]) not in [list, lsz]:
            lst_ctt = lsz([lst_ctt.seq])            # 当仅检查单列时，需要控制其待识别内容为唯一个列表
        lst_clm.cpy_tal(lsz([lst_clm.seq, lst_ctt.seq]).len_max, spr=True)
        lst_ctt.cpy_tal(lsz([lst_clm.seq, lst_ctt.seq]).len_max, spr=True)
        for i in range(len(lst_clm)):
            if prm is False or prm in ['keep', 'kep']:   # 当not drop 或prm='keep'时，保留完全匹配的行
                self.dts = self.dts.loc[self.dts[lst_clm[i]].isin(lsz(lst_ctt[i]).typ_to_lst(rtn=True)), :]
            elif prm is True or prm in ['drop', 'drp']:     # 当drop 或prm='drop'时，删除完全匹配的行
                self.dts = self.dts.loc[~self.dts[lst_clm[i]].isin(lsz(lst_ctt[i]).typ_to_lst(rtn=True)), :]
            elif prm.lower() in ['partdrop', 'droppart', 'prtdrp', 'drpprt']:   # 当prm='partDrop'时，删除正则匹配的行
                lst_ndx = [k for j in lst_ctt[i] for k in self.dts.index if
                           re_search(j, str(self.dts.loc[k, lst_clm[i]]))]
                self.dts = self.dts.drop(lst_ndx, axis=0)
            elif prm.lower() in ['partkeep', 'keeppart', 'prtkep', 'kepprt']:   # 当prm='partKeep'时，保留正则匹配的行
                lst_ndx = [k for j in lst_ctt[i] for k in self.dts.index if
                           re_search(j, str(self.dts.loc[k, lst_clm[i]]))]
                lst_ndx = [i for i in self.dts.index if i not in lst_ndx]
                self.dts = self.dts.drop(lst_ndx, axis=0)
        if spr:
            self.spr_nit()
        if rtn:
            return self.dts

    def add_dcm_spl(self, *args, prm=',', spr=False, rtn=False):
        """
        add document like transpose in SAS, from one column splitting. 利用某个分隔符进行分列并转变为相同结构的多行.
        >>> tst = dfz([{'cls': '1', 'vls':'1,2,3'},
        >>>            {'cls': '2', 'vls':'a,b,c'},])
        >>> tst.typ_to_dtf()
        >>> tst.add_dcm_spl('vls', rtn=True)    # same as .add_dcm_spl('vls', 'vls', rtn=True)/({'vls':'vls'}, rtn=True)
          cls vls
        0   1   1
        1   1   2
        2   1   3
        3   2   a
        4   2   b
        5   2   c
        :param args: in format (str_clm_spl, str_clm_new), ({str_clm_spl:str_clm_new},(str_clm_spl)
        :param prm: a regex to split the target cell
        :param spr: let self = self.dts
        :param rtn: default False, return None
        :return: None if rtn
        """
        if 0 in self.clm:
            raise KeyError('a necessary column name 0 has been occupied, column 0 needs to be renamed')
        else:
            dct_rgs = lsz(args).rgs_to_typ(prm='dct', rtn=True)
            str_mpt, str_xpt = list(dct_rgs.keys())[0], list(dct_rgs.values())[0]
            dff = self.dts[str_mpt].str.split(prm, expand=True).stack()  # 分离为series形式
            dff.index = dff.index.codes[0]  # 保证index与self.ndx相符, 旧版本使用 = dff.index.labels[0]
            self.dts = concat([self.dts, dff], axis=1)
            self.drp_clm(str_mpt)
            self.rnm_clm({0: str_xpt})
            self.drp_dcm_ctt(str_xpt, '',)
        if spr:
            self.spr_nit()
        if rtn:
            return self.dts

    def add_dcm_agg(self, lst_clm_grp, lst_clm_agg=None, *, clm_cls='_cty', clm_vls='_vls', spr=False, rtn=False):
        """
        add documents from aggregating, group by lst_clm_grp，lst_clm_mpt中的同性质列转行，实现缩列扩行.
        >>> tst = dfz([{'date': '1', 'vls_A': 1.0, 'vls_B': 3.0},
        >>>            {'date': '2', 'vls_A': 2.0, 'vls_B': 2.0}])
        >>> tst.typ_to_dtf()
        >>> tst.add_dcm_agg('date', ['vls_A', 'vls_B'], rtn=True)   # same as .add_dcm_agg('date', rtn=True)
            _cty date _vls
        0  vls_A    1    1
        1  vls_A    2    2
        3  vls_B    1    3
        4  vls_B    2    2
        :param lst_clm_grp: 用于汇总分组的列，即 grouping by and aggregating
        :param lst_clm_agg: 同性质的将要换位同一列的多列名组成的列表, 若表中除lst_clm_grp外均需处理则可为空
        :param clm_cls: 缩列后用于存放标志多列来源的分类列，列中内容为lst_clm_mpt，default _cty
        :param clm_vls: 缩列后用于存放多列数据值的唯一列名，default _vls
        :param spr: let self = self.dts
        :param rtn: default False, return None
        :return: None
        """
        lst_dtf = []
        lst_clm_grp = lsz(lst_clm_grp).typ_to_lst(rtn=True)
        lst_clm_agg = [i for i in self.clm if i not in lst_clm_grp] if lst_clm_agg is None else \
            lsz(lst_clm_agg).typ_to_lst(rtn=True)
        for i_clm in lst_clm_agg:
            if i_clm in self.clm:
                lst_clm_grp_tmp = lsz(lst_clm_grp).typ_to_lst(rtn=True).copy()
                lst_clm_grp_tmp.append(i_clm)
                dtf_tmp = DataFrame(self.dts.values, columns=self.clm, index=lsz(i_clm).cpy_tal(self.len, rtn=True))
                dtf_tmp.index.name = clm_cls  # 设定index name 用于在reset_index环节直接导出成为column name
                dtf_tmp = dtf_tmp[lst_clm_grp_tmp]
                dtf_tmp.rename(columns={i_clm: clm_vls}, inplace=True)
                lst_dtf.append(dtf_tmp)  # 拼接用于for循环结束后的concat环节
        self.dts = concat(lst_dtf).reset_index()  # concat prm axis=0, reset_index prm drop=False
        if spr:
            self.spr_nit()
        if rtn:
            return self.dts


class cllMixin(dcmMixin, clmMixin):
    """
    methods Mixin for collections.
    """
    def __init__(self, dts=None, lcn=None, *, spr=False):
        super(cllMixin, self).__init__(dts, lcn, spr=spr)

    def mrg_dtf(self, dtf_mrg, *args, spr=False, rtn=False, prm='outer'):
        """merge dataframe horizontally or vertically.
        >>> import pandas as pd
        >>> tst = dfz()
        >>> tst.mrg_dtf(pd.DataFrame([{"A":1}]), pd.DataFrame([{"A":2}]), prm='vrl', rtn=True)
           A
        0  1
        1  2
        >>> tst = dfz([{'A':1,'B':'a'},{'A':2,'B':'b'}])
        >>> tst.typ_to_dtf()
        >>> tst.mrg_dtf(pd.DataFrame([{'A':2,'C':'x'}]), 'A', rtn=True)
           A  B    C
        0  1  a  NaN
        1  2  b    x
        :param dtf_mrg: if prm is 'vrl': merge[self.dts,dtf_mrg,*args]; else: dataframe merged on the right side
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
            self.dts = concat(lst_dtf, ignore_index=True)  # default keys=None, axis=0, sort=False
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
                self.dts = self.dts.loc[self.dts['_merge'] != 'both', :].drop(['_merge'], axis=1)   # 删除标记来源行
        if spr:
            self.spr_nit()
        if rtn:
            return self.dts

    def t(self, *, spr=False, rtn=False):
        """
        对第一行第一列为坐标轴的dataframe进行转置.
        :param spr: let self = self.dts
        :param rtn: default False, return None
        :return:
        """
        str_clm_tgt = self.clm[0]
        dtmp = self.dts.T
        dtmp.columns = list(dtmp.iloc[0, :])
        dtmp = dtmp.iloc[1:, :]
        dtmp[str_clm_tgt] = dtmp.index
        self.dts = dtmp
        lst_clm_srt = list(self.clm)
        lst_clm_srt.remove(str_clm_tgt)
        lst_clm_srt = [str_clm_tgt] + lst_clm_srt
        self.srt_clm(lst_clm_srt, drp=False)
        if spr:
            self.spr_nit()
        if rtn:
            return self.dts


class tmsMixin(cllMixin):
    """
    methods Mixin for columns on type timeStamp.
    """
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
        lst_rgs = lsz(args).rgs_to_typ(prm='eql', rtn=True)
        lst_old, lst_new = lst_rgs[0], lst_rgs[1]
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

    def add_dcm_by_day(self, str_grb, str_tms, lst_prd=None):
        """
        对某列进行分类后对类中所有可能的取值进行时间列的行补足.
        >>> tst = dfz([{'cls':1,'date':'2019-01-01'},{'cls':1,'date':'2019-01-04'},])
        >>> tst.typ_to_dtf()
        >>> tst.add_dcm_by_day('cls','date')
        >>> tst.tms_to_typ('date','str')
        >>> tst.srt_dcm('date')
        >>> print(tst.dts)
           cls        date
        0    1  2019-01-01
        1    1  2019-01-02
        2    1  2019-01-03
        3    1  2019-01-04
        :param str_grb: 用于分类的列
        :param str_tms: 需要填充的时间列
        :param lst_prd: 补足的时间窗口
        :return: None
        """
        prcdtf = cllMixin(self.dts)
        lst_prd = [None, None] if lst_prd is None else lst_prd
        lst_prd[0] = prcdtf.dts[str_tms].min() if lst_prd[0] is None else lst_prd[0]
        lst_prd[1] = prcdtf.dts[str_tms].max() if lst_prd[1] is None else lst_prd[1]
        self.dts = DataFrame(dtz().lst_of_day(lst_prd, rtn=True), columns=[str_tms])
        for i_cll in list(prcdtf.dts[str_grb].unique()):
            dtf_cll = self.dts.copy()
            dtf_cll[str_grb] = i_cll
            dtf_cll = cllMixin(dtf_cll)
            dtf_cll.drp_dcm_ctt(str_tms, list(prcdtf.dts.loc[prcdtf.dts[str_grb] == i_cll, :][str_tms].unique()))
            prcdtf.mrg_dtf(dtf_cll.dts, prm='vrl')
        self.dts = prcdtf.dts


class pltMixin(cllMixin):
    """
    methods Mixin for plots and tables.
    """
    def __init__(self, dts=None, lcn=None, *, spr=False):
        super(pltMixin, self).__init__(dts, lcn, spr=spr)

    def add_clm_mom(self, *args, spr=False, rtn=False, prm=None):
        """
        求环比变化率, add columns on month over month.
        >>> tst =dfz([{'A':'1','B':100},{'A':'2','B':110},{'A':'4','B':150},{'A':'3','B':100},])
        >>> tst.typ_to_dtf()
        >>> tst.add_clm_mom('B','b',prm='A')
           A    B       b
        0  1  100     NaN
        1  2  110  0.1000
        2  3  100 -0.0909
        3  4  150  0.5000
        :param args: target columns' name in format (x,) ,(oldColumn, newColumn), ({oldColumn: newColumn})
        :param spr: let self = self.dts
        :param rtn: default False, return None
        :param prm: sorted documents by which columns before generating mom
        :return: None
        """
        if prm:
            self.srt_dcm(prm)
        dct_rgs = lsz(args).rgs_to_typ(prm='dct', rtn=True)
        lst_clm = list(dct_rgs.keys())
        lst_new = [i+'_pop' for i in dct_rgs.keys()] if len(args) == 1 and type(args[0]) not in [dict] else \
            list(dct_rgs.values())
        for k in range(len(lst_clm)):
            for i in range(1, self.len):
                if self.dts.loc[i - 1, lst_clm[k]] > 0:
                    self.dts.loc[i, lst_new[k]] = round((self.dts.loc[i, lst_clm[k]] - self.dts.loc[i - 1, lst_clm[k]])
                                                        / self.dts.loc[i - 1, lst_clm[k]], 4)
        self.dts_nit()
        if spr:
            self.spr_nit()
        if rtn:
            return self.dts

    def add_clm_per(self, lst_grb, *args, spr=False, rtn=False, prm=True):
        """
        add columns on each documents percents of each group. 根据某个分组汇总后计算每一行在该分组总量中的占比并形成新列.
        >>> tst =dfz([{'A':'a','B':100},{'A':'a','B':110},{'A':'b','B':150},{'A':'c','B':100},])
        >>> tst.typ_to_dtf()
        >>> tst.add_clm_per('A', 'B', 'b', prm=False)
           A    B       b
        0  a  100  0.4762
        1  a  110  0.5238
        2  b  150  1.0000
        3  c  100  1.0000
        :param lst_grb: 用于分组的变量名列表
        :param args: 需要计算值的比例的变量名列表
        :param spr: let self = self.dts
        :param rtn: default False, return None
        :param prm: if drop template columns '_sum_x' or not, default True=drop.
        :return: None
        """
        dct_rgs = lsz(args).rgs_to_typ(prm='dct', rtn=True)
        dct_grb = dct_rgs.copy()
        for i in dct_grb.keys():
            dct_grb[i] = np_sum
        dtmp = self.dts.copy()
        dsrt = self.stt_clm(lst_grb, dct_grb, prm=['_sum_'+i for i in dct_grb.keys()], rtn=True)
        self.dts = dtmp
        self.mrg_dtf(dsrt, lst_grb, prm='left')
        for i in dct_rgs.keys():
            self.dts[dct_rgs[i]] = self.dts.apply(lambda x: round(x[i]/x['_sum_'+i], 4), axis=1)
        self.dts_nit()
        if prm:
            self.drp_clm(['_sum_'+i for i in dct_grb.keys()])
        if spr:
            self.spr_nit()
        if rtn:
            return self.dts


class dfz(tmsMixin, pltMixin):
    """
    dataFrame operating class with slots.
    """
    def __init__(self, dts=None, lcn=None, *, spr=False):
        super(dfz, self).__init__(dts, lcn, spr=spr)


print('info: class on dataFrame imported.')
