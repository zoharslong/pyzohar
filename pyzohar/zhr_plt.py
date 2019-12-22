#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 22 16:30:38 2019

@author: sl
@alters: 19-03-26 sl
"""
from os import listdir
from os.path import exists, join
from openpyxl import load_workbook
from re import search
from numpy import sum as np_sum
from pandas import read_csv, read_excel, DataFrame, ExcelWriter, ExcelFile
from .zhr_dtf import PrcDtf
from .zhr_bsc import fnc_lst_frm_all,fnc_lst_cpy_cll,fnc_dct_frm_lst,fnc_lst_frm_cll_mrg
from warnings import warn


def fnc_xpt_for_plt_js(pth_xpt, lst_btm, lst_slt, lst_plt):
    """
    仅用于产生适应当前bottom, select, dataset三类变量的js数据集供当前泛用型echarts脚本调用
    :param pth_xpt: 输出地址
    :param lst_btm: <bottom>相关的变量名和文本内容
    :param lst_slt: <select>相关的变量名和选项内容——与下项相匹配的键值对
    :param lst_plt: <div echarts>相关的变量名和等待调用的键值对
    :return: None
    """
    # 修改bottom文档数据调用的内容
    dxpt = PrcPlt()
    dxpt.init_dts_by_lines('print', lst_btm[1])
    dxpt.add_clm_fmt_for_plt([lst_btm[0]], ['str'])
    dxpt.xpt_txt_frm_dts(pth_xpt, 'w', 2)
    # 修改select目录数据为var lst_slt_0100 = [{'vls':'ptn00','txt':'date'},{'vls':'ptn01','txt':'week'},..];形式
    dplt_tmp = DataFrame([['ptn'+str('%02d' % i) for i in range(len(lst_slt[1]))], lst_slt[1]]).T
    dplt_tmp.columns = ['vls', 'txt']
    dxpt.init_dts_by_lines('print', str(dplt_tmp.to_dict('records')),)
    dxpt.add_clm_fmt_for_plt([lst_slt[0]], [''])
    dxpt.xpt_txt_frm_dts(pth_xpt, 'a', 2)
    # 修改本体数据为var dct_ptn_0100 = {'ptn00':dts00,'ptn01':dts01,...}形式
    dplt_tmp = DataFrame(lst_plt[1]).T
    dplt_tmp.columns = ['ptn'+str('%02d' % i) for i in range(len(dplt_tmp.columns))]
    dxpt.init_dts_by_lines('print', str(dplt_tmp.to_dict('list')),)
    dxpt.add_clm_fmt_for_plt([lst_plt[0]], [''])
    dxpt.xpt_txt_frm_dts(pth_xpt, 'a', 2)


class PrcPlt(PrcDtf):
    """
    process ploting
    """
    def __init__(self, dts_mpt=None, bln_rst_ndx=False):
        """initiate
        :param dts_mpt: var name of target dataset
        :return: None
        """
        super(PrcPlt, self).__init__(dts_mpt, bln_rst_ndx)

    def add_clm_fmt_for_plt(self, lst_var, lst_typ, str_clm=None, str_clm_xpt='print'):
        """
        将列str_clm的内容添加varname及必要的首尾，写入新列'print'中
        将原dataframe通过lst_var=[A,B,..], lst_typ['str','',..]重构为 var A = '..'; var B = ..; ... 的形式
        :param lst_var: dataframe中每一行的varname, ''意味着不做更改
        :param lst_typ: in ['str',''], 'str' for including content with '', '' on the contrary
        :param str_clm: original column name
        :param str_clm_xpt: the new column in new format
        :return: None
        """
        warn('deprecated')
        str_clm = self.clm[0] if str_clm is None else str_clm
        lst_var = fnc_lst_frm_all(lst_var)
        lst_typ = fnc_lst_cpy_cll(lst_typ, len(lst_var))
        for i in range(len(lst_var)):
            if lst_var[i] == '':
                self.dts.loc[i, str_clm_xpt] = self.dts.loc[i, str_clm]
            elif type(self.dts.loc[i, str_clm]) in [str] and lst_typ[i] == 'str':
                self.dts.loc[i, str_clm_xpt] = \
                    'var ' + str(lst_var[i]) + ' = "' + \
                    str(self.dts.loc[i, str_clm]).replace(', None', '') + '";'
            else:
                self.dts.loc[i, str_clm_xpt] = \
                    'var ' + str(lst_var[i]) + ' = ' + \
                    str(self.dts.loc[i, str_clm]).replace(', None', '') + ';'
        self.init_rst()
    # generate type from dataframe to list for js
    def typ_dts_to_lst_for_plt(self):
        """
        将dataframe转化为[[clmA,clmB,..],[cellA1,cellB1,..],[cellA2,cellB2,..],...]
        :return: None
        """
        warn('deprecated')
        dts_ctt = self.dts.to_dict('split')['data']
        self.dts = [self.dts.to_dict('split')['columns']]
        self.dts.extend(dts_ctt)
        self.init_rst()

    def typ_dts_to_lst_for_plt_frm_date_to_mnth(self, str_clm_tms='ref_date'):
        """
        将包含日期列的标准数据集转化成['date','week','mnth']三元素的列表用于支持echarts图表的dataset
        :param str_clm_tms: 希望转化的数据集中的日期列名
        :return: 列表[[data in date],[data in week],[data in mnth]]
        """
        warn('deprecated')
        # 构造用于合并形成周、月汇总数据的aggregating字典
        lst_clm_vls = list(self.clm)
        lst_clm_vls.remove(str_clm_tms)
        dct_clm_vls = fnc_dct_frm_lst(lst_clm_vls, [np_sum])
        self.add_clm_x_dtt(str_clm_tms)
        #
        lst_fnl = []
        for i_prd in ['x_date', 'x_week', 'x_mnth']:
            lst_clm = [i_prd]
            lst_clm.extend(lst_clm_vls)
            prc_tmp = PrcPlt(self.dts)
            prc_tmp.srt_clm(lst_clm)
            prc_tmp.srt_dcm(i_prd)
            if i_prd == 'x_date':
                pass
            else:
                prc_tmp.add_clm_frm_stt([i_prd], dct_clm_vls)
            prc_tmp.typ_dts_to_lst_for_plt()
            lst_fnl.append(prc_tmp.dts)
        self.init_dts(lst_fnl)
        self.init_rst()
    # merge documents from existed csv, or export the largest cell in a certain column in the csv
    def sav_dtf_to_csv(self, pth_csv, str_typ='date',  str_clm='do_day'):
        """save dataframe to csv, updating a period in the older one; or export a limit date from csv
        :param str_typ: in ['date','upgrade'], 'date' means export a limit date from the csv
        :param pth_csv: the path of a file in type csv
        :param str_clm: column of datetime in type str
        :return: None
        """
        if exists(pth_csv):
            dtf_tmp = self.dts.copy()
            self.add_dts(read_csv(pth_csv, encoding="utf-8"))
            self.drp_clm('Unnamed: 0')
            self.srt_dcm(str_clm, False)
            if str_typ == 'date':
                return self.dts[str_clm].max()
            else:
                self.add_dts(self.dts.loc[self.dts[str_clm] < dtf_tmp[str_clm].min(), :])
                self.mrg_dtf_vrl(None, dtf_tmp)
                self.srt_dcm(str_clm)
        elif self.len > 0:
            print('new created: ', pth_csv)
            self.dts.to_csv(pth_csv, encoding="utf-8")

    def sav_dtf_to_xcl(self, pth_xcl, str_typ='date',  str_clm='do_day'):
        """save dataframe to xlsx, updating a period in the older one; or export a limit date from csv
        :param str_typ: in ['date','upgrade'], 'date' means export a limit date from the csv
        :param pth_xcl: the path of a file in type csv
        :param str_clm: column of datetime in type str
        :return: None
        """
        if exists(pth_xcl):
            dtf_tmp = self.dts.copy()
            self.add_dts(read_excel(pth_xcl))
            self.srt_dcm(str_clm, False)
            if str_typ == 'date':
                return self.dts[str_clm].max()
            else:
                self.add_dts(self.dts.loc[self.dts[str_clm] < dtf_tmp[str_clm].min(), :])
                self.mrg_dtf_vrl(None, dtf_tmp)
                self.srt_dcm(str_clm)
        else:
            print('new created: ', pth_xcl)
        self.dts.to_excel(pth_xcl)

    def sav_dtf_to_txt(self, pth_xpt, str_typ_open='a', int_strip=2, str_typ='slt', str_ptn=None, lst_dts=None):
        """
        save dataframe to .txt
        :param pth_xpt:
        :param str_typ_open:
        :param int_strip:
        :param str_typ:
        :param str_ptn:
        :param lst_dts:
        :return:
        """
        if str_typ in ['slt']:
            dplt_tmp = DataFrame([['ptn' + str('%02d' % i) for i in range(len(lst_dts))], lst_dts]).T
            dplt_tmp.columns = ['vls', 'txt']
            self.init_dts_by_lines('print', str(dplt_tmp.to_dict('records')),)
        elif str_typ in ['dct','dts']:
            dplt_tmp = DataFrame(lst_dts).T
            dplt_tmp.columns = ['ptn'+str('%02d' % i) for i in range(len(dplt_tmp.columns))]
            self.init_dts_by_lines('print', str(dplt_tmp.to_dict('list')), )
        elif str_typ in ['str','btm']:
            self.init_dts_by_lines('print', lst_dts)
        else: raise KeyError('stop')
        self.add_clm_fmt_for_plt([str_ptn], ['' if str_typ in ['slt','dct','dts'] else 'str'])
        self.xpt_txt_frm_dts(pth_xpt, str_typ_open, int_strip)


class PrcSav(PrcDtf):
    """
    process on excel
    """
    def __init__(self, dts_mpt=None, pth_fld=None, str_fil=None):
        """
        initiate with self.init_sav_prm
        :param dts_mpt: data set for inserting
        :param pth_fld: path of the fold
        :param str_fil: filename with .ex, such as .xlsx, .js etc.
        """
        super(PrcSav, self).__init__(dts_mpt)
        self.fld, self.fil, self.xpt, self.wrt = pth_fld, str_fil, None, None
        if pth_fld is not None and str_fil is not None:
            self.init_sav_prm(pth_fld, str_fil)
    # initiation of saving path
    def init_sav_prm(self, pth_fld, str_xcl):
        """

        :param pth_fld:
        :param str_xcl:
        :return:
        """
        self.fld = pth_fld
        self.fil = str_xcl
        self.xpt = join(self.fld, self.fil)
        if search('\.xls', self.fil):
            self.wrt = ExcelWriter(self.xpt, engine='openpyxl')

    def add_clm_fmt_for_jsn(self, lst_var, lst_typ, str_clm=None, str_clm_xpt='print'):
        """
        将列str_clm的内容添加varname及必要的首尾，写入新列'print'中
        将原dataframe通过lst_var=[A,B,..], lst_typ['str','',..]重构为 var A = '..'; var B = ..; ... 的形式
        :param lst_var: dataframe中每一行的varname, ''意味着不做更改
        :param lst_typ: in ['str',''], 'str' for including content with '', '' on the contrary
        :param str_clm: original column name
        :param str_clm_xpt: the new column in new format
        :return: None
        """
        str_clm = self.clm[0] if str_clm is None else str_clm
        lst_var = fnc_lst_frm_all(lst_var)
        lst_typ = fnc_lst_cpy_cll(lst_typ, len(lst_var))
        for i in range(len(lst_var)):
            if lst_var[i] == '':
                self.dts.loc[i, str_clm_xpt] = self.dts.loc[i, str_clm]
            elif type(self.dts.loc[i, str_clm]) in [str] and lst_typ[i] == 'str':
                self.dts.loc[i, str_clm_xpt] = \
                    'var ' + str(lst_var[i]) + ' = "' + \
                    str(self.dts.loc[i, str_clm]).replace(', None', '') + '";'
            else:
                self.dts.loc[i, str_clm_xpt] = \
                    'var ' + str(lst_var[i]) + ' = ' + \
                    str(self.dts.loc[i, str_clm]).replace(', None', '') + ';'
        self.init_rst()
    # save values into a txt file
    def sav_dtf_to_txt(self, str_typ_open='a', int_strip=2, str_typ='slt', lst_ptn=None, lst_dts=None):
        """
        save dataframe to .txt
        :param str_typ_open: in ['w' for first time, 'a' for continue]
        :param int_strip: default 2 for cutting head and tail of the final data text
        :param str_typ: in ['slt' for select, 'str' for <p>, 'dct' for list on echarts,'pur' for 直接进入]
        :param lst_ptn: option name in list
        :param lst_dts: datasets in list
        :return: None
        """
        if str_typ in ['slt']:
            dplt_tmp = DataFrame([['ptn' + str('%02d' % i) for i in range(len(lst_dts))], lst_dts]).T
            dplt_tmp.columns = ['vls', 'txt']
            self.init_dts_by_lines('print', str(dplt_tmp.to_dict('records')),)
        elif str_typ in ['dct','dts']:
            dplt_tmp = DataFrame(lst_dts).T
            dplt_tmp.columns = ['ptn'+str('%02d' % i) for i in range(len(dplt_tmp.columns))]
            self.init_dts_by_lines('print', str(dplt_tmp.to_dict('list')), )
        elif str_typ in ['str','btm','pur']:
            self.init_dts_by_lines('print', str(lst_dts))
        else: raise KeyError('stop: str_typ needs ["slt","dct","dts","str","btm","pur"].')
        self.add_clm_fmt_for_jsn(lst_ptn, ['' if str_typ in ['slt','dct','dts','pur'] else 'str'])
        self.xpt_txt_frm_dts(self.xpt, str_typ_open, int_strip)
    # save dataframe into ceratin sheets in a excel file
    def sav_dtf_to_xcl(self, str_sht, dtf_mpt=None):
        """
        export excel from dataframe to an excel file
        :param str_sht: sheet name
        :param dtf_mpt: a dataframe to be exported
        :return: None
        """
        self.dts = dtf_mpt if dtf_mpt is not None else self.dts
        if self.fil not in listdir(self.fld):
            self.dts.to_excel(self.xpt, sheet_name=str_sht, index=None)
        else:
            prc_chk = ExcelFile(self.xpt)
            if str_sht in prc_chk.sheet_names: raise KeyError('stop: sheet is already exist.')
            self.wrt.book = load_workbook(self.wrt.path)
            self.dts.to_excel(excel_writer=self.wrt, sheet_name=str_sht, index=None)
            self.wrt.close()
    # merge documents from existed csv, or export the largest cell in a certain column in the csv
    def sav_dtf_to_csv(self, str_typ='date',  str_clm='do_day'):
        """save dataframe to csv, updating a period in the older one; or export a limit date from csv
        :param str_typ: in ['date','upgrade'], 'date' means export a limit date from the csv
        :param str_clm: column of datetime in type str
        :return: None
        """
        if exists(self.xpt):
            dtf_tmp = self.dts.copy()
            self.add_dts(read_csv(self.xpt, encoding="utf-8"))
            self.drp_clm('Unnamed: 0')
            self.srt_dcm(str_clm, False)
            if str_typ == 'date':
                return self.dts[str_clm].max()
            else:
                self.add_dts(self.dts.loc[self.dts[str_clm] < dtf_tmp[str_clm].min(), :])
                self.mrg_dtf_vrl(None, dtf_tmp)
                self.srt_dcm(str_clm)
        elif self.len > 0:
            print('new created: ', self.xpt)
            self.dts.to_csv(self.xpt, encoding="utf-8")