#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 22 16:30:38 2019

@author: sl
@alters: 19-03-26 sl
"""
from os import listdir
from sas7bdat import SAS7BDAT as typ_sas7bdat                           # 定义sas7bdatb包带来的sas文件类型sas7bdat
from numpy import ndarray as typ_np_ndarray                             # 定义np.ndarray类型
from pandas import DataFrame, concat
from pandas.core.series import Series as typ_pd_Series                  # 定义pd.series类型
from pandas.core.frame import DataFrame as typ_pd_DataFrame             # 定义pd.dataframe类型
# from warnings import warn
from .zhr_bsc import fnc_dtf_frm_csv_mpt, fnc_dtf_frm_xcl_mpt


class PrcDts:
    """process dataset
    define a dataset and alter it between different types
    use dir(PrcDts) to show def list
    """
    def __init__(self, dts_mpt=None, bln_rst_ndx=False):
        """initiate
        :param dts_mpt: a dataset to insert into class
        :return: None
        """
        self.dts, self.typ, self.len, self.ndx, self.clm, self.vls, self.kys, self.head, self.tail = \
            None, None, None, None, None, None, None, None, None
        self.init_dts(dts_mpt)
        self.init_rst(bln_rst_ndx)

    def init_dts(self, dts_mpt):
        """
        initiate data set into self.dts
        :param dts_mpt:
        :return:
        """
        try:                            # try to copy raw dataset on [list, dict, array, dataframe, series,]
            self.dts = dts_mpt.copy()
        except AttributeError:          # the datasets cannot be copied like [tuple]
            self.dts = dts_mpt
        # self.init_rst()

    def init_dts_by_lines(self, str_clm=None, *cll):
        """
        逐行添加dts，其实没啥用，等同于pd.DataFrame([*cll], columns=[str_clm])
        :param str_clm: 唯一的列名
        :param cll: 每一行的内容
        :return: None
        """
        # warn('useless def, can be instead with pd.DataFrame')
        str_clm = 'print' if str_clm is None else str_clm
        self.dts = DataFrame(list(cll), columns=[str_clm])
        self.init_rst()

    def init_rst(self, bln_rst_ndx=True, bln_drp=True, str_lvl=None):
        """initiate resetting
        :param bln_rst_ndx: 是否重置index
        :param str_lvl: 若不抛弃原Index，则第几级index将进入数据表成为列, default None
        :param bln_drp: 是否抛弃index，若False则将index按照index名还原至表中(用于group by/aggregate), default True
        :return: None
        """
        self.typ = type(self.dts)   # type of dts
        if self.dts is None:
            pass
        elif type(self.dts) in [typ_pd_DataFrame, typ_pd_Series]:
            self.init_rst_pds(bln_rst_ndx, bln_drp, str_lvl)
        elif type(self.dts) in [list]:
            self.init_rst_lst()

    def init_rst_pds(self, bln_rst_ndx=True, bln_drp=True, str_lvl=None):
        """
        :param bln_rst_ndx: reset index or not
        :param bln_drp: drop index or not
        :param str_lvl: if index is not dropped, which level of the indexes is send into dataframe
        :return:  None
        """
        if bln_rst_ndx is True:
            self.dts.reset_index(drop=bln_drp, inplace=True, level=str_lvl)
        self.len = len(self.dts)
        self.ndx = self.dts.index
        self.clm = self.dts.columns if self.typ in [typ_pd_DataFrame] else self.clm
        self.kys = self.dts.keys()
        self.head = self.dts.head()
        self.tail = self.dts.tail()
        # self.vls = self.dts.values

    def init_rst_lst(self):
        """
        initiation on reset list dts
        :return:
        """
        self.len = len(self.dts)
        self.ndx = range(self.len)
        self.clm = None
        self.vls = None
        self.kys = None
        self.head = self.dts[:5]
        self.tail = self.dts[-5:]

    def typ_dts_to_dtf(self, lst_clm=None, bln_rst_ndx=True):
        """turn self.dts's type to pd.DataFrame
        :param lst_clm: some type needs columns' name
        :param bln_rst_ndx:
        :return: None
        """
        if self.dts is None:
            self.dts = DataFrame()
        elif self.typ in [dict]:
            self.dts = DataFrame([self.dts])
        elif self.typ in [list, typ_np_ndarray]:
            self.dts = DataFrame(self.dts, columns=lst_clm)
        elif self.typ in [typ_pd_Series]:
            self.dts = DataFrame(self.dts)
        elif self.typ in [typ_sas7bdat]:  # https://pypi.org/project/sas7bdat/
            self.dts = self.dts.to_data_frame()
        elif self.typ in [typ_pd_DataFrame]:
            pass
        else:
            raise AttributeError('type of dts is not available')
        self.init_rst(bln_rst_ndx)

    def typ_dtf_to_lst(self, str_orient='records', bln_rst_ndx=False):
        """ alter dataframe to list
        http://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.to_dict.html
        :param str_orient: to_dict(orient=''), default 'records' in ['records','dict','list','series','split','index']
        :param bln_rst_ndx:
        :return: None
        """
        if type(self.dts) in [typ_pd_DataFrame]:
            self.dts = self.dts.to_dict(orient=str_orient)
        else:
            raise AttributeError('type of dts is not available')
        self.init_rst(bln_rst_ndx)

    def mpt_dts_frm_fld(self, str_typ, pth_fold, str_sep_hdr=None):
        """import excel, csv files to dataframe from fold
        :param str_typ: the type of files in target fold
        :param pth_fold: the fold path in type str
        :param str_sep_hdr: the sep of csv, default None, in ['\t','\ ',...]
        :return: None
        """
        cnt_fil = 0
        self.typ_dts_to_dtf()
        lst_fil = listdir(pth_fold)
        for i_fil in lst_fil:
            if str_typ.lower() in ['csv'] and i_fil[-4:] in ['.csv']:
                cnt_fil += 1
                dtf_mpt = fnc_dtf_frm_csv_mpt(pth_fold, i_fil, str_sep_hdr)
            elif str_typ.lower() in ['xls', 'xlsx'] and i_fil[-4:] in ['.xls', 'xlsx']:
                cnt_fil += 1
                dtf_mpt = fnc_dtf_frm_xcl_mpt(pth_fold, i_fil, str_sep_hdr)
            else:
                dtf_mpt = DataFrame()
            # self.dts = concat([self.dts, dtf_mpt], axis=0, ignore_index=True, sort=False)
            self.dts = concat([self.dts, dtf_mpt], axis=0, ignore_index=True)
        self.init_rst(True)
        print("info: input %.i / %.i from %s\n" % (cnt_fil, len(lst_fil), pth_fold))

    def xpt_txt_frm_dts(self, pth_txt, str_typ_open='w', int_strip=2, bln_cover=True):
        """
        import dataset to file.txt or file.js(https://blog.csdn.net/matrix_google/article/details/76861485) line by line
        :param pth_txt: path of file in ['.txt','.js']
        :param str_typ_open: type of open, usually in ['a','w'], a for continue, w for cover
        :param int_strip: cut how many units in the head and tail of each row, default 2 to be compatible with echarts
        :param bln_cover: check if the pth_txt is already exists or not, False means if it exists, then do nothing
        :return: None
        """
        from os.path import exists
        if exists(pth_txt) and bln_cover is False:
            print("stop: the txt %s already exists\n" % pth_txt)
        elif type(self.dts) not in [list, typ_pd_DataFrame]:
            print('stop: type of dts_npt needs [list, pd.DataFrame]')
        else:
            if type(self.dts) is list:
                lst_dts_mpt = [str(i_dcm) for i_dcm in self.dts]
            else:  # alter the type of a line in dataframe from slice to str
                lst_dts_mpt = [str(i_dcm)[int_strip: -int_strip] for i_dcm in
                               self.dts.to_dict(orient='split')['data']]
            prc_txt_writer = open(pth_txt, str_typ_open, encoding='utf-8')
            for i in range(len(self.dts)):
                prc_txt_writer.writelines(lst_dts_mpt[i])
                prc_txt_writer.write('\n')
            prc_txt_writer.close()
            print("info: success input dts to txt %s\n" % pth_txt)


