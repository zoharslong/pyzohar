#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 22 16:30:38 2019

@author: sl
@alters: 19-03-26 sl
"""
from pandas import DataFrame, merge
from pandas.core.series import Series as typ_pd_Series                  # 定义series类型
from socket import getfqdn, gethostname
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError # 当unique索引重复时抛出此错误
from progressbar import ProgressBar, UnknownLength
from .zhr_bsc import fnc_dct_for_mng_xpt, fnc_lst_frm_all, fnc_lst_to_thr


# process on mongodb basic
class PrcMngBsc:
    """proc mongodb basic
    find/input/update/output/drop documents or collections
    """
    def __init__(self, str_cln=None, str_dbs=None, str_hst=None):
        """initiate the target host - database - collection
        :param str_cln: the name of target collection in parameter type str
        :param str_dbs: the name of target database in parameter type str
        :param str_hst: the name of MongoClient host in parameter type str, default 'mongodb://localhost:27017/'
        :return: None
        """
        self.myHst, self.myDbs, self.myCln,self.mpt_id = None, None, None, None
        self.add_prm_mng(str_cln, str_dbs, str_hst)

    def add_prm_mng(self, str_cln, str_dbs, str_hst=None):
        """
        :param str_cln:
        :param str_dbs:
        :param str_hst:
        :return:
        """
        if str_hst is None:
            str_hst = "mongodb://shenlong:sl19890421@172.16.0.7:27017/admin" if getfqdn(gethostname()) == '172_16_0_14'\
                else "mongodb://localhost:27017"
        self.myHst = MongoClient(host=str_hst)
        self.myDbs = self.myHst[str_dbs] if str_dbs is not None else None
        self.myCln = self.myDbs[str_cln] if str_dbs is not None else None

    def lst_dbs(self):
        """pymongo.list_database_names
        :return: list of name of databases
        """
        return self.myHst.list_database_names()

    def lst_cln(self):
        """pymongo.list_collection_names
        :return: list of name of collections in the target database
        """
        return self.myDbs.list_collection_names()

    def lst_clm(self):
        """pymongo.find_one
        show column names in the first document
        :return: list of columns in the first document
        """
        return list(self.myCln.find_one().keys())

    def lst_dcm_for_clm(self, str_clm):
        """pymongo.distinct
        list all the value under one column
        :param str_clm: the target column's name
        :return: a list of the target column's values
        """
        return self.myCln.distinct(str_clm)

    def dlt_dcm(self, dct_qry=None, bln_cnt=False):  # delete documents in target collection
        """pymongo.delete_many
        delete documents by dct_qry
        :param dct_qry: query for deleting documents, default None for delete all documents
        :param bln_cnt: return the number of delete documents
        :return:
        """
        if dct_qry is None:
            if input("delete all(y/n): %s\n" % str(self.myCln)).lower() not in ["y", "yes", "t", "true"]:
                raise KeyError('nothing will happen for null dct_qry and wrong answer')
        prc_delete_many = self.myCln.delete_many(dct_qry)
        if bln_cnt:
            return prc_delete_many.deleted_count

    def ltr_dcm(self, dct_qry, dct_ltr):
        """
        alter documents by update
        :param dct_qry:
        :param dct_ltr:
        :return:
        """
        self.myCln.update(dct_qry, {"$set": dct_ltr})

    def dlt_dcm_by_dtf(self, dtf_mpt, bln_print=True):
        """pymongo.delete_many
        利用一个已有的dataframe,删除库中与其相同的documents
        :param dtf_mpt: the target dataframe for deleting documents
        :param bln_print: if print a short message for this step or not, default True
        :return: None
        """
        cnt_dlt = 0
        lst_mpt = dtf_mpt.to_dict(orient='records')
        bar = ProgressBar(max_value=UnknownLength)
        for i in range(len(lst_mpt)):
            cnt_delete_many = self.dlt_dcm(lst_mpt[i], True)
            cnt_dlt += cnt_delete_many
            bar.update(i)
        if bln_print:
            print("info: deleted %.i documents from collection" % cnt_dlt)

    def drp_cln(self, bln_ask=True):  # drop target collection
        """pymongo.drop
        print 'Y' to make sure that the target collection should be dropped
        :return: None
        """
        if bln_ask:
            if input("delete(y/n): %s\n" % str(self.myCln)).lower() in ["y", "yes", "t", "true"]:
                print("info: deleted: %s\n" % str(self.myCln))
                self.myCln.drop()
            else:
                raise KeyError('nothing happened for unknown input(needs yes/true)')
        else: self.myCln.drop()

    def stt_clm(self, str_clm, bln_ascending=True, dct_qry=None, lst_clm=None, flt_no=0, flt_no_ttl=10):
        """exporting a certain ranking value from a row in a collection
        :param str_clm: the name of the row/column
        :param bln_ascending: ascending - min; descending - max; default is 'max'
        :param dct_qry: query of this exporting method, default None, means all the documents
        :param lst_clm: all the columns set into calculation, means all the columns
        :param flt_no: exporting top n from the ranking, default 0 for number one
        :param flt_no_ttl: if flt_num is not default, flt_num_ttl needs to be big enough
        :return: a single value of the top flt_num
        """
        flt_typ = 1 if bln_ascending is True else -1        # 1 for min, -1 for max
        dct_clm_xpt = fnc_dct_for_mng_xpt(lst_clm, False)   # default no _id
        mtx_stt = self.myCln.find(dct_qry, dct_clm_xpt).sort([(str_clm, flt_typ)]).limit(flt_no_ttl)
        lst_xpt = []
        for dct_xpt in mtx_stt:
            if dct_xpt not in lst_xpt:
                lst_xpt.extend([dct_xpt])
        return lst_xpt[flt_no][str_clm]

    def crt_ndx(self, lst_ndx, bln_unique=False, bln_drp_eld=True, lst_srt=1):
        """
        当选定unique indexs时，索引在本集合中唯一，重复插入报错KeyError
        :param lst_ndx: 索引列表，也可以为想要添加为复合索引的列名的list, 最终自动转化为[(,1),(,1),...]形式
        :param bln_unique: 单个索引或复合索引在本集合中是否唯一，若是则重复插入的同索引将报错KeyError，默认False为可重复
        :param bln_drp_eld: 在生成新索引前是否删除全部现有的索引，默认True为全部删除
        :param lst_srt: 建立索引时各列名为正序（1）或倒序（-1），默认为1即全部正序
        :return: None
        """
        lst_ndx = fnc_lst_frm_all(lst_ndx) if type(lst_ndx) is str else lst_ndx
        lst_ndx = fnc_lst_to_thr(lst_ndx, 'listTuple', lst_srt)
        if bln_drp_eld:
            self.myCln.drop_indexes()
        self.myCln.create_index(lst_ndx, unique=bln_unique)


# process on mongodb import and export
class PrcMng(PrcMngBsc):
    """process of mongodb
    import/export dataset into/from collection
    nodup input from: https://blog.csdn.net/qq_23926575/article/details/79184055
    """
    def __init__(self, str_cln=None, str_dbs=None, str_hst=None):
        """initiating
        :param str_cln: the name of target collection in parameter type str
        :param str_dbs: the name of target database in parameter type str
        :param str_hst: the name of MongoClient host in parameter type str, default 'mongodb://localhost:27017/'
        :return: None
        """
        super(PrcMng, self).__init__(str_cln, str_dbs, str_hst)

    def xpt_cln_to_lst(self, dct_qry=None, lst_clm=None, bln_id=False, bln_print=True):
        """pymongo.find: 根据手动设置的条件对documents进行筛选导出
        :param dct_qry: a dict of exporting query, default None; formats as below
        :param lst_clm: a list of column names for export, default None
        :param bln_id: a boolean for _id's export
        :param bln_print: if print a info for this step or not, default True
        :return: the result in type nested list
        """
        # build a list of columns for exporting
        prc_find = self.myCln.find(dct_qry, fnc_dct_for_mng_xpt(lst_clm, bln_id))
        lst_xpt = []
        for dct_xpt in prc_find:
            lst_xpt.extend([dct_xpt])
        if bln_print:
            print("info: export %.i documents\n" % (len(lst_xpt)))
        return lst_xpt

    def xpt_cln_to_lst_by_dtf(self, dtf_mpt, lst_clm=None, bln_id=False, bln_print=True):
        """pymongo.find
        根据数据框中的列和内容对documents进行筛选导出
        :param dtf_mpt: 用于标记筛选的dtf
        :param lst_clm: a list of column names for export, default None
        :param bln_id: a boolean for _id's export
        :param bln_print: if print a info for this step or not, default True
        :return: the result in type nested list
        """
        lst_xpt = []
        for i_dct in dtf_mpt.to_dict(orient='records'):
            prc_find = self.xpt_cln_to_lst(i_dct, lst_clm, bln_id, False)
            lst_xpt.extend(prc_find)
        if bln_print:
            print("info: export %.i documents\n" % (len(lst_xpt)))
        return lst_xpt
    # # 整体导入
    def mpt_dtf_to_cln(self, dtf_mpt, str_clm_dtt=None, bln_cvr=False, lst_clm_dpl=None, bln_print=True):
        """import dataframe to collection, drop time period duplicates
        :param dtf_mpt: var name of target dataframe
        :param str_clm_dtt: column name of datetime in type str for upgrading the same period in collection
        :param bln_cvr: 是否采用覆盖导入, True - 用dtf中的重复变量列所在的行覆盖cln， False - 抛弃dtf中的重复列下的行并保持cln不变
        :param lst_clm_dpl: 需要根据dtf中具体的列进行去重时此项非None, 用于规定使用那几行对cln或dtf进行去重, default None,
        :param bln_print: if print a info for this step or not, default True
        :return: None
        """
        len_tmp = len(dtf_mpt)
        if str_clm_dtt:     # 时间列名不为空，则按照dtf_mpt中的时间段对库进行覆盖更新
            clm_dt_min, clm_dt_max = dtf_mpt[str_clm_dtt].min(), dtf_mpt[str_clm_dtt].max()
            try:
                clm_cl_max = self.stt_clm(str_clm_dtt, False)
            except IndexError:
                clm_cl_max = None
            if bln_cvr:
                cnt_delete_many = self.dlt_dcm({str_clm_dtt: {"$gte": clm_dt_min, "$lte": clm_dt_max}}, True)
                if bln_print:
                    print("info: %.i duplicates deleted from collection" % cnt_delete_many)
            else:
                len_tmp = len(dtf_mpt)
                dtf_mpt = dtf_mpt.loc[dtf_mpt[str_clm_dtt] > clm_cl_max, :] if clm_cl_max else dtf_mpt
                if bln_print:
                    print("info: %.i duplicates deleted from dataframe" % (len_tmp - len(dtf_mpt)))
        if lst_clm_dpl:     # 去重列名的列表不为空，则对该列名下的内容进行去重
            lst_clm_dpl = fnc_lst_frm_all(lst_clm_dpl)
            dtf_dpl = dtf_mpt[lst_clm_dpl].copy()
            dtf_dpl.drop_duplicates(subset=lst_clm_dpl, inplace=True)
            if bln_cvr:     # 覆盖法，则删除库中将会与dtf_mpt重复的documents
                self.dlt_dcm_by_dtf(dtf_dpl, bln_print)
            else:           # 不覆盖，则删除原dtf_mpt中将会与库中数据重复的行
                dtf_dpl = DataFrame(self.xpt_cln_to_lst_by_dtf(dtf_dpl, lst_clm_dpl))       # 导出重复行
                if dtf_dpl.shape[1] > 0:                                                    # 当重复行为零时不进行筛除
                    dtf_dpl.drop_duplicates(subset=lst_clm_dpl, inplace=True)
                    dtf_mpt = merge(dtf_mpt, dtf_dpl, 'left', lst_clm_dpl, indicator=True)  # 标记重复行
                    dtf_mpt = dtf_mpt.loc[dtf_mpt['_merge'] != 'both', :]                   # 删除重复行
                    dtf_mpt.drop(['_merge'], axis=1, inplace=True)                          # 删除标记列
                if bln_print:
                    print("info: %.iduplicates deleted from dataframe" % (len_tmp - len(dtf_mpt)))
        if len(dtf_mpt) > 0:
            x = self.myCln.insert_many(dtf_mpt.to_dict(orient='records'))                   # 导入非重行
            self.mpt_id = x.inserted_ids
        if bln_print:
            print("info: %.i documents inserted\n" % len(dtf_mpt))
    # # 逐行导入
    def pdt_srs_to_cln(self, dts_mpt, lst_clm_ndx, bln_cvr=True, bln_return=False):
        """
        向集合中更新一行pd.Series，若lst_clm_ndx重复则可以选择是否覆盖
        :param dts_mpt: 一行数据，进入集合后视为一个文档，可选类型为[pd.Series, dict]
        :param lst_clm_ndx: 在此集合中选定为索引且unique=True的列名
        :param bln_cvr: 当索引重复时是否用dts_mpt覆盖集合内的行，默认True
        :param bln_return: 返回输入的情况
        :return: None
        """
        if type(dts_mpt) == typ_pd_Series:
            dct_clm_ndx = dts_mpt[lst_clm_ndx].to_dict()
            dct_mpt = dts_mpt.to_dict()
        elif type(dts_mpt) == dict:
            dct_mpt = dts_mpt.copy()
            dct_clm_ndx = {i: dct_mpt[i] for i in lst_clm_ndx}
        else:
            raise KeyError("stop: type of dts_mpt needs in [pd.Series, dict]")
        n, c, d= 0, 0, 0
        try:
            self.myCln.insert_one(dct_mpt)
            n+=1
        except DuplicateKeyError:
            dct_mpt.pop('_id')
            if bln_cvr and len(dct_mpt) > 0:
                self.myCln.update_one(dct_clm_ndx, {'$set': dct_mpt})
                c+=1
            else: d += 1
        if bln_return:
            return n, c, d
    # # 以逐行为基础导入dataframe
    def pdt_dtf_to_cln(self, dtf_mpt, lst_clm_ndx, bln_cvr=True, bln_progressbar=False):
        """
        逐行更新，保持集合中lst_clm_ndx唯一
        :param dtf_mpt: pd.DataFrame
        :param lst_clm_ndx: 在此集合中选定为索引且unique=True的列名
        :param bln_cvr: 当索引重复时是否用dts_mpt覆盖集合内的行，默认True
        :param bln_progressbar: 是否显示导入计数, 默认False
        :return: None
        """
        nt, ct, dt = 0, 0, 0
        bar = ProgressBar(max_value=UnknownLength) if bln_progressbar else None
        for i in range(len(dtf_mpt)):
            if bln_progressbar:
                bar.update(i)
            n, c, d = self.pdt_srs_to_cln(dtf_mpt.loc[i, :], lst_clm_ndx, bln_cvr, True)
            nt+=n
            ct+=c
            dt+=d
        print('info: %i inserted, %i updated, %i dropped.\n' % (nt, ct, dt))
    # # 新建&创建索引或导入dataframe
    def crt_and_pdt(self, dtf_mpt, lst_clm_ndx, str_prd_mpt=None, str_prd_bgn=None,
                    bln_cvr=True, bln_progressbar=False):
        """import from dataframe to collections
        根据库是否存在, 进行有选择性的导入策略,全部导入或更新导入
        :param dtf_mpt: 待导入的数据集
        :param lst_clm_ndx: 为新建的集合创建唯一索引组
        :param str_prd_mpt: 本次操作所选择的时间窗起点，当此起点与初始化窗口起点相同时，采取创建策略
        :param str_prd_bgn: 初始化的窗口起点
        :param bln_cvr: 覆盖或是舍弃, default cover, True
        :param bln_progressbar: 进度条, default False
        :return: None
        """
        print('*****', self.myDbs.name, '.', self.myCln.name, '*****')
        if str_prd_mpt == str_prd_bgn and self.myCln.count() == 0:
            self.mpt_dtf_to_cln(dtf_mpt)
            self.crt_ndx(lst_clm_ndx, True)
        else:
            self.pdt_dtf_to_cln(dtf_mpt, lst_clm_ndx, bln_cvr, bln_progressbar)
