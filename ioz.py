#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 22 16:30:38 2019
input/output operation.
@author: zoharslong

@alters:
2020-01-16 zoharslong
"""
from numpy import ndarray as typ_np_ndarray
from pandas.core.series import Series as typ_pd_Series                  # 定义series类型
from pandas.core.frame import DataFrame as typ_pd_DataFrame             # 定义dataframe类型
from pandas.core.indexes.base import Index as typ_pd_Index              # 定义dataframe.columns类型
from pandas.core.indexes.range import RangeIndex as typ_pd_RangeIndex   # 定义dataframe.index类型
from pandas.core.groupby.generic import DataFrameGroupBy as typ_pd_DataFrameGroupBy     # 定义dataframe.groupby类型
from pandas import DataFrame as pd_DataFrame, read_csv, read_excel, concat, ExcelWriter
from os import path
from os.path import exists
# from socket import getfqdn, gethostname                               # 获得本机IP
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from pymysql import connect, IntegrityError
from requests import post
from json import loads, dumps, JSONDecodeError
from .bsc import stz, lsz, dcz


class ioBsc(pd_DataFrame):
    """
    I/O basic
    ioBsc.lcn in format {'fld','fls','mng','mdb','cln','sql','sdb','tbl','url','pst','hdr'}
    """
    lst_typ_dts = [
        str,
        stz,
        list,
        lsz,
        dict,
        dcz,
        tuple,
        typ_np_ndarray,
        typ_pd_DataFrame,
        typ_pd_Series,
        typ_pd_Index,
        typ_pd_RangeIndex,
        typ_pd_DataFrameGroupBy,
    ]   # data sets' type
    lst_typ_lcn = [list, lsz, dict, dcz]   # io methods' type

    def __init__(self, dts=None, lcn=None, *, spr=False):
        # all the i/o operations have the same attributes for locate target data: location and collection
        super().__init__()                  # 不将dts预传入DataFrame
        self.__dts, self.typ = None, None   # 接受数据
        self.len, self.clm, self.hdr, self.tal = None, None, None, None
        self.kys, self.vls = None, None
        self.__lcn, self.iot = None, None   # 连接信息
        self._mySql, self._mySdb, self._myTbl = None, None, None
        self._myMng, self._myMdb, self._myCln = None, None, None
        self.__init_rst(dts, lcn, spr=spr)

    def __init_rst(self, dts=None, lcn=None, *, spr=False):
        """
        private reset initiation.
        :param dts: a data set to input or output
        :return: None
        """
        self.dts = dts if self.dts is None and dts is not None else []
        self.lcn = lcn if self.lcn is None and lcn is not None else {}
        if spr:
            self.spr_nit()

    def spr_nit(self, rtn=False):
        try:
            super(ioBsc, self).__init__(self.__dts)
        except ValueError:
            print('info: %s cannot convert to DataFrame.' % (str(self.__dts)[:8]+'..'))
        if rtn:
            return self

    def __str__(self):
        """
        print(io path).
        :return: None
        """
        return '<io: %s; ds: %s>' % (str(self.lcn)[1:-1], self.typ)
    __repr__ = __str__  # 调用类名的输出与print(className)相同

    @property
    def dts(self):
        """
        @property get & set lsz.seq.
        :return: lsz.seq
        """
        return self.__dts

    @dts.setter
    def dts(self, dts):
        """
        self.dts = dts
        :param dts: a dataset to import.
        :return: None
        """
        if dts is None or type(dts) in self.lst_typ_dts:
            self.__dts = dts
            self.__attr_rst('dts')
        else:
            raise TypeError('info: dts\'s type %s is not available.' % type(dts))

    def set_dts(self, dts, *, ndx_rst=True, ndx_lvl=None):
        """
        if do not reset index after set data set, use self.set_dts() instead of self.dts
        :param dts: data set to fill self.dts
        :param ndx_rst: if reset data set's index or not, default True
        :param ndx_lvl: DataFrame.reset_index(level=prm), default None
        :return: None
        """
        if dts is None or type(dts) in self.lst_typ_dts:
            self.__dts = dts
            self.__attr_rst('dts', ndx_rst=ndx_rst, ndx_lvl=ndx_lvl)
        else:
            raise TypeError('info: dts\'s type %s is not available.' % type(dts))

    @property
    def lcn(self):
        return self.__lcn

    @lcn.setter
    def lcn(self, lcn):
        if lcn is None or type(lcn) in self.lst_typ_lcn:
            if self.__lcn is None:
                self.__lcn = lcn
            else:
                for ikey in lcn.keys():
                    self.__lcn[ikey] = lcn[ikey]
            self.__attr_rst('lcn')
        else:
            raise TypeError('info: lcn\'s type %s is not available.' % type(lcn))

    def mng_nit(self):
        """
        if self.io type in mongodb, reset mongo attributes _myMng, _mySdb, _myCln.
        :return: None
        """
        if 'mng' not in self.lcn.keys():
            self.lcn['mng'] = None
        self.lcn['mng'] = "mongodb://localhost:27017" if not self.lcn['mng'] else self.lcn['mng']
        self._myMng = MongoClient(host=self.lcn['mng'])
        self._myMdb = self._myMng[self.lcn['mdb']] if [True if 'mdb' in self.lcn.keys() else False] else None
        self._myCln = self._myMdb[self.lcn['cln']] if [True if 'cln' in self.lcn.keys() else False] else None

    def sql_nit(self):
        if 'sql' not in self.lcn.keys():
            self.lcn['sql'] = None
        self.lcn['sql'] = {'hst': '172.16.0.13', 'prt': 3306, 'usr': None, 'psw': None} if \
            not self.lcn['sql'] else self.lcn['sql']
        self._mySql = self.lcn['sql'] if [True if 'sql' in self.lcn.keys() else False] else None
        self._mySdb = self.lcn['sdb'] if [True if 'sdb' in self.lcn.keys() else False] else None
        self._myTbl = self.lcn['tbl'] if [True if 'tbl' in self.lcn.keys() else False] else None

    def api_nit(self):
        if 'pst' not in self.lcn.keys():
            self.lcn['pst'] = None
        if 'hdr' not in self.lcn.keys():
            self.lcn['hdr'] = None

    def dts_nit(self, ndx_rst=True, ndx_lvl=None):
        """
        dataset initiate, generate attributes typ, len, kys, vls, clm, hdr, tal and if reset index or not.
        :param ndx_rst: if reset index or not, default True
        :param ndx_lvl: if reset index, set the level of index
        :return: None
        """
        lst_xcp = []
        try:
            self.typ = type(self.__dts)
        except TypeError:
            lst_xcp.append('type')
        try:
            self.len = self.dts.__len__()
        except AttributeError:
            lst_xcp.append('len')
        try:
            self.kys = self.dts.keys()
        except (AttributeError, TypeError):
            lst_xcp.append('keys')
        try:
            self.vls = self.dts.values()
        except (AttributeError, TypeError):
            lst_xcp.append('values')
        self.clm = self.dts.columns if self.typ in [typ_pd_DataFrame] else self.clm
        self.hdr = self.dts.head() if self.typ in [typ_pd_DataFrame] else self.hdr
        self.tal = self.dts.tail() if self.typ in [typ_pd_DataFrame] else self.tal
        try:
            if ndx_rst:
                self.dts.reset_index(drop=True, inplace=True, level=ndx_lvl)
        except AttributeError:
            lst_xcp.append('resetIndex')
        if not lst_xcp:
            print('info: %s is not available for %s.' % (str(lst_xcp), str(self.__dts)[:8] + '..'))

    def lcn_nit(self):
        """
        location initiate, let self.iot in ['lcl','mng','sql','api'] for [local, mongodb, sql, api].
        :return: None
        """
        self.iot = []
        if [True for i in self.lcn.keys() if i in ['fld', 'fls']] == [True, True]:
            self.iot.append('lcl')
        if [True for i in self.lcn.keys() if i in ['sdb']] == [True]:
            self.iot.append('sql')
        if [True for i in self.lcn.keys() if i in ['mdb', 'cln']] == [True, True]:
            self.iot.append('mng')
        if [True for i in self.lcn.keys() if i in ['url']] == [True]:
            self.iot.append('api')
        if not self.iot:
            print(' info: <.lcn: %s> is not available.' % self.lcn)

    def __attr_rst(self, typ=None, *, ndx_rst=True, ndx_lvl=None):
        """
        reset attributes lsz.typ.
        :param typ: type of attributes resets, in ['dts','lcn'] for data set reset and location reset
        :return: None
        """
        if typ in ['dts', None]:
            self.dts_nit(ndx_rst, ndx_lvl)
        if typ in ['lcn', None]:
            self.lcn_nit()
            if [True for i in self.iot if i in ['mng', 'mnz']]:  # for special cases, reset some attributes
                self.mng_nit()
            if [True for i in self.iot if i in ['sql', 'sqz']]:
                self.sql_nit()
            if [True for i in self.iot if i in ['api', 'apz']]:
                self.api_nit()

    def typ_to_dtf(self, clm=None, *, spr=False, rtn=False):
        if self.len == 0 or not self.dts:
            self.dts = pd_DataFrame()
        elif self.typ in [dict, dcz]:
            self.dts = pd_DataFrame([self.dts])
        elif self.typ in [list, lsz, typ_np_ndarray]:
            self.dts = pd_DataFrame(self.dts, columns=clm)
        elif self.typ in [typ_pd_Series]:
            self.dts = pd_DataFrame(self.dts)
        # from sas7bdat import SAS7BDAT as typ_sas7bdat
        # elif self.typ in [typ_sas7bdat]:  # https://pypi.org/project/sas7bdat/
        #     self.dts = self.dts.to_data_frame()
        elif self.typ in [typ_pd_DataFrame]:
            pass
        else:
            raise AttributeError('type of dts is not available')
        if spr:
            self.spr_nit()
        if rtn:
            return self.dts

    def dtf_to_typ(self, typ='list', *, rtn=False, prm='records'):
        """
        alter dataFrame to other type.
        :param typ: alter type dataFrame to this type, default 'list'
        :param rtn: return the result or not, default False
        :param prm: to_dict(orient=''), default 'records' in ['records','dict','list','series','split','index']
        :return: if rtn is True, return the result
        """
        if typ.lower() in ['list', 'lst', 'lsz'] and self.typ in [typ_pd_DataFrame]:
            self.dts = self.dts.to_dict(orient=prm)
        else:
            raise AttributeError('stop: type is not available')
        if rtn:
            return self.dts


class lclMixin(ioBsc):
    """
    local files input and output operations.
    lcn format in {'fld':'','fls':['','',...],'mng':None,'mdb':}.
    >>> lclMixin(lcn={'fld':'dst/samples','fls':['smp01.xlsx']}).lcl_mpt(rtn=True) # 从指定文件夹位置导入文件到内存
       A  B  C
    0  a  1  e
    1  b  2  f
    2  c  3  g

    """
    def __init__(self, dts=None, lcn=None, *, spr=False):
        super(lclMixin, self).__init__(dts, lcn, spr=spr)

    def mpt_csv(self, fld=None, fls=None, sep=None, *, spr=False, rtn=False):
        fld = self.lcn['fld'] if fld is None else fld
        fls = self.lcn['fls'] if fls is None else fls
        sep = ',' if sep is None else sep
        self.dts = read_csv(path.join(fld, fls), sep=sep)
        if spr:
            self.spr_nit()
        if rtn:
            return self.dts

    def mpt_xcl(self, fld=None, fls=None, hdr=None, sht=None, *, spr=False, rtn=False):
        fld = self.lcn['fld'] if fld is None else fld
        fls = self.lcn['fls'] if fls is None else fls
        hdr = 0 if hdr is None else hdr
        sht = 0 if sht is None else sht
        self.dts = read_excel(path.join(fld, fls), header=hdr, sheet_name=sht)
        if spr:
            self.spr_nit()
        if rtn:
            return self.dts

    def mpt_txt(self, fld=None, fls=None, *, spr=False, rtn=False):
        pass

    def lcl_mpt(self, *, sep=None, hdr=None, sht=None, spr=False, rtn=False):
        if type(self.lcn['fls']) is str:    # 对可能存在的'fls'对多个文件的情况进行统一
            self.lcn = {'fls': [self.lcn['fls']]}
        dtf_mrg = pd_DataFrame() if not self.dts else self.dts  # 初始化数据框存放多个文件, 若self.dts有值则要求为数据框
        for i_fls in self.lcn['fls']:
            if i_fls.rsplit('.')[1] in ['csv']:
                self.mpt_csv(fls=i_fls, sep=sep)
            elif i_fls.rsplit('.')[1] in ['xls', 'xlsx']:
                self.mpt_xcl(fls=i_fls, hdr=hdr, sht=sht)
            elif i_fls.rsplit('.')[1] in ['txt']:
                pass
            dtf_mrg = concat([dtf_mrg, self.dts], ignore_index=True)    # 忽视index的多文件纵向拼接
        self.dts = dtf_mrg
        if spr:
            self.spr_nit()
        if rtn:
            return self.dts

    def xpt_txt(self, *, typ='w', sep=2, cvr=True):
        """
        import dataset to file.txt or file.js(https://blog.csdn.net/matrix_google/article/details/76861485).
        :param typ: type of open, usually in ['a','w'], a for continue, w for cover
        :param sep: cut how many units in the head and tail of each row, default 2 to be compatible with echarts
        :param cvr: check if the pth_txt is already exists or not, False means if it exists, then do nothing
        :return: None
        """
        if exists(path.join(self.lcn['fld'], self.lcn['fls'])) and cvr is False:
            print("stop: the txt %s already exists." % str(self.lcn.values()))
        elif type(self.dts) not in [list, typ_pd_DataFrame]:
            print('stop: type of dts_npt needs [list, pd.DataFrame].')
        else:
            if self.typ is list:
                lst_dts_mpt = [str(i_dcm) for i_dcm in self.dts]
            else:  # alter the type of a line in dataframe from slice to str
                lst_dts_mpt = [str(i_dcm)[sep: -sep] for i_dcm in self.dts.to_dict(orient='split')['data']]
            prc_txt_writer = open(path.join(self.lcn['fld'], self.lcn['fls']), typ, encoding='utf-8')
            for i in range(len(self.dts)):
                prc_txt_writer.writelines(lst_dts_mpt[i])
                prc_txt_writer.write('\n')
            prc_txt_writer.close()
            print("info: success input dts to txt %s." % str(self.lcn.values()))

    def lcl_xpt(self, *, typ='w', sep=2, cvr=True, ndx=False, prm='sheet1'):
        """
        local exporting, type in lcn['fls']
        :param typ: exporting type in ['w','a'] if lcn['fls'] in ['.js','.txt'], default 'w' means cover the old file
        :param sep: cut how many units in the head and tail of each row, default 2 to be compatible with echarts
        :param cvr: check if the pth_txt is already exists or not, False means if it exists, then do nothing
        :param ndx: remain index in the first line if lcn['fls'] in ['.xlsx'], default False
        :param prm: if typ='a' and fls in type '.xlsx', continue to write the excel, use prm to define the sheet name
        :return: None
        """
        if self.lcn['fls'].rsplit('.')[1] in ['xlsx'] and typ in ['w'] and cvr:
            self.dts.to_excel(path.join(self.lcn['fld'], self.lcn['fls']), index=ndx)
        elif self.lcn['fls'].rsplit('.')[1] in ['xlsx'] and (typ in ['a'] or not cvr):
            writer = ExcelWriter(path.join(self.lcn['fld'], self.lcn['fls']))
            self.dts.to_excel(writer, sheet_name=prm, index=ndx)
            writer.save()
            writer.close()
        elif self.lcn['fls'].rsplit('.')[1] in ['csv']:
            self.dts.to_csv(path.join(self.lcn['fld'], self.lcn['fls']), encoding='UTF-8_sig')    # 不明原因的解码方式
        elif self.lcn['fls'].rsplit('.')[1] in ['js', 'txt']:
            self.xpt_txt(typ=typ, sep=sep, cvr=cvr)
        else:
            print('stop: known exporting type.')


class mngMixin(ioBsc):
    """
    local files input and output operations.
    >>> from pandas import DataFrame
    >>> tst = ioz(dts=DataFrame([{'A':'a','B':1.0},{'A':'b','B':2.0}]),lcn={'mdb':'db_tst','cln':'cl_tst'})
    >>> tst.mng_xpt()
    ***** db_tst . cl_tst *****
    info: 2 inserted, 0 updated, 0 dropped.
    >>> tst.mng_mpt(rtn=True)
    [{'_id': ObjectId('5e48f0ae8daa9bfcc3c0987f'), 'A': 'a', 'B': 1.0},
     {'_id': ObjectId('5e48f0ae8daa9bfcc3c09880'), 'A': 'b', 'B': 2.0}]
    """
    def __init__(self, dts=None, lcn=None, *, spr=False):
        super(mngMixin, self).__init__(dts, lcn, spr=spr)
        self._mpt_id = None

    def mng_nfo(self, typ=None, *, prm=None, dct_qry=None, lst_clm=None):
        """
        export mongodb connection information.
        :param typ: which information is needed, in ['dbs','cln','clm','dcm','vls','max','min']
        :param prm: if typ is 'dcm', insert a certain column name for all the unique values in this column
        :param dct_qry: only referred when typ in ['vls','max','min'] for documents' choosing
        :param lst_clm: only referred when typ in ['vls','max','min'] for columns' choosing
        :return: a list of target values
        """
        if typ in ['dbs', 'database', 'mdb', 'mongodatabase']:
            return self._myMng.list_database_names()
        elif typ in ['cln', 'collection']:
            return self._myMdb.list_collection_names()
        elif typ in ['clm', 'column', 'columns']:
            return list(self._myCln.find_one().keys())
        elif typ in ['dcm', 'document', 'documents']:
            return self._myCln.distinct(prm)
        elif typ in ['vls', 'values', 'max', 'min']:
            flt_typ = -1 if typ in ['max'] else 1
            dct_qry = {} if not dct_qry else dct_qry
            dct_clm = dcz().typ_to_dct(lst_clm, 1, rtn=True) if lst_clm else None
            try:                # 当对目标collection的find无异常时
                mtx_stt = self._myCln.find(dct_qry, dct_clm).sort([(prm, flt_typ)]).limit(100)
                lst_xpt = []
                for dct_xpt in mtx_stt:
                    if dct_xpt not in lst_xpt:
                        lst_xpt.extend([dct_xpt])
                if lst_xpt:
                    return lst_xpt[0][prm]
                else:           # 当目标collection的find未找到document时返回空
                    return None
            except KeyError:    # 当目标collection导出失败时返回空
                return None
        else:
            return [self._myMdb.name, self._myCln.name, list(self._myCln.find_one().keys())]

    def crt_ndx(self, lst_ndx, unq=False, drp=True, srt=1):
        """
        当选定unique indexs时，索引在本集合中唯一，重复插入报错KeyError
        :param lst_ndx: 索引列表，也可以为想要添加为复合索引的列名的list, 最终自动转化为[(,1),(,1),...]形式
        :param unq: 单个索引或复合索引在本集合中是否唯一，若是则重复插入的同索引将报错KeyError，默认False为可重复
        :param drp: 在生成新索引前是否删除全部现有的索引，默认True为全部删除
        :param srt: 建立索引时各列名为正序（1）或倒序（-1），默认为1即全部正序
        :return: None
        """
        lst_ndx = lsz(lst_ndx).lst_to_typ('listtuple', srt, rtn=True)   # return [('A',1),('B',1)]
        if drp:
            self._myCln.drop_indexes()                                  # if True, delete old indexes
        self._myCln.create_index(lst_ndx, unique=unq)

    def dlt_cln(self, ask=True):
        """pymongo.drop
        print 'Y' to make sure that the target collection should be dropped.
        :return: None
        """
        if ask:
            if input("delete(y/n): %s\n" % str(self._myCln)).lower() in ["y", "yes", "t", "true"]:
                print("info: deleted: %s\n" % str(self._myCln))
                self._myCln.drop()
            else:
                raise KeyError('stop: nothing happened for unknown input(needs yes/true)')
        else:
            self._myCln.drop()

    def dlt_cln_dcm(self, dct_qry=None, *, rtn=False):
        if dct_qry is None:
            if input("delete all(y/n): %s\n" % str(self._myCln)).lower() not in ["y", "yes", "t", "true"]:
                raise KeyError('nothing will happen for null dct_qry and wrong answer')
        prc_delete_many = self._myCln.delete_many(dct_qry)
        if rtn:
            return prc_delete_many.deleted_count

    def ltr_cln_dcm(self, dct_qry=None, dct_ltr=None):
        prc_update_many = self._myCln.update_many(dct_qry, dct_ltr, upsert=True)
        if prc_update_many.raw_result['updatedExisting']:
            return prc_update_many.raw_result

    def mng_mpt(self, dct_qry=None, lst_clm=None, *, spr=False, rtn=False):
        dct_qry = {} if not dct_qry else dct_qry
        dct_clm = dcz().typ_to_dct(lst_clm, 1, rtn=True) if lst_clm else None
        prc_find = self._myCln.find(dct_qry, dct_clm)
        self.dts = [dct_xpt for dct_xpt in prc_find]
        if spr:
            self.spr_nit()
        if rtn:
            return self.dts

    def xpt_cln_dcm(self, dts_mpt, lst_ndx=None, cvr=True, rtn=False):
        """
        向集合中更新一行pd.Series，若lst_clm_ndx重复则可以选择是否覆盖
        :param dts_mpt: 一行数据，进入集合后视为一个文档，可选类型为[pd.Series, dict]
        :param lst_ndx: 在此集合中选定为索引且unique=True的列名
        :param cvr: 当索引重复时是否用dts_mpt覆盖集合内的行，默认True
        :param rtn: 返回输入的情况
        :return: None
        """
        self._mpt_id = [] if not self._mpt_id else self._mpt_id     # 为后续记录输入文档的id做准备
        if type(dts_mpt) == typ_pd_Series:
            lst_ndx = dts_mpt.index.to_list() if lst_ndx is None else lst_ndx
            dct_mpt = dts_mpt.to_dict()
        elif type(dts_mpt) == dict:
            lst_ndx = list(dts_mpt.keys()) if lst_ndx is None else lst_ndx
            dct_mpt = dts_mpt.copy()
        else:
            raise KeyError("stop: type of dts_mpt needs in [pd.Series, dict]")
        n, c, d = 0, 0, 0
        dct_ndx = {i: dct_mpt[i] for i in lst_ndx}  # 构造与索引同格式的用于核实是否已存在于库中的列字典
        try:
            prc_insert = self._myCln.insert_one(dct_mpt)
            self._mpt_id.append(prc_insert.inserted_id)
            n += 1
        except DuplicateKeyError:
            dct_mpt.pop('_id')
            if cvr and len(dct_mpt) > 0:
                self._myCln.update_one(dct_ndx, {'$set': dct_mpt})
                c += 1
            else:
                d += 1
        if rtn:
            return n, c, d

    def xpt_cln_dtf(self, lst_ndx=None, cvr=True):
        """
        逐行更新，保持集合中lst_ndx唯一
        :param lst_ndx: 在此集合中选定为索引且unique=True的列名
        :param cvr: 当索引重复时是否用dts_mpt覆盖集合内的行，默认True
        :return: None
        """
        nt, ct, dt = 0, 0, 0
        for i in range(self.len):
            n, c, d = self.xpt_cln_dcm(self.dts.loc[i, :], lst_ndx, cvr, True)
            nt += n
            ct += c
            dt += d
        print('info: %i inserted, %i updated, %i dropped.\n' % (nt, ct, dt))

    def mng_xpt(self, lst_ndx=None, cvr=True):
        """import from dataframe to collections
        根据库是否存在, 进行有选择性的导入策略,全部导入或更新导入
        :param lst_ndx: 为新建的集合创建唯一索引组
        :param cvr: 覆盖或是舍弃, default cover, True
        :param dtz_nit: 初始化的窗口起点
        :param dtz_bgn: 本次操作所选择的时间窗起点，当此起点与初始化窗口起点相同时，采取创建策略
        :return: None
        """
        print('*****', self._myMdb.name, '.', self._myCln.name, '*****')
        # estimated_document_count/count_documents instead count
        if self._myCln.estimated_document_count() == 0:
            self.xpt_cln_dtf()
            if lst_ndx is not None:
                self.crt_ndx(lst_ndx, True)
        else:
            self.xpt_cln_dtf(lst_ndx, cvr)


class sqlMixin(ioBsc):
    """
    mysql tables input and output operations.
    >>> tst = ioz(lcn={'sql':{'hst':"cdb-cwlc1vtt.gz.tencentcdb.com", 'prt':10109,'usr':'**','psw':'sl********'},
    >>>                'sdb':'db_spd_dly',
    >>>                'tbl':'tb_spd_est_shd_ljw_190409',})
    >>> tst.sql_run("SELECT table_name FROM information_schema.tables WHERE table_schema=%s",tst._mySdb)    # 查看表名
    """
    def __init__(self, dts=None, lcn=None, *, spr=False):
        super(sqlMixin, self).__init__(dts, lcn, spr=spr)

    def sql_run(self, str_sql=None, lst_kys=None, *, spr=False, rtn=False):
        """
        tencent cloud mysql operation
        # create
        str_clm = 'estatename CHAR(36) PRIMARY KEY, ndx TINYINT NOT NULL'
        sql_crt = "CREATE TABLE %s(%s)ENGINE=innodb DEFAULT CHARSET=utf8;"%('tb_spd_bdu_adv_kwd',str_clm)
        # alter - add multi primary key
        sql_ltr = 'ALTER TABLE %s ADD PRIMARY KEY(do_date, estatename, _cty)'%('tb_spd_bdu_adv')
        # insert
        sql_nst = 'insert into %s(%s) values(%s)'%('tb_spd_bdu_adv_kwd','estatename, ndx','"云", "5"')
        # update
        sql_pdt = 'update %s set %s = %s where %s = %s' %('tb_spd_bdu_adv_kwd','estatename','"桑泰龙樾"','estatename','"桑泰龙樾"')
        # delete
        sql_dlt = 'delete from %s where %s=%s'%('tb_spd_bdu_adv_kwd','estatename','"云"')
        # select
        sql_slt = 'select * from %s'%('tb_spd_bdu_adv_kwd')
        # drop
        sql_drp = 'DROP TABLE %s'%('tb_spd_adv_bdu_kwd')
        # show tables' name in database
        >>> "SELECT table_name FROM information_schema.tables WHERE table_schema='%s'" % self.lcn['sdb']
        :param str_sql: sql sentences
        :param lst_kys: the lst_dts in crs.execute(str_sql, lst_dts) for %s without '"' in str_sql
        :param spr: let ioz = ioz.dts or not, default False
        :param rtn: return the result or not, default False
        :return: the target dataset in type DataFrame or None
        """
        cnn = connect(host=self._mySql['hst'], port=self._mySql['prt'],
                      user=self._mySql['usr'], password=self._mySql['psw'],
                      database=self._mySdb, charset="utf8")
        crs = cnn.cursor()
        try:
            crs.execute(str_sql, lst_kys)
            if str_sql[:6].lower() in ['select']:
                dts = crs.fetchall()    # values in type tuple
                clm = crs.description   # columns information
                self.dts = pd_DataFrame(list(dts), columns=[i_clm[0] for i_clm in list(clm)]) if dts else self.dts
            elif str_sql[:6].lower() in ['insert', 'update', 'delete']:
                cnn.commit()
            else:
                raise KeyError('stop: the sentence of sql needs [select, insert, update, delete]')
            if spr:
                self.spr_nit()
            if rtn:
                return self.dts
        except IntegrityError:
            return 'stop: pymysql.IntegrityError'
        except ():
            return 'stop: unknown error.'
        finally:
            crs.close()
            cnn.close()


class apiMixin(ioBsc):
    """
    local files input and output operations.
    >>> self = ioz(lcn={'mdb':'db_tst','cln':'cl_cld',
    >>>     'url':"http://haixfsz.centaline.com.cn/service/postda",
    >>>     'pst':{'uid':'haixf**', 'pwd':'SZ.***$', 'key':'requestCCESdeallog',
    >>>     'ContractDate_from':'2020-01-01','ContractDate_to':'2020-01-02'}})
    >>> self.api_run()  # 从api导入数据
    >>> self.mng_xpt()  # 将api数据导出到mongodb
    """
    def __init__(self, dts=None, lcn=None, *, spr=False):
        super(apiMixin, self).__init__(dts, lcn, spr=spr)

    def api_run(self, *, spr=False, rtn=False):
        mdl_rqt = post(self.lcn['url'], data=self.lcn['pst'], headers=self.lcn['hdr'], timeout=300)
        mdl_rqt.encoding = "utf-8"
        try:
            try:
                self.dts = loads(mdl_rqt.text)
            except JSONDecodeError:
                self.dts = mdl_rqt.text
            if spr:
                self.spr_nit()
            if rtn:
                return self.dts
        except ValueError:
            print(mdl_rqt.text[0:100])
            raise KeyError("cannot load data")

    def get_vls(self, lst_kys, *, spr=False, rtn=False):
        """
        get values from dict by keys in api feedback.
        :param lst_kys:
        :param spr:
        :param rtn:
        :return:
        """
        if lst_kys:
            lst_kys = lsz(lst_kys).typ_to_lst(rtn=True)
            for i in lst_kys:
                try:
                    self.dts = self.dts[i]
                    if spr:
                        self.spr_nit()
                    if rtn:
                        return self.dts
                except KeyError:
                    print(str(self.__dts)[:8]+'..')
                    raise KeyError('%s do not exist' % i)

    def api_mpt(self, lst_kys=None, *, spr=False, rtn=False):
        self.api_run()
        self.get_vls(lst_kys)
        if spr:
            self.spr_nit()
        if rtn:
            return self.dts


class ioz(mngMixin, sqlMixin, apiMixin, lclMixin):
    """
    """
    def __init__(self, dts=None, lcn=None, *, spr=False):
        super(ioz, self).__init__(dts, lcn, spr=spr)
