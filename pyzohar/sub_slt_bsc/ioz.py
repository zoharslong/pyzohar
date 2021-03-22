#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 22 16:30:38 2019
input/output operation.
@author: zoharslong
"""
from base64 import b64encode, b64decode
from numpy import ndarray as typ_np_ndarray
from pandas.core.series import Series as typ_pd_Series                  # 定义series类型
from pandas.core.frame import DataFrame as typ_pd_DataFrame             # 定义dataframe类型
from pandas.core.indexes.base import Index as typ_pd_Index              # 定义dataframe.columns类型
from pandas.core.indexes.range import RangeIndex as typ_pd_RangeIndex   # 定义dataframe.index类型
from pandas.core.groupby.generic import DataFrameGroupBy as typ_pd_DataFrameGroupBy     # 定义dataframe.groupby类型
from pandas import DataFrame as pd_DataFrame, read_csv, read_excel, concat, ExcelWriter
from time import sleep
from datetime import timedelta as typ_dt_timedelta
from os import listdir
from tempfile import gettempdir                                         # 用于搜索fakeuseragent本地temp
from os.path import exists, join as os_join
from openpyxl import load_workbook                                      # 保存已有的excel文件中的表
from fake_useragent import UserAgent, VERSION as fku_version            # FakeUserAgentError,
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from pymysql import connect, IntegrityError
from urllib3.util.retry import MaxRetryError
from urllib3.response import ProtocolError
from urllib3.connection import NewConnectionError
from requests.models import ChunkedEncodingError
from requests.adapters import ProxyError
from requests import post, get, TooManyRedirects, ReadTimeout
from re import findall as re_find, sub as re_sub
from random import randint
from json import loads, JSONDecodeError
from pyzohar.sub_slt_bsc.bsz import stz, lsz, dcz, dtz
# from socket import getfqdn, gethostname  # 获得本机IP  # from telnetlib import Telnet  # 代理ip有效性检测的第二种方法


class ioBsc(pd_DataFrame):
    """
    I/O basic
    ioBsc.lcn in {
        'fld','fls',
        'mng','mdb','cln',
        'sql','sdb','tbl',
        'url'/'url_lst'/'url_ctt','url_htp',
        'hdr','pst','prm',
        'prx',‘prx_tms’,
        'ppc':{
            'key': [],
            'ndx': [],
        },
    }
    """
    lst_typ_dts = [
        str,
        stz,
        list,
        lsz,
        dict,
        dcz,
        tuple,
        bytes,
        typ_np_ndarray,
        typ_pd_DataFrame,
        typ_pd_Series,
        typ_pd_Index,
        typ_pd_RangeIndex,
        typ_pd_DataFrameGroupBy,
        type(None)
    ]   # data sets' type
    lst_typ_lcn = [list, lsz, dict, dcz, type(None)]   # io methods' type

    def __init__(self, dts=None, lcn=None, *, spr=False):
        # all the i/o operations have the same attributes for locate target data: location and collection
        super().__init__()                  # 不将dts预传入DataFrame
        self.__dts, self._dts, self.typ = None, None, None   # 接受数据
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
        """
        super initiation.
        :param rtn: default False
        :return:
        """
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
        dct_prn = {i: self.lcn[i] for i in self.lcn.keys() if i in ['fls', 'cln', 'tbl', 'url']}
        return '<io: %s; ds: %s>' % (str(dct_prn), self.typ)
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
        """
        self.location.
        :return: self.__lcn
        """
        return self.__lcn

    @lcn.setter
    def lcn(self, lcn):
        """
        set self.__lcn in self.lcn.
        :param lcn: a dict of params for self
        :return: None
        """
        if type(lcn) in self.lst_typ_lcn:
            if self.__lcn is None:      # 当self.__lcn为空时, 直接对self__lcn进行赋值
                self.__lcn = lcn
            elif type(lcn) in [dict]:   # 当self.__lcn中已有值时, 使用lcn对其进行更新
                self.__lcn.update(lcn)  # 使用update更新self.__lcn, 要求self.__lcn必为dict类型
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
        """
        SQL initiate. needs self.lcn={'sql'={'hst','prt','usr','psw'},'sdb','tbl'}
        :return: None
        """
        if 'sql' not in self.lcn.keys():
            self.lcn['sql'] = None
        self.lcn['sql'] = {'hst': '172.16.0.13', 'prt': 3306, 'usr': None, 'psw': None} if \
            not self.lcn['sql'] else self.lcn['sql']
        self._mySql = self.lcn['sql'] if [True if 'sql' in self.lcn.keys() else False] else None
        self._mySdb = self.lcn['sdb'] if [True if 'sdb' in self.lcn.keys() else False] else None
        self._myTbl = self.lcn['tbl'] if [True if 'tbl' in self.lcn.keys() else False] else None

    def api_nit(self):
        """
        API initiate. needs self.lcn={'url'/'url_lst'/'url_ctt','pst','hdr','prx','prm'}
        :return:
        """
        # 检查本地fakeUserAgent文件是否存在, 若否则自动创建
        if 'fake_useragent_' + fku_version + '.json' not in listdir(gettempdir()):
            fku = get('https://fake-useragent.herokuapp.com/browsers/' + fku_version, timeout=180)
            with open(os_join(gettempdir(), 'fake_useragent_' + fku_version + '.json'), "w") as wrt:
                wrt.write(fku.text)
        if 'pst' not in self.lcn.keys():
            self.lcn['pst'] = None      # post请求中在请求data中发送的参数数据
        if 'hdr' not in self.lcn.keys():
            self.lcn['hdr'] = {'User-Agent': UserAgent(use_cache_server=False).random}  # 若未指定请求头就现编一个简直可怕
        else:
            self.lcn['hdr'].update({'User-Agent': UserAgent(use_cache_server=False).random})  # 若制定了则自动刷新一次假头
        if 'prx' not in self.lcn.keys():
            self.lcn['prx'] = None      # 是否调用代理
        if 'prm' not in self.lcn.keys():
            self.lcn['prm'] = None      # get请求中后缀于url的参数

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
        if self.typ in [typ_pd_DataFrame]:
            self.clm = self.dts.columns
            self.hdr = self.dts.head()
            self.tal = self.dts.tail()
        self.hdr = self.dts[:5] if self.typ in [list] else self.hdr
        try:
            if ndx_rst:
                self.dts.reset_index(drop=True, inplace=True, level=ndx_lvl)
        except AttributeError:
            lst_xcp.append('resetIndex')
        if not lst_xcp:
            print('info: %s is not available for %s.' % (str(lst_xcp), str(self.__dts)[:8] + '..'))

    def lcn_nit(self, prn=False):
        """
        location initiate, let self.iot in ['lcl','mng','sql','api'] for [local, mongodb, sql, api].
        :return: None
        """
        self.iot = []
        if [True for i in self.lcn.keys() if i in ['fld']] == [True]:
            self.iot.append('lcl')
        if [True for i in self.lcn.keys() if i in ['sdb']] == [True]:
            self.iot.append('sql')
        if [True for i in self.lcn.keys() if i in ['mdb']] == [True]:
            self.iot.append('mng')
        if set([True for i in self.lcn.keys() if re_find('url', i)]) in [{True}]:
            self.iot.append('api')
        if not self.iot and prn:
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
        """
        self.dts's type from others to dataFrame.
        :param clm: define the columns' name in the final dataFrame
        :param spr: super or not, default False
        :param rtn: return or not, default False
        :return: None if not rtn
        """
        if self.typ in [typ_pd_DataFrame]:
            pass
        elif self.len == 0 or self.dts in [None, [], [{}]]:
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
        """
        import csv from folds into RAM.
        :param fld:
        :param fls:
        :param sep:
        :param spr:
        :param rtn:
        :return:
        """
        fld = self.lcn['fld'] if fld is None else fld
        fls = self.lcn['fls'] if fls is None else fls
        fls = fls[0] if type(fls) in [list] else fls
        sep = ',' if sep is None else sep
        self.dts = read_csv(os_join(fld, fls), sep=sep)
        if spr:
            self.spr_nit()
        if rtn:
            return self.dts

    def mpt_xcl(self, fld=None, fls=None, hdr=None, sht=None, *, spr=False, rtn=False):
        """
        import excel data from folds into RAM
        :param fld:
        :param fls:
        :param hdr:
        :param sht:
        :param spr:
        :param rtn:
        :return:
        """
        fld = self.lcn['fld'] if fld is None else fld
        fls = self.lcn['fls'] if fls is None else fls
        fls = fls[0] if type(fls) in [list] else fls
        hdr = 0 if hdr is None else hdr
        sht = 0 if sht is None else sht
        self.dts = read_excel(os_join(fld, fls), header=hdr, sheet_name=sht)
        if spr:
            self.spr_nit()
        if rtn:
            return self.dts

    def mpt_txt(self, fld=None, fls=None, *, spr=False, rtn=False):
        """
        import txt from folds into RAM
        :param fld: target fold
        :param fls: target file
        :param spr:
        :param rtn:
        :return:
        """
        fld = self.lcn['fld'] if fld is None else fld
        fls = self.lcn['fls'] if fls is None else fls
        fls = fls[0] if type(fls) in [list] else fls
        with open(os_join(fld, fls), mode='r', encoding='utf-8') as act:
            self.dts = act.readlines()  # 读出的为列表
        if spr:
            self.spr_nit()
        if rtn:
            return self.dts

    def mpt_img(self, fld=None, fls=None, *, spr=False, rtn=False):
        """
        import image from disc
        :param fld: target fold
        :param fls: target file
        :param spr:
        :param rtn:
        :return: if rtn, return a string in type base64
        """
        fld = self.lcn['fld'] if fld is None else fld
        fls = self.lcn['fls'] if fls is None else fls
        fls = fls[0] if type(fls) in [list] else fls
        with open(os_join(fld, fls), mode='rb') as act:
            self.dts = b64encode(act.read())
        if spr:
            self.spr_nit()
        if rtn:
            return self.dts

    def lcl_mpt(self, *, sep=None, hdr=None, sht=None, spr=False, rtn=False):
        """
        local files importation.
        :param sep:
        :param hdr:
        :param sht:
        :param spr:
        :param rtn:
        :return:
        """
        if type(self.lcn['fls']) is str:    # 对可能存在的'fls'对多个文件的情况进行统一
            self.lcn = {'fls': [self.lcn['fls']]}
        if type(self.dts) in [typ_pd_DataFrame] and self.len == 0:
            dtf_mrg = pd_DataFrame()
        elif self.dts in [None, []]:
            dtf_mrg = pd_DataFrame()
        else:
            dtf_mrg = self.dts.copy()
        for i_fls in self.lcn['fls']:
            if i_fls.rsplit('.')[1] in ['csv']:
                self.mpt_csv(fls=i_fls, sep=sep)
            elif i_fls.rsplit('.')[1] in ['xls', 'xlsx']:
                self.mpt_xcl(fls=i_fls, hdr=hdr, sht=sht)
            elif i_fls.rsplit('.')[1] in ['txt']:
                self.mpt_txt(fls=i_fls)
                self.typ_to_dtf()
            elif i_fls.rsplit('.')[1] in ['jpeg', 'jpg', 'png']:
                self.mpt_img(fls=i_fls)
                self.dts = pd_DataFrame([self.dts], columns=[i_fls.rsplit('.')[1]])
            dtf_mrg = concat([dtf_mrg, self.dts], ignore_index=True)    # 忽视index的多文件纵向拼接, prm: sort=False
        self.dts = dtf_mrg
        if spr:
            self.spr_nit()
        if rtn:
            return self.dts

    def xpt_txt(self, fld=None, fls=None, *, typ='w', sep=2, cvr=True):
        """
        import dataset to file.txt or file.js(https://blog.csdn.net/matrix_google/article/details/76861485).
        :param fld: fold location
        :param fls: files name
        :param typ: type of open, usually in ['a','w'], a for continue, w for cover
        :param sep: cut how many units in the head and tail of each row, default 2 to be compatible with echarts
        :param cvr: check if the pth_txt is already exists or not, False means if it exists, then do nothing
        :return: None
        """
        fld = self.lcn['fld'] if fld is None else fld
        fls = self.lcn['fls'] if fls is None else fls
        fls = fls[0] if type(fls) in [list] else fls
        if exists(os_join(self.lcn['fld'], fls)) and cvr is False:
            print("stop: the txt %s already exists." % str(self.lcn.values()))
        elif type(self.dts) not in [str, list, typ_pd_DataFrame]:
            print('stop: type of dts_npt needs [str, list, pd.DataFrame].')
        else:
            self.dts = [self.dts] if self.typ in [str] else self.dts
            if self.typ is list:
                lst_dts_mpt = [str(i_dcm) for i_dcm in self.dts]
            else:  # alter the type of a line in dataframe from slice to str
                lst_dts_mpt = [str(i_dcm)[sep: -sep] for i_dcm in self.dts.to_dict(orient='split')['data']]
            prc_txt_writer = open(os_join(fld, fls), typ, encoding='utf-8')
            for i in range(len(self.dts)):
                if typ in ['a', 'A']:
                    prc_txt_writer.write('\n')
                prc_txt_writer.writelines(lst_dts_mpt[i])
            prc_txt_writer.close()
            print("info: success input dts to txt %s." % str(self.lcn.values()))

    def lcl_xpt(self, *, typ='w', sep=2, cvr=True, ndx=False, prm='sheet1', fld=None, fls=None):
        """
        local exporting, type in lcn['fls']
        :param typ: exporting type in ['w','a'] if lcn['fls'] in ['.js','.txt'], default 'w' means cover the old file
        :param sep: cut how many units in the head and tail of each row, default 2 to be compatible with echarts
        :param cvr: check if the pth_txt is already exists or not, False means if it exists, then do nothing
        :param ndx: remain index in the first line if lcn['fls'] in ['.xlsx'], default False
        :param prm: if typ='a' and fls in type '.xlsx', continue to write the excel, use prm to define the sheet name
        :param fld: default None, use self.lcn['fld']
        :param fls: default None, use self.lcn['fls']
        :return: None
        """
        if fld is None:
            fld = self.lcn['fld']
        if fls is None:
            fls = self.lcn['fls'][0] if type(self.lcn['fls']) in [list] else self.lcn['fls']
        if fls.rsplit('.')[1] in ['xlsx'] and typ in ['w'] and cvr:
            self.dts.to_excel(os_join(fld, fls), sheet_name=prm, index=ndx)
        elif fls.rsplit('.')[1] in ['xlsx'] and (typ in ['a'] or not cvr):
            writer = ExcelWriter(os_join(fld, fls), engine='openpyxl')
            try:
                book = load_workbook(os_join(fld, fls))     # 尝试保存已有的文件内容
                writer.book = book
            except FileNotFoundError:
                pass
            self.dts.to_excel(writer, sheet_name=prm, index=ndx)
            writer.save()
            writer.close()
        elif fls.rsplit('.')[1] in ['csv']:
            self.dts.to_csv(os_join(fld, fls), encoding='UTF-8_sig')    # 不明原因的解码方式
        elif fls.rsplit('.')[1] in ['js', 'txt']:
            self.xpt_txt(typ=typ, sep=sep, cvr=cvr)
        elif fls.rsplit('.')[1] in ['png', 'jpeg', 'jpg']:              # 图片文件导出到硬盘
            if type(self.dts) in [bytes]:
                self_dts = b64decode(self.dts)
            elif type(self.dts) in [typ_pd_DataFrame]:
                self_dts = b64decode(self.dts.iloc[0, 0])
            else:
                self_dts = b64decode(self.dts[0])
            with open(os_join(fld, fls), 'wb') as act:
                act.write(self_dts)
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
        >>> dfzMng = mngMixin(lcn={'mdb':'dbs_tst','cln':'cln_tst'})
        >>> dfzMng.mng_nfo('max', prm='x_tst') # 返回prm列的最大值
        '20w13'
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
        @param lst_ndx:
        @param unq:
        @param drp:
        @param srt:
        """
        lst_ndx = lsz(lst_ndx).lst_to_typ('listtuple', srt, rtn=True)   # return [('A',1),('B',1)]
        if drp:
            self._myCln.drop_indexes()                                  # if True, delete old indexes
        self._myCln.create_index(lst_ndx, unique=unq)

    def dlt_cln(self, ask=True):
        """pymongo.drop
        print 'Y' to make sure that the target collection should be dropped.
        :return: None
        @param ask:
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
        """

        @param dct_qry:
        @param rtn:
        @return:
        """
        if dct_qry is None:
            if input("delete all(y/n): %s\n" % str(self._myCln)).lower() not in ["y", "yes", "t", "true"]:
                raise KeyError('nothing will happen for null dct_qry and wrong answer')
        prc_delete_many = self._myCln.delete_many(dct_qry)
        if rtn:
            return prc_delete_many.deleted_count

    def ltr_cln_dcm(self, dct_qry=None, dct_ltr=None):
        """

        @param dct_qry:
        @param dct_ltr: needs '$set', '$inc'
        @return:
        """
        prc_update_many = self._myCln.update_many(dct_qry, dct_ltr, upsert=True)
        if prc_update_many.raw_result['updatedExisting']:
            return prc_update_many.raw_result

    def mng_mpt(self, dct_qry=None, lst_clm=None, *, prm='', spr=False, rtn=False):
        """
        import data from mongodb to RAM.
        :param dct_qry: a dict of query
        :param lst_clm: default not import _id
        :param prm: match 'onepiprcrdtf' for find_one, aggregate, return recuration, return dataframe
        :param spr: up to self, default False
        :param rtn: return the result or not, default False
        :return: None
        """
        dct_qry = {} if not dct_qry else dct_qry
        dct_clm = {'_id': 0}
        if lst_clm is not None:
            dct_clm.update(dcz().typ_to_dct(lst_clm, 1, rtn=True))
        if re_find('one', prm):
            prc_find = [self._myCln.find_one(dct_qry, dct_clm)]
        elif re_find('pip', prm):
            prc_find = self._myCln.aggregate(dct_qry)
        else:
            prc_find = self._myCln.find(dct_qry, dct_clm)
        if re_find('rcr', prm):     # prm in ['rcr']直接返回迭代器
            return prc_find
        else:                       # 否则将全部documents解出存放再self.dts中
            self.dts = [dct_xpt for dct_xpt in prc_find] if prc_find not in [None, [None]] else []
            if prm in ['dtf', 'dataframe']:
                self.typ_to_dtf()
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
        print('info: %i inserted, %i updated, %i dropped.' % (nt, ct, dt))

    def mng_xpt(self, lst_ndx=None, cvr=True):
        """import from dataframe to collections
        根据库是否存在, 进行有选择性的导入策略,全部导入或更新导入
        :param lst_ndx: 为新建的集合创建唯一索引组
        :param cvr: 覆盖或是舍弃, default cover, True
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
    >>> tst = ioz(lcn={'sql':{'hst':"cdb-********.gz.tencentcdb.com", 'prt':1***9,'usr':'**','psw':'**********'},
    >>>                'sdb':'db_spd_dly',
    >>>                'tbl':'tb_spd_est_shd_ljw_190409',})
    >>> tst.sql_run("SELECT table_name FROM information_schema.tables WHERE table_schema=%s", tst.lcn['sdb'])  # 查看表名
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
        sql_pdt = 'update %s set %s = %s where %s = %s' %(
            'tb_spd_bdu_adv_kwd','estatename','"桑泰龙樾"','estatename','"桑泰龙樾"')
        # delete
        sql_dlt = 'delete from %s where %s=%s'%('tb_spd_bdu_adv_kwd','estatename','"云"')
        # select
        sql_slt = 'select * from %s'%('tb_spd_bdu_adv_kwd')
        # drop
        sql_drp = 'DROP TABLE %s'%('tb_spd_adv_bdu_kwd')
        # show tables' name in database
        >>> "SELECT table_name FROM information_schema.tables WHERE table_schema='%s'" % self.lcn['sdb']
        @param str_sql: sql sentences
        @param lst_kys: the lst_dts in crs.execute(str_sql, lst_dts) for %s without '"' in str_sql
        @param spr: let ioz = ioz.dts or not, default False
        @param rtn: return the result or not, default False
        @return: the target dataset in type DataFrame or None
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
            elif str_sql[:6].lower() in ['insert', 'update', 'delete', 'replac']:
                cnn.commit()
            elif str_sql[:6].lower() in ['create']:
                pass
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

    def sql_xpt(self):
        """
        export self.dts into mysql, insert and update when unique keys error
        """
        nt, ct, dt = 0, 0, 0
        str_clm = re_sub("'", '', str(self.clm.tolist()))[1:-1]
        for i_ndx in range(self.len):
            str_ctt = '"'
            for i in range(len(self.clm)):
                str_ctt += self.dts.iloc[i_ndx, :][i]
                str_ctt += '","'
            str_ctt = str_ctt[:-2]
            nt += 1
            rsl = self.sql_run('insert into %s(%s) values(%s)' % (self.lcn['tbl'], str_clm, str_ctt))
            if rsl and re_find('IntegrityError', rsl):
                self.sql_run('replace into %s(%s) values(%s)' % (self.lcn['tbl'], str_clm, str_ctt))
                ct += 1
                nt -= 1
        print('info: %s, %i inserted, %i updated.' % (self.lcn['tbl'], nt, ct))


class apiMixin(ioBsc):
    """
    local files input and output operations.
    >>> self = ioz(lcn={
    >>>     'url':"http://haixfsz.centaline.com.cn/service/postda",
    >>>     'url_htp': 'post',  # 用于制定调用接口的http方法, 默认post, 也可以于此处指定或在self.api_run(prm='post')指定
    >>>     'prx': 'auto',      # 是否开启代理
    >>>     'prx_tms': '%Y-%m-%d %H:%M:%S',         # 更新代理的最后时间
    >>>     'prx_tkn': {'neek':'','appkey':''},     # 极光代理的token
    >>>     'pst':{
    >>>         'uid':'haixf**',
    >>>         'pwd':'**.***$',
    >>>         'key':'requestCCESdeallog',
    >>>         'ContractDate_from':'2020-01-01',
    >>>         'ContractDate_to':'2020-01-02'
    >>>     }, # 用于post方法传参
    >>>     'prm':{},  # 用于get方法传参
    >>>     'hdr':{},
    >>>     'ppc': {
    >>>         'key': [],      # 从返回数据中提取目标数据的key
    >>>     }
    >>> })
    >>> self.api_run()  # 从api导入数据
    >>> self.mng_xpt()  # 将api数据导出到mongodb
    """
    def __init__(self, dts=None, lcn=None, *, spr=False):
        super(apiMixin, self).__init__(dts, lcn, spr=spr)

    def _pi_prx_jgw(self, prm='http', rty=3, dct_wht=None):
        """
        get proxy from jiguang api. 从极光代理获取一个代理ip地址, 同时使用self.lcn['prx_tms']记录这个代理的获取时间
        from http://h.jiguangdaili.com/api/new_api.html; userid 158xxxx9977;
        :param prm: in ['http', 'https']
        :param rty: retry times, default 3
        :param dct_wht: 极光代理的卡密白名单, http://h.jiguangdaili.com/api/new_api.html; userid 158xxxx9977;
        :return: None
        """
        dct_jgw = {
            'url': 'http://d.jghttp.golangapi.com/getip',
            'prm': {
                'num': '1',         # 一次请求的数量
                'type': '2',        # json type
                'pro': '0',         # 440000 - 广东
                'city': '0',        # 440100 - 广州; 441900 - 东莞
                'yys': '0',
                'port': '1',        # 1 for http, 11 for https
                'time': '1',        # [1,2,3] for different available time period
                'ts': '0',
                'ys': '0',
                'cs': '0',
                'lb': '1',
                'sb': '0',
                'pb': '4',
                'mr': '1',
                'regions': '440000',  # 广东省份混拨
                # 'pack': '20855',    # 套餐ID, 无套餐直接使用余额则为空
            },
            'ppc': {'key': ['data']},
        }
        dct_jgw['prm']['port'] = '1' if prm == 'http' else '11'
        dct_wht = self.lcn['prx_tkn'] if 'prx_tkn' in self.lcn.keys() else dct_wht  # 从self.lcn['prx_tkn']获得代理token
        htp = 'http' if prm == 'http' else 'https'
        prc, bch, ipp_prx = True, 0, None
        while prc and bch <= rty:
            if bch == rty:                              # 当运行到第四轮时, 视为运行失败, 直接报错跳出
                raise AttributeError('stop: something wrong with jiguang API, max retry.')
            mdl_prx = get(dct_jgw['url'], params=dct_jgw['prm'], timeout=180)
            mdl_prx.encoding = "utf-8"
            dct_prx = loads(mdl_prx.text)
            try:                                        # 尝试拼接标准格式的proxy
                ipp_prx = htp + '://' + str(dct_prx['data'][0]['ip']) + ':' + str(dct_prx['data'][0]['port'])
                print('info: proxy shifted to %s.' % ipp_prx)
                prc = False
            except (IndexError, KeyError):              # 当API未能返回有效格式的proxy时，尝试将当前机器IP添加白名单来解决这个问题
                if dct_prx['code'] in [113, '113']:     # {"code":113,"data":[],"msg":"请添加白名单xxxx","success":false}
                    url_wht = 'http://webapi.jghttp.golangapi.com/index/index/save_white'
                    dct_wht.update({'white': re_find('请添加白名单(.*$)', dct_prx['msg'])[0]})
                    get(url_wht, params=dct_wht)
                    sleep(3)                            # 添加白名单的操作后需要等待2秒
                elif dct_prx['code'] in [111, '111']:   # {"code":111,"data":[],"msg":"请2秒后再试","success":false}
                    sleep(int(re_find('请(\d+)秒后再试', dct_prx['msg'])[0])+1)
                else:                                   # 当API异常并不是由于白名单问题造成时报错
                    raise KeyError('stop: something wrong with the result <%s>.' % str(mdl_prx.text))
            finally:
                bch += 1
        if ipp_prx:
            self.lcn['prx'] = {htp: ipp_prx}
            self.lcn['prx_tms'] = dtz('now').typ_to_dtt(rtn=True)                               # 定时更换proxy的时间戳
            self.lcn['hdr'].update({'User-Agent': UserAgent(use_cache_server=False).random})    # 在更新proxy的同时更换头

    def _pi_ipl(self, url="http://icanhazip.com/"):
        """
        返回本机当次的ip地址（若self.lcn.prx存在则返回该代理的ip地址）. get local ip from API.
        from https://blog.csdn.net/weixin_44285988/article/details/102837864
        from https://www.cnblogs.com/hankleo/p/11771682.html
        # >>> Telnet('116.22.50.144', '4526', timeout=2)
        <telnetlib.Telnet at 0x1e8dcd8d548>     # 返回一个实例化后的类，说明该代理ip有效，否则返回timeOutError
        :param url: 用于返回本次使用的本机或代理ip的API地址
        :return: ip location in str
        """
        if self.lcn['prx'] in ['auto']:
            raise AttributeError('stop: self.lcn.prx is auto, proxy does not available yet. run api_prx before this.')
        try:
            mdl_rqt = get(url, proxies=self.lcn['prx'], timeout=180)
            if mdl_rqt.status_code == 200:
                return mdl_rqt.text.replace('\n', '')
            elif mdl_rqt.status_code in [402, 403]:
                return re_find("IP\<\/span>: ([0-9.]*?)<", mdl_rqt.text)[0]
            elif mdl_rqt.status_code == 502:
                return 'stop: 502 - connection timed out.'
            else:
                return mdl_rqt
        except (MaxRetryError, NewConnectionError, ConnectionResetError, ProxyError, ReadTimeout):
            return 'stop: MaxRetryError/NewConnectionError.'

    def _pi_prx_chk(self):
        """
        API: if proxies is alright, return True, else return False.
        # >>> Telnet('116.22.50.144', '4526', timeout=2)
        # <telnetlib.Telnet at 0x1e8dcd8d548>     # 返回一个实例化后的类，说明该代理ip有效，否则返回timeOutError
        # >>> # 如上方法的脚本示例
        # >>> try:
        # >>>     if Telnet(re_find('http://(.*?):.*$', self.lcn['prx'][list(self.lcn['prx'].keys())[0]])[0],
        # >>>               re_find('http://.*:(.*$)', self.lcn['prx'][list(self.lcn['prx'].keys())[0]])[0],
        # >>>               timeout=30):  # 使用telnet验证代理效果
        # >>>         return True
        # >>> except TimeoutError:
        # >>>     return False
        :return: True or False for a good proxy
        """
        if self.lcn['prx'] in [None, 'auto']:
            return False
        rsp = self._pi_ipl()        # 可能返回字符串, <respons401> 等
        if type(rsp) not in [str]:
            return False            # 非字符串默认无效
        elif re_find(rsp, self.lcn['prx'][list(self.lcn['prx'].keys())[0]]):
            return True             # 当可以在ip地址测试中发现本次挂载的代理ip时则证明代理成功
        else:
            return False            # 其他情况默认无效

    def api_prx(self, prm='http', frc=False, rty=5, dct_jgh=None):
        """
        当self.lcn.prx代理不为空, 即采用代理时, 调用代理是否有效的检查，若无效则更新代理, 更新五次失败则报错
        :param frc: forced refresh, default False means not refresh if ip test is passed
        :param prm: in ['http','https'], default 'http'
        :param rty: retry times, default 2
        :param dct_jgh: 极光代理的卡密, http://h.jiguangdaili.com/api/new_api.html; userid 158xxxx9977;
        :return:
        """
        if self.lcn['prx']:
            bch, dtz_now = 0, dtz('now').typ_to_dtt(rtn=True)
            if frc:
                self._pi_prx_jgw(prm=prm, dct_wht=dct_jgh)       # 强制更新一次proxy
            elif 'prx_tms' in self.lcn.keys() and (dtz_now - self.lcn['prx_tms']).seconds > randint(0, 30) + 60:
                self._pi_prx_jgw(prm=prm, dct_wht=dct_jgh)       # 原代理超时后强制更新一次proxy
            while not self._pi_prx_chk() and bch <= rty:
                if bch == rty:
                    raise AttributeError('stop: max retry, cannot pass proxy test for %s times.' % str(rty))
                self._pi_prx_jgw(prm=prm, dct_wht=dct_jgh)
                bch += 1

    def api_run(self, *, spr=False, rtn=False, prm='post', frc=False, rty=3, dct_jgh=None):
        """
        兼容POST/GET两种方法的调用, self.lcn.pst在post中输入data, self.lcn.prm在get中输入params.
        此处的循环检查只针对网页连接结果反馈不为200的情况, 而不是检查返回的内容是否满足需求.
        from https://www.cnblogs.com/roadwide/p/10804888.html
        :param spr:
        :param rtn:
        :param prm: in ['post', 'get'], the methods of requests, self.lcn['url_htp']
        :param frc: force change proxy or not
        :param rty: retry, if the result is not available, define retry times, default 3.
        :param dct_jgh: 极光代理的卡密, http://h.jiguangdaili.com/api/new_api.html; userid 158xxxx9977;
        :return:
        """
        if 'url' not in self.lcn.keys():
            raise KeyError('stop: url not exist.')
        else:
            self.api_prx(frc=frc, dct_jgh=dct_jgh)  # 初始化更新proxy, 仅用于处理prx='auto'的情况
            prc, bch, mdl_rqt, rty = True, 0, None, 1 if self.lcn['prx'] in [None] else rty
            prm = self.lcn['url_htp'] if 'url_htp' in self.lcn.keys() else prm  # self.lcn优先
            while prc and bch <= rty:
                # 满足本次请求的返回statusCode为200即请求成功, 或循环rty次失败
                try:
                    mdl_rqt = post(
                        self.lcn['url'], params=self.lcn['prm'], data=self.lcn['pst'],
                        headers=self.lcn['hdr'], proxies=self.lcn['prx'], timeout=300
                    ) if prm.lower() in ['post', 'pst'] else get(
                        self.lcn['url'], params=self.lcn['prm'], data=self.lcn['pst'],
                        headers=self.lcn['hdr'], proxies=self.lcn['prx'], timeout=300
                    )                   # 针对POST/GET请求, 分别注意self.lcn.pst/self.lcn/prm参数
                    mdl_rqt.encoding = "utf-8"
                    if mdl_rqt.status_code in [200]:
                        prc = False             # 当不使用代理或网页返回了信息时, 不进行循环
                    else:
                        self.api_prx(frc=frc, dct_jgh=dct_jgh)   # 其他情况酌情更新代理
                except (
                        ProtocolError, ChunkedEncodingError, TooManyRedirects,
                        MaxRetryError, NewConnectionError, ConnectionError, TimeoutError
                ):   # proxy ip timeout, change proxy
                    print('info: ConectionError, retrying.')
                    sleep(25+randint(0, 5))
                    self.api_prx(frc=frc, dct_jgh=dct_jgh)
                finally:
                    bch += 1
                    if bch == rty:              # 当不使用代理时，默认的rty=1
                        self.api_prx(frc=True, dct_jgh=dct_jgh)  # 当本次POST/GET请求未能得到200且为最后一次运行时, 代理强制更新
                        print('info: max batches, proxy forces switch.')
            # 请求阶段结束, 进入API结果的装载阶段
            try:                                # 优先尝试js解码
                self.dts = loads(mdl_rqt.text)
            except JSONDecodeError:             # 若js解码失败则直接装载self.dts
                self.dts = mdl_rqt.text
            if spr:
                self.spr_nit()
            if rtn:
                return self.dts

    def get_vls(self, lst_kys=None, *, spr=False, rtn=False):
        """
        get values from dict by keys in api feedback.
        :param lst_kys:
        :param spr:
        :param rtn:
        :return:
        """
        if lst_kys:
            lst_kys, i = lsz(lst_kys).typ_to_lst(rtn=True), 'initiation'
            try:
                for i in lst_kys:
                    self.dts = self.dts[i]
                if spr:
                    self.spr_nit()
                if rtn:
                    return self.dts
            except (KeyError, AttributeError):
                print(str(self.dts)[:32]+'..')
                raise KeyError('%s do not exist, something wrong with this request.' % i)

    def api_mpt(self, lst_kys=None, *, spr=False, rtn=False, prm='post', frc=False, rty=3, dct_jgh=None):
        """
        import data from API in json decoding.
        :param lst_kys:
        :param spr:
        :param rtn:
        :param prm:
        :param frc: shift proxy ip forced
        :param rty: retry times, default 3
        :param dct_jgh: 极光代理的卡密, http://h.jiguangdaili.com/api/new_api.html; userid 158xxxx9977;
        :return:
        """
        self.api_run(prm=prm, frc=frc, rty=rty, dct_jgh=dct_jgh)
        self.get_vls(lst_kys)
        if spr:
            self.spr_nit()
        if rtn:
            return self.dts

    def api_tkn(self, dly=3600, frc=False, rty=2):
        """
        Get API token from wechat, baiduMap, needs lcn in [lcn_bdu_ocr_tkn, lcn_bdu_img_tkn, lcn_hhs_tkn]
        @param dly: delay seconds, default 259200 for baiduMap and 3600 for wechat
        @param frc: force to update token
        @param rty: max retry times
        @return: token in type str
        """
        dly = 2592000 if self.lcn['fls'] in ['tkn_bdu_ocr.txt', 'tkn_bdu_img.txt'] else dly
        prc, bch = True, 0
        if self.lcn['fls'] in listdir(self.lcn['fld']):   # 当目录中已经存在txt文件时, 读取
            self.mpt_txt()
            self.dts = loads(self.dts[0].replace('\n', ''))
        else:                       # 否则虚拟一个必定超时的时间点
            self.dts = {'tms': '2020-01-01 00:00:01'}
        while prc and bch <= rty:
            if frc or dtz('now').val - dtz(self.dts['tms']).typ_to_dtt(rtn=True) >= typ_dt_timedelta(seconds=dly):
                self.api_run()   # 当强制刷新参数为True或超时时重新调取token
                self.dts.update({'tms': dtz('now').dtt_to_typ(str_fmt='%Y-%m-%d %H:%M:%S', rtn=True)})
                self.lcl_xpt()   # 生成本地存放文件
                frc = False
            else:
                return self.get_vls(self.lcn['ppc']['key'], rtn=True)
            bch += 1


class ioz(mngMixin, sqlMixin, apiMixin, lclMixin):
    """
    input/output class on multiple methods.
    """
    def __init__(self, dts=None, lcn=None, *, spr=False):
        super(ioz, self).__init__(dts, lcn, spr=spr)
