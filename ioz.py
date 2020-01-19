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
from pandas import DataFrame as pd_DataFrame, read_csv, read_excel, concat
from os import path
from os.path import exists
from bsc import stz, lsz, dcz
from socket import getfqdn, gethostname
from pymongo import MongoClient


class ioBsc(pd_DataFrame):
    """
    I/O basic
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
    ]   # data sets' type
    lst_typ_lcn = [list, lsz, dict, dcz]   # io methods' type

    def __init__(self, dts=None, lcn=None, *, spr=False):
        # all the i/o operations have the same attributes for locate target data: location and collection
        super().__init__()                  # 不将dts预传入DataFrame
        self.__dts, self.typ = None, None   # 接受数据
        self.len, self.clm, self.hdr, self.tal = None, None, None, None
        self.kys, self.vls = None, None
        self.__lcn, self.iot = None, None   # 连接信息
        self._myHst, self._myDbs = None, None
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
        self.dts = dts.
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
            self.__lcn = lcn
            self.__attr_rst('lcn')
        else:
            raise TypeError('info: lcn\'s type %s is not available.' % type(lcn))

    def __attr_rst(self, str_typ=None, *, ndx_rst=True, ndx_lvl=None):
        """
        reset attributes lsz.typ.
        :param str_typ: type of attributes resets, in ['dts','lcn'] for data set reset and location reset
        :return: None
        """
        if str_typ in ['dts', None]:
            try:
                self.typ = type(self.__dts)
            except TypeError:
                print('info: %s is not available for type().' % (str(self.__dts)[:8]+'..'))
            try:
                self.len = self.dts.__len__()
            except AttributeError:
                print('info: %s is not available for __len__().' % (str(self.__dts)[:8]+'..'))
            try:
                self.kys = self.dts.keys()
            except AttributeError:
                print('info: %s is not available for keys().' % (str(self.__dts)[:8]+'..'))
            try:
                self.vls = self.dts.values()
            except AttributeError:
                print('info: %s is not available for values().' % (str(self.__dts)[:8]+'..'))
            self.clm = self.dts.columns if self.typ in [typ_pd_DataFrame] else self.clm
            self.hdr = self.dts.head() if self.typ in [typ_pd_DataFrame] else self.hdr
            self.tal = self.dts.tail() if self.typ in [typ_pd_DataFrame] else self.tal
            try:
                if ndx_rst:
                    self.dts.reset_index(drop=True, inplace=True, level=ndx_lvl)
            except AttributeError:
                print('info: %s is not available for reset_index().' % (str(self.__dts)[:8]+'..'))
        if str_typ in ['lcn', None]:    # in ['lcz','mnz','sqz','apz'] for [local, mongodb, sql, api]
            if [True for i in self.lcn.keys() if i in ['fld', 'fls']] == [True, True]:
                self.iot = 'lcz'
            elif [True for i in self.lcn.keys() if i in ['sql', 'dbs', 'tbl']] == [True, True, True]:
                self.iot = 'sqz'
            elif [True for i in self.lcn.keys() if i in ['mng', 'dbs', 'cln']] == [True, True, True]:
                self.iot = 'mnz'
            elif [True for i in self.lcn.keys() if i in ['url', 'prm']] == [True, True]:
                self.iot = 'apz'
            else:
                raise KeyError('stop: keys in %s is not available.' % self.lcn)

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


class lczMixin(ioBsc):
    """
    local files input and output operations.
    lcn format in {'fld':'','fls':['','',...]}.
    >>> lczMixin(lcn={'fld':'D:/','fls':['fgr.xlsx','fgr.xlsx']}).lcz_mpt(rtn=True) # 从指定文件夹位置导入文件到内存
    """
    def __init__(self, dts=None, lcn=None, *, spr=False):
        super(lczMixin, self).__init__(dts, lcn, spr=spr)

    def mpt_csv(self, fld=None, fls=None, sep=None, *, spr=False, rtn=False):
        fld = self.lcn['fld'] if fld is None else fld
        fls = self.lcn['fls'] if fls is None else fls
        sep = "','" if sep is None else sep
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

    def lcz_mpt(self, *, sep=None, hdr=None, sht=None, spr=False, rtn=False):
        if type(self.lcn['fls']) is str:    # 对可能存在的'fls'对多个文件的情况进行统一
            self.lcn['fls'] = [self.lcn['fls']]
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

    def xpt_txt(self, typ='w', sep=2, cvr=True):
        """
        import dataset to file.txt or file.js(https://blog.csdn.net/matrix_google/article/details/76861485).
        :param typ: type of open, usually in ['a','w'], a for continue, w for cover
        :param sep: cut how many units in the head and tail of each row, default 2 to be compatible with echarts
        :param cvr: check if the pth_txt is already exists or not, False means if it exists, then do nothing
        :return: None
        """
        if exists(path.join(*self.lcn.values())) and cvr is False:
            print("stop: the txt %s already exists\n" % str(self.lcn.values()))
        elif type(self.dts) not in [list, typ_pd_DataFrame]:
            print('stop: type of dts_npt needs [list, pd.DataFrame]')
        else:
            if self.typ is list:
                lst_dts_mpt = [str(i_dcm) for i_dcm in self.dts]
            else:  # alter the type of a line in dataframe from slice to str
                lst_dts_mpt = [str(i_dcm)[sep: -sep] for i_dcm in self.dts.to_dict(orient='split')['data']]
            prc_txt_writer = open(path.join(*self.lcn.values()), typ, encoding='utf-8')
            for i in range(len(self.dts)):
                prc_txt_writer.writelines(lst_dts_mpt[i])
                prc_txt_writer.write('\n')
            prc_txt_writer.close()
            print("info: success input dts to txt %s\n" % str(self.lcn.values()))

    def lcz_xpt(self, *, typ='w', sep=2, cvr=True):
        if self.lcn['fls'].rsplit('.')[1] in ['xlsx']:
            self.dts.to_excel(path.join(*self.lcn.values()))
        if self.lcn['fls'].rsplit('.')[1] in ['csv']:
            self.dts.to_csv(path.join(*self.lcn.values()))
        elif self.lcn['fls'].rsplit('.')[1] in ['js', 'txt']:
            self.xpt_txt(typ=typ, sep=sep, cvr=cvr)


class mngMixin(ioBsc):
    """
    local files input and output operations.
    """
    def __init__(self, dts=None, lcn=None, *, spr=False):
        super(mngMixin, self).__init__(dts, lcn, spr=spr)
        self._myCln, self._mpt_id = None, None
        if self.iot in ['mng', 'mnz']:
            self.mng_nit()

    def mng_nit(self):
        self.lcn['mng'] = "mongodb://localhost:27017" if not self.lcn['mng'] else self.lcn['mng']
        self._myHst = MongoClient(host=self.lcn['mng'])
        self._myDbs = self._myHst[self.lcn['dbs']] if self.lcn['dbs'] is not None else None
        self._myCln = self._myDbs[self.lcn['cln']] if self.lcn['cln'] is not None else None

    def mng_nfo(self):
        return [self._myHst.list_database_names(),
                self._myDbs.list_collection_names(),
                list(self.myCln.find_one().keys())]


class sqlMixin(ioBsc):
    """
    local files input and output operations.
    """
    def __init__(self, dts=None, lcn=None, *, spr=False):
        super(sqlMixin, self).__init__(dts, lcn, spr=spr)


class apiMixin(ioBsc):
    """
    local files input and output operations.
    """
    def __init__(self, dts=None, lcn=None, *, spr=False):
        super(apiMixin, self).__init__(dts, lcn, spr=spr)


class ioz(mngMixin, sqlMixin, apiMixin, lczMixin):
    def __init__(self, dts=None, lcn=None, *, spr=False):
        super(ioz, self).__init__(dts, lcn, spr=spr)


print('info: multiple io\'s alteration ready.')
