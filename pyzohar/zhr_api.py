#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on 19-04-19 15:24:01

@author: sl
"""
from numpy import array                 # list转array便于转置
from pandas import DataFrame, concat    # 多次调用API返回值的拼接，兼容str, float, int拼list和list,dict拼dataframe
from datetime import timedelta as dt_timedelta
from json import loads, dumps, JSONDecodeError
from requests import post
from re import search as re_search
from .zhr_dtf import PrcDtf
from .zhr_ncd import fnc_ncd_str_hsh
from .zhr_bsc import fnc_lst_frm_all, fnc_flt_for_lst_len, fnc_lst_cpy_cll, fnc_dct_mrg_hrl, fnc_tms_frm_all, \
    fnc_dtt_frm_tdy, fnc_dct_frm_lst, fnc_tms_to_thr, fnc_pdt_mng, fnc_slt_dtf_frm_sql, fnc_lst_frm_cll_mrg

# request basic
def fnc_api_pst(str_url, dct_dta, dct_hdr, bln_print=True):
    """
    post api
    :param str_url: target url
    :param dct_dta: content in post data part
    :param dct_hdr: content in post header
    :param bln_print:
    :return: content from the api if success, else raise error
    """
    mdl_rqt = post(str_url, data=dct_dta, headers=dct_hdr, timeout=60)
    mdl_rqt.encoding = "utf-8"
    try:
        dct_rqt = loads(mdl_rqt.text)
        return dct_rqt
    except ValueError:
        if bln_print:
            print(mdl_rqt.text[0:100])
        raise KeyError("cannot load data")
# # ppc
def fnc_api_dct(dct_dts, lst_key=None):
    """
    unload contents from the api in type of dict
    :param dct_dts: the content from the api
    :param lst_key: key of the content in the dict requested from the api
    :return: the target dataset
    """
    if lst_key:
        lst_key = fnc_lst_frm_all(lst_key)
        for i in lst_key:
            try:
                dct_dts = dct_dts[i]
            except KeyError:
                print(dct_dts)
                raise KeyError('%s do not exist' % i)
    return dct_dts
# # run
def fnc_api_rqt(str_url, dct_dta, dct_hdr, lst_key, bln_print=True):
    """
    post and then get data from dict
    :param str_url: target url
    :param dct_dta: content in post data part
    :param dct_hdr: content in post header
    :param lst_key: key of the content in the dict requested from the api
    :param bln_print:
    :return: target data set
    """
    dct_rqr = fnc_api_pst(str_url, dct_dta, dct_hdr, bln_print)
    return fnc_api_dct(dct_rqr, lst_key)

# Process API basic
class PrcApiBsc(PrcDtf):
    """
    processing requesting data from API
    """
    def __init__(self, dtf_mpt=None):
        super(PrcApiBsc, self).__init__(dtf_mpt)
        self.lst_url, self.lst_dta, self.lst_hdr = None, None, None

    def init_api(self, bln_refresh=None):
        """
        initiate API
        :param bln_refresh: 对PrcApiBsc的参数进行更新，默认全部更新
        :return: None
        """
        bln_refresh = [True, True, True, True] if bln_refresh is None else bln_refresh
        bln_refresh = fnc_lst_cpy_cll(bln_refresh, 4)
        self.dts = None if bln_refresh[0] else self.dts
        self.lst_url = None if bln_refresh[1] else self.lst_url
        self.lst_dta = None if bln_refresh[2] else self.lst_dta
        self.lst_hdr = None if bln_refresh[3] else self.lst_hdr
        self.init_rst()

    def add_url(self, *str_prt):
        """
        add url from tuple to list in format ('A',['B','C']) -> [['AB'],['AC']]
        :param str_prt:
        :return:
        """
        self.lst_url = [] if self.lst_url is None else self.lst_url
        flt_len = fnc_flt_for_lst_len(str_prt)                  # 得到元组嵌套列表中最长列表的长度
        lst_prt = []
        for j_prt in str_prt:                                   # 嵌套列表中的个子表复制为等长
            j_prt = fnc_lst_cpy_cll(j_prt, flt_len)
            lst_prt.append(j_prt)
        lst_prt = fnc_lst_frm_all(array(lst_prt).T)             # 嵌套列表行转列
        self.lst_url = [''.join(i_prt) for i_prt in lst_prt]    # 嵌套列表内层字符串横拼

    def add_dta(self, bln_dmp=False, *dct_prt):
        """
        add contents in data part in post
        :param bln_dmp: if type of data needs json.dumps or not, default false
        :param dct_prt: contents of data part in post
        :return: None
        """
        self.lst_dta = [] if self.lst_dta is None else self.lst_dta
        flt_len = fnc_flt_for_lst_len(dct_prt, False)       # 得到元组嵌套列表中最长列表的长度
        lst_prt = []
        for j_prt in dct_prt:                               # 嵌套列表中的个子表复制为等长
            j_prt = fnc_lst_cpy_cll(j_prt, flt_len, False)  # {} -> [{},{},...]
            lst_prt.append(j_prt)                           # [{},{},..]+[{},{},..]+.. -> [[{},{},..],[{},{},..],..]
        lst_prt = fnc_lst_frm_all(array(lst_prt).T)         # [[A,B],[C,D],...] -> [[A,C,..],[B,D,..]]
        for i_prt in lst_prt:                               # [[{A},{C},..],[{B},{D},..]] -> [{A,C,..},{B,D,..}]
            dct_tmp = fnc_dct_mrg_hrl(i_prt)                # [{A},{C},..] -> {A,C,...}
            dct_tmp = dumps(dct_tmp) if bln_dmp else dct_tmp
            self.lst_dta.append(dct_tmp)

    def add_hdr(self, *str_prt):
        """
        add header part in post
        :param str_prt: contents of header part in post
        :return: None
        """
        self.lst_hdr = [] if self.lst_hdr is None else self.lst_hdr
        for i_prt in str_prt:
            self.lst_hdr.append(i_prt)

    def run_rqt(self, lst_key=None, bln_refresh=True, bln_print=True):
        """
        posting when url, data, header are ready
        :param lst_key: the key or index of target data in the posting back
        :param bln_refresh: if delete self.dts, url, dta, hdr or not, default [True, False, False, False]
        :param bln_print:
        :return: None
        """
        self.init_api([True, False, False, False] if bln_refresh is True else False)            # 默认刷新self.dts
        flt_len = fnc_flt_for_lst_len([self.lst_url, self.lst_dta, self.lst_hdr])
        self.lst_url = fnc_lst_cpy_cll(self.lst_url, flt_len)
        self.lst_dta = fnc_lst_cpy_cll(self.lst_dta, flt_len)
        self.lst_hdr = fnc_lst_cpy_cll(self.lst_hdr, flt_len)
        for i in range(flt_len):
            lst_rqt = fnc_api_rqt(self.lst_url[i], self.lst_dta[i], self.lst_hdr[i], lst_key, bln_print)
            if type(lst_rqt) in [str, float, int]:
                self.dts = [lst_rqt] if self.dts is None else self.dts + [lst_rqt]
            elif type(lst_rqt) in [dict, list]:
                lst_rqt = fnc_lst_frm_all(lst_rqt, False)
                self.dts = DataFrame(lst_rqt) if self.dts is None else \
                    concat([self.dts, DataFrame(lst_rqt)], axis=0, ignore_index=True)
        self.init_rst()

    def run_rqt_ccs_to_mng(self, dct_prm, str_end=None, bln_print=False):
        """
        :param dct_prm: API及导入Mongodb的参数字典
        :param str_end: 手工设置的结束日期, 默认为当前日期，格式要求同dct_prm['prd_rgn]
        :param bln_print: 是否打印, default False
        :return: None
        """
        self.add_url("http://haixfsz.centaline.com.cn/service/postda")
        bln_flg, int_prd, str_bgn = True, dct_prm['prd_jmp'][0], dct_prm['prd_rgn']
        str_end = fnc_dtt_frm_tdy(0, 'str', dct_prm['prd_fmt']) if str_end is None else str_end
        while bln_flg and int_prd>0.1:
            try:
                bch_bgn = fnc_tms_frm_all(dct_prm['mng_prc'].stt_clm(dct_prm['prd_clm'], False, None, [dct_prm['prd_clm']])) + \
                          dt_timedelta(seconds=1)
            except IndexError: bch_bgn = fnc_tms_frm_all(str_bgn)
            if bch_bgn > fnc_tms_frm_all(str_end): raise KeyError('stop:str_end needs later than %s.' % bch_bgn)
            bch_end = bch_bgn + dt_timedelta(days=int_prd)
            dct_prd = fnc_dct_frm_lst([dct_prm['prd_clm']+'_from', dct_prm['prd_clm']+'_to'],
                                      [fnc_tms_to_thr(bch_bgn, 'str', dct_prm['prd_fmt']),
                                       fnc_tms_to_thr(bch_end, 'str', dct_prm['prd_fmt'])])
            if bln_print: print(dct_prd, int_prd)
            try:
                self.init_api([True, False, True, True])
                self.add_dta(False, {'uid': 'haixfsz', 'pwd': 'SZ.999$'}, {'key': dct_prm['api_key']}, dct_prd)
                self.run_rqt(dct_prm['api_xpt'], True, False)
                self.add_clm_typ_to_dtt(dct_prm['prd_clm'], None, ['str', 'str'], [None, dct_prm['prd_fmt']])
                self.drp_dcm_na(dct_prm['prd_clm'])
                self.drp_dcm_dpl(dct_prm['mng_ndx'])
                fnc_pdt_mng(dct_prm['mng_prc'], self.dts, dct_prm['mng_ndx'])
                int_prd = round(int_prd * dct_prm['prd_jmp'][1], 2)
                bln_flg = False if bch_end > fnc_tms_frm_all(str_end) else bln_flg  # 能够顺利执行的超结尾导出则证明全局已跑完
            except (JSONDecodeError, KeyError):
                self.init_api([True, False, True, True])
                int_prd = round(int_prd * dct_prm['prd_jmp'][2], 2)

# Process API
class PrcApi(PrcApiBsc):
    """
    pre-processing on data sets from API
    """
    def __init__(self, dct_prm=None, dtf_mpt=None):
        super(PrcApi, self).__init__(dtf_mpt)
        self.dct_prm = dct_prm.copy() if dct_prm is not None else None
    # initiate self.dct_prm
    def add_dct_prm(self, dct_prm=None):
        """
        add dictionary of params into self
        :param dct_prm: dictionary of params
        :return: None
        """
        self.dct_prm = dct_prm if dct_prm is not None else self.dct_prm
    # # export the beginning of the target mongo collection
    def xpt_prd_bgn(self, dct_prm=None):
        """generate the start point of period column
        export the date of operation beginning in type str.
        :param dct_prm: needs key 'mng_prc', 'prd_clm', 'prd_rgn'.
        :return: the date information on beginning.
        """
        self.add_dct_prm(dct_prm)
        try:
            prd_bgn = self.dct_prm['mng_prc'].stt_clm(
                self.dct_prm['prd_clm'], False, None, [self.dct_prm['prd_clm']]
            )
        except IndexError:
            prd_bgn = self.dct_prm['prd_rgn']
        return prd_bgn
    # # delete period gap in self.dct_prm['prd_gap']
    def dlt_prd_gap(self, dct_prm=None):
        """
        delete period gap in target mongo collection
        :param dct_prm:
        :return:
        """
        self.add_dct_prm(dct_prm)
        prd_dlt = fnc_dtt_frm_tdy(-self.dct_prm['prd_gap'],self.dct_prm['prd_typ'],self.dct_prm['prd_fmt'],self.xpt_prd_bgn())
        self.dct_prm['mng_prc'].dlt_dcm({self.dct_prm['prd_clm']:{'$gt':prd_dlt}})
    # # make a judgement on upgrading if last upgrading time is earlier than prd_jmp days
    def bln_pdt(self, dct_prm=None, prd_jmp=2):
        """
        当前日期与collection最晚更新时间的间隔是否大于prd_jmp，用于确定ccs_stf/ccs_est字典是否需要更新的函数内
        :param dct_prm:
        :param prd_jmp:
        :return: boolen on needing update or not
        """
        self.add_dct_prm(dct_prm)
        ddmx = PrcDtf(self.dct_prm['mng_prc'].xpt_cln_to_lst(None, None, True, False))
        try:
            ddmx.add_clm_oid_to_dtt('date')
            ddmx_dmx = ddmx.dts['date'].max()
        except KeyError:
            ddmx_dmx = fnc_dtt_frm_tdy(-(prd_jmp + 1), 'tms')
        return fnc_dtt_frm_tdy(0, 'tms') - fnc_tms_frm_all(ddmx_dmx) >= dt_timedelta(days=prd_jmp)
    # import data set from mysql to mongodb
    def xpt_frm_sql(self, dct_prm=None,bln_new=False):
        """
        export data set from mysql
        :param dct_prm:
        :param bln_new: if refresh the whole mongodb or not, default false not update all
        :return: None
        """
        self.add_dct_prm(dct_prm)
        if bln_new:     # delete all 全局删除重新调取API数据
            self.dct_prm['mng_prc'].drp_cln(False)
        else:           # delete by dct_prm['prd_gap'] 向前回推一定时长以保证收录最近发生变动的记录
            self.dlt_prd_gap()
        # export from mysql cloud
        str_sql = "SELECT %s from %s where %s>=%s" % (
            '*', self.dct_prm['sql_frm'][0], self.dct_prm['prd_clm'],self.xpt_prd_bgn()
        )
        self.add_dts(fnc_slt_dtf_frm_sql(self.dct_prm['sql_frm'][1], str_sql))
        self.typ_dts_to_dtf()
    # export from mongodb db_ccs
    def xpt_frm_mng(self, dct_prm=None,lst_prd=None,lst_clm=None):
        """
        :param dct_prm:
        :param lst_prd: 手工指定的导出时段,若为None则自动按目标mongodb的区段进行导出
        :param lst_clm: 手工指定导出的列名
        :return: None
        """
        self.add_dct_prm(dct_prm)
        lst_prd = fnc_lst_cpy_cll(fnc_lst_frm_all(lst_prd),2)   # if lst_prd is None, build [None,None]
        bgn_prd = self.xpt_prd_bgn() if lst_prd[0] is None else lst_prd[0]
        end_prd = lst_prd[1]
        prd_gte = fnc_dtt_frm_tdy(-self.dct_prm['prd_gap'], self.dct_prm['prd_typ'], self.dct_prm['prd_fmt'], bgn_prd)
        prd_lte = fnc_dtt_frm_tdy(0, self.dct_prm['prd_typ'], self.dct_prm['prd_fmt'],end_prd)
        if self.dct_prm['prd_typ'] =='int':
            prd_gte,prd_lte = str(prd_gte),str(prd_lte)
        tgt_qry = {self.dct_prm['prd_clm']:{'$gte': prd_gte, '$lte': prd_lte}}
        self.add_dts(self.dct_prm['mng_frm'].xpt_cln_to_lst(tgt_qry,lst_clm))
        self.typ_dts_to_dtf()
    # pre-processing on data set from CCES API by self.dct_prm['pre_sqd']
    def ppc_prt_add_dpt(self, str_clm, str_clm_add='dpt', pre_sqd=None):
        """
        对某包含部门信息的列进行关键信息抽离形成新变量dpt，用于ccs_stf和ccs_est的预处理函数中
        :param str_clm:
        :param str_clm_add: default new column name 'dpt'
        :param pre_sqd: 核心部门信息 - 部门详情对应关系字典
        :return: None
        """
        pre_sqd = self.dct_prm['pre_sqd'] if pre_sqd is None else pre_sqd
        for i_key in pre_sqd.keys():
            self.add_clm_frm_clm_rgx(str_clm, str_clm_add, pre_sqd[i_key])
        self.dts[str_clm_add] = self.dts.apply(
            lambda x: x[str_clm_add] if type(x[str_clm_add]) == str else x[str_clm], axis=1)
        self.init_rst()
        for i_key in pre_sqd.keys():
            self.ltr_clm_rpc_prt(str_clm_add, pre_sqd[i_key], i_key)
    # seperating sqd from dpt 二级事业
    def ppc_dpt_add_sqd(self, clm_ful='FullName', clm_new='Name', clm_fll='dpt'):
        """
        pre-processing on 事业部 from dpt to Name
        :param clm_ful: find certain part from clm_ful into clm_new
        :param clm_new: the new column name
        :param clm_fll: if cannot find clm_new, fill clm_fll into clm_new
        :return: None
        """
        self.init_rst()
        self.add_clm_frm_clm_rgx(clm_ful,clm_new,['一部','二部','三部','四部','五部','六部','七部','八部','九部',])
        self.dts[clm_new] = self.dts.apply(lambda x: x[clm_fll] if (x[clm_new] is None)|(x[clm_fll]!='二级事业') else x[clm_new],axis=1)
        self.init_rst()

# # web user information checking
class ApiPip(PrcApi):
    """
    用于外部调用接口相关校验,default dct_prm=dct_web_act
    """
    def __init__(self, dct_prm=None, dtf_mpt=None):
        """initiate
        define self.dts, self.len, self.typ
        :param dts_mpt: var name of target dataset
        :return: None
        """
        super(ApiPip, self).__init__(dct_prm, dtf_mpt)
    # add user
    def add_usr(self,usr, typ, authority, dct_prm=None):
        """
        add user in dct_web_act['mng_prc']
        :param usr: username
        :param typ: type of this user
        :param authority: in format {'typ':[authority,authority,...],'typ':[...],...}
        :param dct_prm: default dct_web_act
        :return: None
        """
        self.add_dct_prm(dct_prm)
        usr = PrcDtf(DataFrame([{'usr': usr, 'psw': fnc_ncd_str_hsh(usr),
                                 'typ': typ,'authority': authority,'tms': fnc_dtt_frm_tdy(0, 'str', '%Y-%m-%d %H:%M:%S')}]))
        self.dct_prm['mng_prc'].crt_and_pdt(usr.dts, self.dct_prm['ndx'])
    # alter authorities
    def ltr_thy(self,usr,authority):
        """
        alter aothorities, authority in type dict means add, alter, del; in type str&list means del
        :param usr: target user
        :param authority:
        :return:
        """
        thy = self.dct_prm['mng_prc'].xpt_cln_to_lst({'usr':usr},['authority'],False,False)[0]['authority']
        if type(authority) in [dict]:
            for i_key in authority.keys():
                if i_key in thy.keys() and authority[i_key] != thy[i_key]:    # 改
                    thy[i_key] = fnc_lst_frm_cll_mrg('union',authority[i_key],thy[i_key])
                elif i_key in thy.keys() and authority[i_key] == thy[i_key]:  # 删
                    thy.pop(i_key)
                else: thy.update(authority)                                 # 增
        elif type(authority) in [str,list]:
            for i_key in fnc_lst_frm_all(authority):
                thy.pop(i_key)                                              # 删
        self.dct_prm['mng_prc'].ltr_dcm({'usr':usr},{'authority':thy})
    # request.form['npt_usr','npt_psw'] checking
    def chk_usr(self, dct_rqt, dct_prm=None):
        """
        卡密检查 - request.form dict包含 npt_usr, npt_psw
        :param dct_rqt: request.form
        :param dct_prm: default dct_web_act, a cln for usr and psw storage
        :return: dict of the target user
        """
        self.add_dct_prm(dct_prm)
        str_psw = fnc_ncd_str_hsh(dct_rqt['npt_psw'])
        self.dts = self.dct_prm['mng_prc'].xpt_cln_to_lst({'usr': dct_rqt['npt_usr'], 'psw': str_psw})
        if self.dts:
            self.dts = self.dts[0]
            self.dts.update({'msg':'200','info':'ok'})
        else: self.dts = {'msg':'403','info':'password error'}  # 权限拒绝
    # from request.form['npt_usr','npt_psw'] to 'authority'
    def chk_thy(self, dct_rqt, str_thy, dct_prm=None):
        """
        check private authority, 利用request.form中的卡密，对应寻找到库表中的'authority'
        :param str_thy: 需要指定检查的权限类别, 如ctn_tct
        :param dct_rqt: request.form, needs ['npt_usr','npt_psw']
        :param dct_prm: default dct_web_act
        :return: None
        """
        self.add_dct_prm(dct_prm)
        if self.len==0:
            self.chk_usr(dct_rqt)
        if self.dts['msg']=='200' and str_thy not in self.dts['authority']:
            self.dts.update({'msg':'403','info':'authority error'})
    # visitors' logs
    def add_log(self,dct_rqt, pth_txt=None, dct_prm=None):
        """
        add logs for web action
        :param dct_rqt: request.form, needs ['npt_usr','cip']
        :param pth_txt: default 'C:/inetpub/wwwroot/v1/sys/dts/log.txt'
        :param dct_prm: default dct_web_act
        :return: None
        """
        self.add_dct_prm(dct_prm)
        pth_txt = 'C:/inetpub/wwwroot/v1/sys/dts/log.txt' if pth_txt is None else pth_txt
        log = PrcDtf([{'date': fnc_dtt_frm_tdy(0,'str','%Y-%m-%d %H:%M:%S'), 'log': dct_rqt['cip'],'usr':dct_rqt['npt_usr']}])
        log.xpt_txt_frm_dts(pth_txt, 'a')

# Web API undump and checking
def fnc_spr_prm(str_prm):
    """
    alter format and change type from djs-str to dict
    :param str_prm: str in format 'key=contents&key=contents&...'
    :return: a dict of params, key _msg's value 200 means alright
    """
    if re_search('.*=.*&',str_prm):
        str_prm = loads(('{"'+str_prm+'"}').replace('=','":"').replace('&','","').replace('",""}','"}'))
        str_prm.update({'_msg':'200'})
    else: str_prm = {'_msg':'formatError'}
    return str_prm

# # 建立新客户
# chk = ApiPip(dct_web_act)
# chk.add_usr('shenlong','adm',{'ctn_tct':['hhl'],'ctn_shd':['xpt']})
# # 客户权限修改
# chk = ApiPip(dct_web_act)
# chk.ltr_thy('shenlong',{'ctn_tct':['hhl','stf'],'ctn_shd':['xpt']}) # means alter, add or delete authority
# # 客户权限快速删除
# chk = ApiPip(dct_web_act)
# chk.ltr_thy('shenlong','ctn_shd') # means delete authority 'ctn_shd'
