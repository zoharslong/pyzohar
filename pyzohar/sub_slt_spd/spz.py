#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on 2021.03.19
spider operation.
@author: zoharslong
"""
from time import sleep
from random import randint
from re import findall as re_find, sub as re_sub
from numpy import nan as np_nan, count_nonzero as np_count
from pyzohar.sub_slt_bsc.dfz import dtz, dfz


class spz_nit(dfz):
    """
    web spider class initiation
    >>> dct_ppc = {
    >>>     'ndx_rgx':{'_id':'(\d+)'},  # 指定详情页url替换规则: {用于搜索详情页的ID: url_ctt中需要替换的正则表达式}
    >>>     'ndx':['_id','...'],        # 最终入库时的唯一组合键, 必然包含上一行的唯一识别ID
    >>>     'clm_typ':{}                # 其他常规列处理参数
    >>> }
    >>> spd = spz_nit(lcn={
    >>>     'url_lst': 'https://sz.ke.com/chengjiao/start/pg(\d+)/',    # crt_url_lst使(\d+)递增生成url, start为占位符
    >>>     'url_ctt': 'https://sz.ke.com/chengjiao/c(\d+)/pg1/',       # crt_url_ctt使ppc.ndx_rgx.key替换(\d+)生成url
    >>>     'prx':'auto', 'prx_tkn': {'neek':'', 'appkey':''},          # 开启极光代理
    >>>     'hdr':{},                                                   # 虚构请求头
    >>>     'ppc':dct_ppc})
    >>> spd.spd_bch(None, prm='lst', pg_max=9)  # 爬取列表页, 最高至第八页, 数据处理函数选择None
    """
    def __init__(self, dts=None, lcn=None, *, spr=False):
        """
        more param of self is available, in [clm_x, clm_y, _x, _y, mdl]
        :param dts: self.dts, premier than dfz_mpt
        :param lcn: self.lcn, premier than dfz_mpt
        :param spr: let self = self.dts
        """
        super(spz_nit, self).__init__(dts, lcn, spr=spr)

    def crt_url_lst(self, pg_max=100):
        """
        create url on list pages. 对列表类的url爬虫, 做url页面的序号递增.
        :param pg_max: 最大页面控制数
        :return: None
        """
        if 'url' not in self.lcn.keys() or re_find(self.lcn['url_ctt'], self.lcn['url']):   # 初次定义url
            self.lcn['url'] = self.lcn['url_lst'].replace('(\d+)', '1')
        else:
            now = int(re_find(self.lcn['url_lst'].replace('?', '\?'), self.lcn['url'])[0])
            pgs = now + 1
            if pgs >= pg_max:   # 最大爬取页面控制线
                self.lcn.pop('url')
            else:               # 正常情况下的操作
                self.lcn['url'] = self.lcn['url_lst'].replace('(\d+)', str(pgs))   # 在已有url的基础上对url做递增处理

    def crt_url_ctt(self, prd_gap=15, srt=None):
        """
        create url on content pages. 对内容类的url爬虫, 从mongodb中搜索对应的详情页id拼接形成新的内容页url.
        @prd_gap: 设定的时间间隔, 当目标estateID最后一次运行爬虫距今时差大于这个值时, 执行爬虫
        @return: None
        """
        srt = {'cnt': -1} if srt is None else srt
        edg = dtz('now')
        edg.shf(-prd_gap)
        edg = edg.dtt_to_typ('str', '%Y-%m-%d %H:%M:%S', rtn=True)
        eid = False
        try:
            eid = self._myCln.aggregate([
                {'$match': {'$or': [
                    {'__time_ctt': {'$exists': False}},
                    {'__time_ctt': {'$lte': edg}},
                    {'__time_ctt': {'$in': [None, '', np_nan]}}
                ]}},
                {'$group': {'_id': '$' + list(self.lcn['ppc']['ndx_rgx'].keys())[0], 'cnt': {'$sum': 1}}},
                {'$sort': srt}]).next()['_id']
        except StopIteration:   # 当无法查找到目标时pymongo会报Error: StopIteration, 此时将不再产生有效的url
            pass
        finally:
            if eid:
                if 'url' not in self.lcn.keys() or re_find(self.lcn['url_lst'], self.lcn['url']):
                    self.lcn['url'] = self.lcn['url_ctt'].replace(list(self.lcn['ppc']['ndx_rgx'].values())[0], eid)
                else:
                    rpc = re_find(self.lcn['url_ctt'], self.lcn['url'])[0]
                    self.lcn['url'] = self.lcn['url'].replace(rpc, eid)
            elif 'url' in self.lcn.keys():   # 如果库中已不存在有效的eid
                self.lcn.pop('url')

    def spd_xpt(self, fnc_sop=None, *, rty=2, frc=False, rtn=True):
        """
        spider export from html files.
        @param fnc_sop: a special function to deal with target html
        @param rty: repeat running times
        @param frc: force to change proxy ip
        @param rtn: return spider is alright or not
        @return: in [True, False] for [got target, cannot got anything]
        """
        prc, bch = True, 0
        while prc and bch <= rty:           # 对self.api_run获得的信息进行有效性判断后发现无效时的循环重复
            if bch == rty and rty >= 2:     # 当最后一次循环的时候强制更换proxy
                self.api_run(frc=True, prm='get')
            else:                           # 其他时候不强制刷新一次proxy
                self.api_run(frc=frc, prm='get')
            if fnc_sop is None:             # 当未指定soup - xml处理函数时在此处中断运行
                raise KeyError('stop: need a fnc_sop.')
            self.dts = fnc_sop(self)        # 自定义的网页数据摘取过程
            self.drp_dcm_dpl(self.lcn['ppc']['ndx'])
            if self.len > 0:                # 此时代表经过fnc_sop处理后, 有效的数据已经产生了, 循环结束
                self.mng_xpt(self.lcn['ppc']['ndx'])
                prc = False                 # 网页定制化处理有数据且成功入库后, 终止循环
                if rtn:
                    return True
            else:
                self.api_prx(frc=True)
                if rtn and bch == rty:      # 当第三次循环仍旧未能得到理想结果时, return False
                    return False
            bch += 1
            sleep(1 + randint(0, 2))

    def spd_bch(self, fnc_sop=None, *, prm='lst', srt=None, pg_max=100):
        """
        spider batch. 循环运作直到self.spd_xpt返回False停止.
        :param fnc_sop: a special function to deal with target html
        :param prm: in ['list', 'lst', 'content', 'ctt']
        :param srt: only when prm in ['ctt'], define the sort of id, default {'cnt': -1}
        :param pg_max: default 100
        :return: None
        """
        self.lcn['url_lst'] = self.lcn['url_lst'].replace('start/', '')     # 使用本方法即可排除需要二次循环的情况
        prc = True
        while prc:                              # 决定是否终止整个爬虫循环, 每个循环代表一个新的url被执行
            if prm in ['list', 'lst']:
                self.crt_url_lst(pg_max)        # 创造本次爬取的url
                if 'url' not in self.lcn.keys():    # 当self.crt_url未能产生新的url时终止batch running
                    print('stop: url not exist.')
                    break
                else:
                    prc = self.spd_xpt(fnc_sop)     # 爬取页并分离其中的数据导入mongodb, 当爬取列表页连续三次失败时终止batch running
            elif prm in ['content', 'ctt']:
                self.crt_url_ctt(srt=srt)
                if 'url' not in self.lcn.keys():    # 当self.crt_url未能产生新的url时终止batch running
                    print('stop: url not exist.')
                    break
                else:
                    self.spd_xpt(fnc_sop, rtn=False)
            sleep(2 + randint(0, 5))

    def spd_bch_whl_swh(self, fnc_sop=None, *, srt=None, pg_max=100, lst_bch=None):
        """
        专用定制函数 - 由于贝壳每个筛选下只能显示100页，因此对各个行政区分别运行列表页爬虫; 本质就是self.spd_bch
        :param fnc_sop: default sop_est_shd_bke_lst
        :param srt: default {'cnt': -1}
        :param pg_max: max pages, default 100
        :param lst_bch: a list of different districts, default for shenzhen beike
        :return: None
        """
        lst_bch = [
            'longgangqu', 'longhuaqu', 'luohuqu', 'futianqu', 'dapengxinqu',
            'yantianqu', 'nanshanqu', 'baoanqu', 'guangmingqu', 'pingshanqu'
        ] if lst_bch is None else lst_bch   # default shenzhen beike
        if lst_bch[0] != 'start':
            lst_bch = ['start'] + lst_bch
        if lst_bch[-1] != 'end':
            lst_bch += ['end']
        for i in range(len(lst_bch) - 1):
            try:
                self.lcn.pop('url')  # self.lcn中不存在url以保证初始化状态
            except KeyError:
                pass
            if lst_bch[i] in ['start']:
                print('info: page series switch from %s to %s.' % (lst_bch[i], lst_bch[i+1]))
                self.lcn['url_lst'] = re_sub(lst_bch[i], lst_bch[i + 1], self.lcn['url_lst'])
            elif re_find(lst_bch[i], self.lcn['url_lst']):
                self.spd_bch(fnc_sop, prm='lst', srt=srt, pg_max=pg_max)
                print('info: page series switch from %s to %s.' % (lst_bch[i], lst_bch[i+1]))
                self.lcn['url_lst'] = re_sub(lst_bch[i], lst_bch[i + 1], self.lcn['url_lst'])

    def ltr_spd_for_dcm(self, flt_dcm=2, mtd='lte', prm='drop', nit=True):
        """
        因为现有文档数量的原因, 通过修改时间的形式对是否运作某一个文档的爬虫进行预设, 将成交数量较少的楼盘排除出爬虫序列
        默认情况为将文档数量少于等于2的ID排除出爬虫序列不再爬取
        :param flt_dcm: 文档数量判断
        :param mtd: 大于还是小于上述文档数量边界, 默认'lte' in [‘lte’,'gt']
        :param prm: 对满足上述条件的文档执行什么操作, 默认放弃爬取'drop' in ['drop','keep']
        :param nit: 是否对'__time_ctt'为空的行也进行修改操作, 默认True
        :return: None
        """
        str_dtz = dtz('now').dtt_to_typ(str_fmt='%Y-%m-%d %H:%M:%S', rtn=True) if prm in ['drop', 'drp'] else \
            dtz(dtz('now').shf(-30, rtn=True)).dtt_to_typ(str_fmt='%Y-%m-%d %H:%M:%S', rtn=True)
        str_key = list(self.lcn['ppc']['ndx_rgx'].keys())[0]
        self.mng_mpt({}, [str_key, '__time', '__time_ctt'])
        self.typ_to_dtf()
        if not nit:
            self.drp_dcm_na('__time_ctt')
        self.stt_clm(str_key, '__time', [np_count])
        if mtd in ['lte']:
            self.dts = self.dts.loc[self.dts['__time'] <= flt_dcm]
        elif mtd in ['lt']:
            self.dts = self.dts.loc[self.dts['__time'] < flt_dcm]
        elif mtd in ['gte']:
            self.dts = self.dts.loc[self.dts['__time'] >= flt_dcm]
        elif mtd in ['gt']:
            self.dts = self.dts.loc[self.dts['__time'] > flt_dcm]
        for i in range(self.len):  # 更新爬虫时间
            self.ltr_cln_dcm({str_key: self.dts.loc[i, str_key]}, {'$set': {'__time_ctt': str_dtz}})


class spz(spz_nit):
    """
    web spider
    """
    def __init__(self, dts=None, lcn=None, *, spr=False):
        super(spz, self).__init__(dts, lcn, spr=spr)
