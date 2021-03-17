#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on Fri Sep 18 15:52:01 2020
modeling operation.
needs sklearn, factor_analyzer
@author: zoharslong
"""
from numpy import max as np_max, min as np_min, ravel as np_ravel, argsort as np_argsort, abs as np_abs
from numpy import array as np_array, int32 as np_int32, int64 as np_int64, float64 as np_float64
from pandas import DataFrame, concat as pd_concat
from re import findall as re_find
import seaborn as sns               # plot on factor analysis
from factor_analyzer import FactorAnalyzer, calculate_kmo, calculate_bartlett_sphericity, Rotator
from sklearn import metrics
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.cluster import KMeans
from sklearn.svm import SVC, SVR
from sklearn.svm._classes import SVC as typ_svc, SVR as typ_svr
from sklearn.manifold import TSNE
from matplotlib import pyplot as plt
from os.path import join as os_join
from random import sample as rnd_smp
from joblib import dump as jb_dump, load as jb_load
from pyzohar.bsz import lsz
from pyzohar.dfz import dfz


class mdz_nit(dfz):
    """
    modeling class initiation
    >>> mdl = mdz_nit(DataFrame(),dfz_mpt=dfz(),dct_mdl={'_id':'','fld':'','clm_x':[],'clm_y':[],'drp':None,'kep':None})
    >>> mdl.mdl_nit()
    """
    def __init__(self, dts=None, lcn=None, *, spr=False, dfz_mpt=None, dct_mdl=None):
        """
        more param of self is available, in [clm_x, clm_y, _x, _y, mdl]
        :param dts: self.dts, premier than dfz_mpt
        :param lcn: self.lcn, premier than dfz_mpt
        :param spr: let self = self.dts
        :param dfz_mpt: a dfz for inserted
        :param dct_mdl: a dict of params, keys in ['_id', 'fld', 'clm_y', 'clm_x', 'drp', 'kep']
        :param dct_mdl: more params are auto generated, in ['fls', 'smp_raw/_y_0/_y_1/_trn/_tst']
        """
        if dfz_mpt is not None:
            dts = dts if dts is not None else dfz_mpt.dts.copy()
            lcn = lcn if lcn is not None else dfz_mpt.lcn.copy()
        super(mdz_nit, self).__init__(dts, lcn, spr=spr)
        self.clm_x, self.clm_y, self._x, self._y, self.mdl = None, None, None, None, None
        self.mdl_nit(dct_mdl=dct_mdl)

    def mdl_nit(self, dct_mdl=None, rst=True):
        """
        针对model的初始化, 会使用self.dts对self.lcn.mdl的一系列params进行刷新, 通常在self.dts发生变化的时候必须调用
        :param dct_mdl: a dict of params, keys in ['_id', 'fld', 'clm_y', 'clm_x', 'drp', 'kep']
        :param rst: reset self.lcn.mdl or not, default True
        :return: None
        """
        if dct_mdl is not None:
            mch_tmp = [self.lcn['mdl']['_id'], self.lcn['mdl']['clm_y'], self.lcn['mdl']['clm_x']] if \
                'mdl' in self.lcn.keys() else None
            dct_mdl['clm_x'] = lsz(dct_mdl['clm_x']).typ_to_lst(rtn=True)
            dct_mdl['clm_y'] = lsz(dct_mdl['clm_y']).typ_to_lst(rtn=True)
            self.lcn.update({'mdl': dct_mdl})
            self.clm_x = self.lcn['mdl']['clm_x']
            self.clm_y = self.lcn['mdl']['clm_y']
            self.lcn['mdl']['fls'] = 'mdl_' + self.lcn['mdl']['_id'] + '_' + self.lcn['mdl']['clm_y'][0] + '.pkl'
            if mch_tmp != [self.lcn['mdl']['_id'], self.lcn['mdl']['clm_y'], self.lcn['mdl']['clm_x']]:
                self.mdl, self._x, self._y = None, None, None  # 当参数发生较大改变时, 删除已经存在的self.mdl
        if rst:
            self.mdl_smp_trc()

    def mdl_smp_trc(self):
        """
        basic sample trace
        """
        tmp = dfz(self.dts.copy(), self.lcn.copy())
        if tmp.len > 0:
            # 标记数据集筛选
            if self.lcn['mdl']['drp']:
                tmp.drp_dcm_ctt(self.lcn['mdl']['drp'], prm='drop', ndx_rst=False)
            if self.lcn['mdl']['kep']:
                tmp.drp_dcm_ctt(self.lcn['mdl']['kep'], prm='keep', ndx_rst=False)
            # 生成样本参数
            self.lcn['mdl']['smp_raw'] = {'ndx': tmp.dts.index.tolist(), 'clm_x': self.clm_x, 'clm_y': self.clm_y}
            tmp.drp_dcm_ctt({self.clm_y[0]: 1}, prm='keep', ndx_rst=False)
            self.lcn['mdl']['smp_y_1'] = {'ndx': tmp.dts.index.tolist(), 'clm_x': self.clm_x, 'clm_y': self.clm_y}
            dff_ndx = lsz().mrg('differ', self.lcn['mdl']['smp_raw']['ndx'], self.lcn['mdl']['smp_y_1']['ndx'],
                                rtn=True)
            self.lcn['mdl']['smp_y_0'] = {'ndx': dff_ndx, 'clm_x': self.clm_x, 'clm_y': self.clm_y}
            self._dl_smp_blc()  # generate smp_trn, smp_tst

    def _dl_smp_blc(self):
        """
        train/test sample balanced; 样本平衡策略, 以标志变量中较少的一方为基准, 保持标志平衡, 测试集占比10%
        >>> # 另一种既有的训练集划分方法
        >>> x_trn, x_tst, y_trn, y_tst = train_test_split(self._x, self._y, test_size=0.2, random_state=2)
        """
        smp_min = min(len(self.lcn['mdl']['smp_y_1']['ndx']), len(self.lcn['mdl']['smp_y_0']['ndx']))
        ndx_y_0 = rnd_smp(self.lcn['mdl']['smp_y_0']['ndx'], int(smp_min * 0.9))
        ndx_y_1 = rnd_smp(self.lcn['mdl']['smp_y_1']['ndx'], int(smp_min * 0.9))
        self.lcn['mdl']['smp_trn'] = {'ndx': ndx_y_0 + ndx_y_1, 'clm_x': self.clm_x, 'clm_y': self.clm_y}
        ndx_t_0 = lsz().mrg('differ', self.lcn['mdl']['smp_y_0']['ndx'], ndx_y_0, rtn=True)
        ndx_t_1 = lsz().mrg('differ', self.lcn['mdl']['smp_y_1']['ndx'], ndx_y_1, rtn=True)
        if len(self.lcn['mdl']['smp_y_1']['ndx']) < len(self.lcn['mdl']['smp_y_0']['ndx']):
            ndx_t_0 = rnd_smp(ndx_t_0, int(smp_min * 0.1))
        else:
            ndx_t_1 = rnd_smp(ndx_t_1, int(smp_min * 0.1))
        self.lcn['mdl']['smp_tst'] = {'ndx': ndx_t_0 + ndx_t_1, 'clm_x': self.clm_x, 'clm_y': self.clm_y}

    def mdl_smp_mpt(self, tgt=None, *, rtn=False):
        """
        model sample from param to real datasets, saved in self._x and self._y
        :param tgt: in [None, 'smp_raw', 'smp_trn', 'smp_tst']
        :param rtn: if return (dtf_y, dtf_x) or not, default False; if return, self._x/_y will not change
        :return: if rtn is True, return (dtf_y, dtf_x)
        """
        tmp, y, x = dfz(self.dts, self.lcn), None, None
        if tmp.len > 0:
            if tgt is None:     # 无样本集条件，则默认导入self.dts
                y, x = tmp.dts[self.clm_y], tmp.dts[self.clm_x]
            else:               # 否则根据样本集参数选择样本集
                tmp.drp_dcm(self.lcn['mdl'][tgt]['ndx'], prm='keep', ndx_rst=False)
                y, x = tmp.dts[self.lcn['mdl'][tgt]['clm_y']], tmp.dts[self.lcn['mdl'][tgt]['clm_x']]
        if rtn:
            return y, x
        self._y, self._x = y, x

    def mdl_sav(self, *, fld=None, fls=None):
        """
        save model into .pkl file
        :param fld: default None, get fold location from self.lcn
        :param fls: default None, get file location from self.lcn
        :return: None
        """
        fld = self.lcn['mdl']['fld'] if fld is None else fld
        fls = self.lcn['mdl']['fls'] if fls is None else fls
        jb_dump(self.mdl, os_join(fld, fls))

    def mdl_lod(self, *, fld=None, fls=None):
        """
        import model from .pkl file
        :param fld: default None, get fold location from self.lcn
        :param fls: default None, get file location from self.lcn
        :return: None
        """
        fld = self.lcn['mdl']['fld'] if fld is None else fld
        fls = self.lcn['mdl']['fls'] if fls is None else fls
        self.mdl = jb_load(os_join(fld, fls))
        str_tmp = self.lcn['mdl']['_id']
        self.lcn['mdl']['_id'] = re_find('mdl_(.*?)_' + self.lcn['mdl']['clm_y'][0], self.lcn['mdl']['fls'])[0]
        if str_tmp != self.lcn['mdl']['_id']:
            print('info: _id switch from %s to %s' % (str_tmp, self.lcn['mdl']['_id']))

    def drp_dcm_for_bly(self, flt_smp=2):
        """
        drop documents for the balance of Target label. 使用self.clm_y进行样本数量平衡, 会直接导致self.dts的改变
        :param flt_smp: 选择数量较多的label进行抽样，使其数量达到flt_smp倍于数量较少的label
        :return: None
        """
        # 不同分类的样本数量控制
        stm_1 = self.dts.loc[self.dts[self.clm_y[0]] == 1]
        stm_0 = self.dts.loc[self.dts[self.clm_y[0]] == 0]
        shp_0, shp_1 = stm_0.shape[0], stm_1.shape[0]
        try:
            stm_0 = stm_0.sample(int(shp_1 * flt_smp)) if shp_1*flt_smp < shp_0 else stm_0
            stm_1 = stm_1.sample(int(shp_0 * flt_smp)) if shp_0*flt_smp < shp_1 else stm_1
        except ValueError:
            pass
        self.dts = pd_concat([stm_1, stm_0])
        self.mdl_nit()


class mdz_fct(mdz_nit):
    """
    model factor analysis
    聚类的验证 https://blog.csdn.net/u010159842/article/details/78624135
    >>> mdl = mdz(DataFrame(),dfz_mpt=dfz(),dct_mdl={'_id':'','fld':'','clm_x':[],'clm_y':[],'drp':None,'kep':None})
    >>> mdl.drp_dcm_na(mdl.clm_x)       # 删除为空的数据行
    >>> mdl.mdl_nit()                   # 刷新参数集状态, 需要在self.dts发生行列变动时执行
    >>> mdl.mdl_smp_mpt()               # 根据参数集生成数据集 [None, 'smp_trn', 'smp_tst', 'smp_raw']
    >>> mdl.rnd_frs_clf()               # 随机森林变量重要性检验
    >>> mdl.fct_fit(10, prm='chksav')   # 拟合模型 ['chk', 'sav']
    >>> mdl.fct_transform(['电话积极','带看高效','上线稳定','电话稳定','在岗稳定','带看稳定'])  # 应用模型
    """
    def __init__(self, dts=None, lcn=None, *, spr=False, dfz_mpt=None, dct_mdl=None):
        """
        more param of self is available, in [clm_x, clm_y, _x, _y, mdl]
        :param dts: self.dts, premier than dfz_mpt
        :param lcn: self.lcn, premier than dfz_mpt
        :param spr: let self = self.dts
        :param dfz_mpt: a dfz for inserted
        :param dct_mdl: a dict of params, keys in ['_id', 'fld', 'clm_y', 'clm_x', 'drp', 'kep']
        :param dct_mdl: more params are auto generated, in ['fls', 'smp_raw/_y_0/_y_1/_trn/_tst']
        """
        super(mdz_fct, self).__init__(dts, lcn, spr=spr, dfz_mpt=dfz_mpt, dct_mdl=dct_mdl)

    def fct_fit(self, nfc=3, mtd='principal', rtt='varimax', *, prm='chksav'):
        """
        factor fit step, to check or save model
        :param nfc: number of factors, default 3
        :param mtd: method, default 'principal'
        :param rtt: rotation method, default 'varimax'
        :param prm: in ['chk', 'sav', ''] for [model check, save to file, import self.mdl only]
        :return: None
        """
        self.mdl = FactorAnalyzer(n_factors=nfc, method=mtd, rotation=rtt)
        self.mdl.fit(self._x)
        if re_find('chk', prm):
            self._ct_fit_chk()
        if re_find('sav', prm):
            self.mdl_sav()

    def _ct_fit_chk(self, prm='kmocmmegnvrnlod'):
        """
        几种常见的factor检查方法
        :param prm: ['kmo/cmm/egn/vrn/lod'] for kmo, communalities, eigenvalues, variance, load matrix
        :return: indicators and pictures in prm
        """
        if re_find('kmo', prm):
            print('kmo %.4f; bartlett %.4f.' % (self._ct_fit_chk_kmo()))  # kmo
        if re_find('cmm', prm):
            print('communalities: %s' % self.mdl.get_communalities())  # 公因子方差
        if re_find('egn', prm):
            self._ct_fit_chk_egn()  # egn图
        if re_find('vrn', prm):
            self._ct_fit_chk_vrn()  # 因子代表性
        if re_find('lod', prm):
            self._ct_fit_chk_lod()  # 载荷矩阵重要性和命名

    def _ct_fit_chk_kmo(self):
        """
        kmo检验和巴特利特, 前者要求尽可能大, 后者要求<0.05
        :return: [kmo_model, bartlett]
        """
        kmo_all, kmo_model = calculate_kmo(self._x)
        bartlett = round(calculate_bartlett_sphericity(self._x)[1], 4)
        return kmo_model, bartlett

    def _ct_fit_chk_egn(self):
        """
        eigenvalues to check factor number
        :return: a picture of eigenvalues
        """
        ev, v = self.mdl.get_eigenvalues()  # eigen values  特征值; 通常要求大于1
        plt.scatter(range(1, ev.shape[0] + 1), ev)
        plt.plot(range(1, ev.shape[0] + 1), ev)
        plt.title('Scree Plot')
        plt.xlabel('Factors')
        plt.ylabel('Eigenvalue')
        plt.grid()
        plt.show()

    def _ct_fit_chk_vrn(self):
        """
        variance plot to check factor number
        :return: a picture of variance
        """
        vrn = self.mdl.get_factor_variance()  # 因子的累积贡献度
        plt.scatter(range(1, vrn[2].shape[0] + 1), vrn[2])
        plt.plot(range(1, vrn[2].shape[0] + 1), vrn[2])
        plt.title('Scree Plot')
        plt.xlabel('Factors')
        plt.ylabel('variance')
        plt.grid()
        plt.show()

    def _ct_fit_chk_lod(self, *, prn=False):
        """
        load plot to check factor number
        :param prn: save the picture or not, default False
        :return: a picture of load matrix
        """
        rtr = Rotator()
        load_matrix = rtr.fit_transform(self.mdl.loadings_)
        sns.set(font="simhei")
        df_cm = DataFrame(np_abs(load_matrix), index=self._x.columns)
        plt.figure(figsize=(8, 8))
        ax = sns.heatmap(df_cm, annot=True, cmap="BuPu")
        ax.yaxis.set_tick_params(labelsize=10)  # 设置y轴的字体的大小
        plt.title('Factor Analysis', fontsize='xx-large')
        plt.ylabel('Sepal Width', fontsize='xx-large')  # Set y-axis label
        plt.show()
        if prn:
            plt.savefig('factorAnalysis.png', dpi=300)

    def fct_transform(self, rnm=None, *, rtn=False):
        """
        factor analysis transform by self.mdl, merge factors on the left side of self.dts.
        :param rnm: new name of factors in list
        :param rtn: if return factors or not, default False
        :return: if rtn is True, return factors in type dataframe
        """
        if self.mdl is None:
            self.mdl_lod()
        vrn = self.mdl.get_factor_variance()
        dtf_fct = self.mdl.transform(self._x)
        # 构造命名
        dct_fct = {i: 'fct_' + str(i) for i in range(len(vrn[0]))}
        if rnm is not None:
            dct_tmp = {i: rnm[i] for i in range(len(rnm))} if type(rnm) in [list] else rnm.copy()
            dct_fct.update(dct_tmp)
        # 计算总分
        lst = []
        for i in dtf_fct:
            lst.append(sum([i[j] * vrn[1][j] for j in range(len(i))]))
        dtf_fnl = DataFrame(dtf_fct, index=self._x.index).rename(columns=dct_fct)
        dtf_fnl['scr'] = lst
        # 将结果附加的根源数据集self.dts中
        self.mrg_dtf(dtf_fnl, prm='outer_index')
        if rtn:
            return dtf_fnl


class mdz_kmn(mdz_nit):
    """
    model factor analysis
    >>> mdl = mdz(DataFrame(),dfz_mpt=dfz(),dct_mdl={'_id':'','fld':'','clm_x':[],'clm_y':[],'drp':None,'kep':None})
    >>> mdl.mdl_nit({'_id':''})         # 切换某些dct_mdl中的参数需要使用这个句子
    >>> mdl.mdl_smp_mpt()               # 根据参数集生成数据集 [None, 'smp_trn', 'smp_tst', 'smp_raw']
    >>> mdl.kmn_fit([2,6])              # 测试二到六个聚类的效果
    >>> mdl.kmn_fit(3, prm='sav')       # 经过测试, 确定3格局类最好时对模型进行保存
    >>> mdl.kmn_transform('类型')        # 应用上一步存档的模型, 对分类变量命名为'类型'
    """
    def __init__(self, dts=None, lcn=None, *, spr=False, dfz_mpt=None, dct_mdl=None):
        """
        more param of self is available, in [clm_x, clm_y, _x, _y, mdl]
        :param dts: self.dts, premier than dfz_mpt
        :param lcn: self.lcn, premier than dfz_mpt
        :param spr: let self = self.dts
        :param dfz_mpt: a dfz for inserted
        :param dct_mdl: a dict of params, keys in ['_id', 'fld', 'clm_y', 'clm_x', 'drp', 'kep']
        :param dct_mdl: more params are auto generated, in ['fls', 'smp_raw/_y_0/_y_1/_trn/_tst']
        """
        super(mdz_kmn, self).__init__(dts, lcn, spr=spr, dfz_mpt=dfz_mpt, dct_mdl=dct_mdl)

    def kmn_fit(self, ncl=None, *, prm='sav'):
        """
        function the best clusters for k-menas.
        :param ncl: iter clusters, default [2,6]
        :param prm: in ['sav'] for save .pkl file to local path
        @return: None
        """
        ncl = lsz(ncl).typ_to_lst(rtn=True) if ncl is not None else [2, 6]
        ncl = range(ncl[0], ncl[1]) if len(ncl) == 2 else ncl  # [None, [2,4], 3] to [[2,6], [2,4], [3]]
        for i in ncl:
            self.mdl = KMeans(n_clusters=i, max_iter=1000)
            self.mdl.fit(self._x)
            y_lbl = self.mdl.labels_
            print('cluster %i; inertia %i; silhouette %.4f; calinski_harabasz %i;' % (
                i,  # cluster number
                self.mdl.inertia_,  # 距离越小说明簇分的越好
                metrics.silhouette_score(self._x, y_lbl, metric='euclidean'),  # 越接近1越好, range[-1, 1]
                metrics.calinski_harabasz_score(self._x, y_lbl)  # 越大越好
            ))
        if prm in ['sav', 'save'] and len(ncl) == 1:  # 只有ncl唯一, 非测试环节时方进行保存
            self.mdl_sav()

    def kmn_transform(self, rnm=None, *, rtn=False):
        """
        K-means transform
        :param rnm: rename in type str, only one new column need this rename param
        :param rtn: return a dataframe or not, default False
        :return: if rtn is True, return a dataframe of cluster label
        """
        rnm = 'clr' if rnm is None else rnm
        if self.mdl is None:
            self.mdl_lod()
        self.mdl.fit(self._x)
        y_lbl = self.mdl.labels_
        self.mrg_dtf(DataFrame(y_lbl, columns=[rnm], index=self._x.index), prm='outer_index')
        if rtn:
            return DataFrame(y_lbl, columns=[rnm], index=self._x.index)


class mdz_tsn(mdz_nit):
    """
    model factor analysis
    >>> mdl = mdz(DataFrame(),dfz_mpt=dfz(),dct_mdl={'_id':'','fld':'','clm_x':[],'clm_y':[],'drp':None,'kep':None})
    >>> mdl.mdl_nit({})
    >>> mdl.mdl_smp_mpt('smp_trn')  # 根据参数集生成数据集 [None, 'smp_trn', 'smp_tst', 'smp_raw']
    >>> mdl.tsn_fit_transform(rnm=['a','b'], prm='savplt')
    """
    def __init__(self, dts=None, lcn=None, *, spr=False, dfz_mpt=None, dct_mdl=None):
        """
        more param of self is available, in [clm_x, clm_y, _x, _y, mdl]
        :param dts: self.dts, premier than dfz_mpt
        :param lcn: self.lcn, premier than dfz_mpt
        :param spr: let self = self.dts
        :param dfz_mpt: a dfz for inserted
        :param dct_mdl: a dict of params, keys in ['_id', 'fld', 'clm_y', 'clm_x', 'drp', 'kep']
        :param dct_mdl: more params are auto generated, in ['fls', 'smp_raw/_y_0/_y_1/_trn/_tst']
        """
        super(mdz_tsn, self).__init__(dts, lcn, spr=spr, dfz_mpt=dfz_mpt, dct_mdl=dct_mdl)

    def tsn_fit_transform(self, ncl=None, rnm=None, *, prm='savplt', rtn=False, tsn_init='random', tsn_random_state=0):
        """
        t-SNE fit and transform in one step
        :param ncl: number of components(factors)
        :param rnm: rename new factors in type list,
        :param prm: when find 'sav', save local .pkl file; when find 'plt', draw a 2D plot for the result
        :param rtn: if return the components in DataFrame or not, default False
        :param tsn_init: TSNE param
        :param tsn_random_state: TSNE paran
        :return: if rtn is True, return the result in a DataFrame
        """
        ncl = 2 if ncl is None else ncl
        # 构造命名
        dct_rnm = {i: 'tsn_' + str(i) for i in range(ncl)}
        if rnm is not None:
            dct_tmp = {i: rnm[i] for i in range(ncl)} if type(rnm) in [list] else rnm.copy()
            dct_rnm.update(dct_tmp)
        # 建模环节
        self.mdl = TSNE(n_components=ncl, init=tsn_init, random_state=tsn_random_state)
        y_ncl = self.mdl.fit_transform(self._x)
        y_dtf = DataFrame(y_ncl, index=self._x.index).rename(columns=dct_rnm)
        self.mrg_dtf(y_dtf, prm='outer_index')
        if re_find('sav', prm):  # 保存
            self.mdl_sav()
        if re_find('plt', prm) and ncl == 2:  # 当降维2时输出散点图
            self._sn_plt(y_dtf)
        if rtn:
            return y_dtf

    def _sn_plt(self, y_dtf, y_lbl=None):
        """
        t-SNE plot in 2D
        """
        y_ncl = dfz(y_dtf).dtf_to_typ(typ='list', rtn=True, prm='split')['data']
        x_min, x_max = np_min(y_ncl, 0), np_max(y_ncl, 0)
        y_tsn = (y_ncl - x_min) / (x_max - x_min)  # 范围标准化
        y_lbl = DataFrame(self._y) if y_lbl is None else y_lbl  # 标志变量在图中展示, 需要其为序数变量为佳, 可区分颜色
        lst_typ_y = [int, float, np_int32, np_int64, np_float64]
        y_lbl = np_array([[i] for i in np_ravel(y_lbl)]) if type(y_lbl.iloc[0, 0]) in lst_typ_y else y_lbl
        plt.figure()
        for i in range(y_tsn.shape[0]):
            plt.text(
                y_tsn[i, 0], y_tsn[i, 1], (y_lbl[i, 0]),  # y为shape(n,1)的ndarray
                color=plt.cm.Set1(1 - y_lbl[i, 0] / len(list(set([i[0] for i in y_lbl])))),  # y为序列时选择此行
                fontdict={'weight': 'bold', 'size': 9}
            )
            plt.xlabel(y_dtf.columns[0], fontsize=14)
            plt.ylabel(y_dtf.columns[1], fontsize=14)
        plt.show()


class mdz_svm(mdz_nit):
    """
    model factor analysis
    >>> mdl = mdz(DataFrame(),dfz_mpt=dfz(),dct_mdl={'_id':'','fld':'','clm_x':[],'clm_y':[],'drp':None,'kep':None})
    >>> mdl.mdl_nit({})
    >>> mdl.mdl_smp_mpt('smp_trn')  # 建模时使用参数'smp_trn', 应用模型则留空
    >>> mdl.svm_fit(prm='chksav')   # 建模, 展示效果并保存
    >>> mdl.svm_transform(rnm=['prb','rst'])
    """
    def __init__(self, dts=None, lcn=None, *, spr=False, dfz_mpt=None, dct_mdl=None):
        """
        more param of self is available, in [clm_x, clm_y, _x, _y, mdl]
        :param dts: self.dts, premier than dfz_mpt
        :param lcn: self.lcn, premier than dfz_mpt
        :param spr: let self = self.dts
        :param dfz_mpt: a dfz for inserted
        :param dct_mdl: a dict of params, keys in ['_id', 'fld', 'clm_y', 'clm_x', 'drp', 'kep']
        :param dct_mdl: more params are auto generated, in ['fls', 'smp_raw/_y_0/_y_1/_trn/_tst']
        """
        super(mdz_svm, self).__init__(dts, lcn, spr=spr, dfz_mpt=dfz_mpt, dct_mdl=dct_mdl)

    def svm_fit(self, typ='SVC', *, prm='chksav', svm_kernel='rbf'):
        """
        SVM fit step
        :param typ: in ['SVC', 'SVR'] for classifier and regression
        :param prm: check confusion matrix and precision when find 'chk', save to .pkl file when find 'sav'
        :param svm_kernel: SVM param
        :return: None
        """
        if typ in ['SVC', 'svc']:       # 分类器
            self.mdl = SVC(kernel=svm_kernel, probability=True)
        elif typ in ['SVR', 'svr']:     # 回归器
            self.mdl = SVR(kernel=svm_kernel)
        self.mdl.fit(self._x, np_ravel(self._y))
        if re_find('chk', prm) and type(self.mdl) in typ_svc:
            self._vm_fit_chk()
        if re_find('sav', prm):
            self.mdl_sav()

    def _vm_fit_chk(self, lst_sch=None):
        y_tst, x_tst = self.mdl_smp_mpt('smp_tst', rtn=True)  # 对测试集进行验证
        lst_sch = [0.4, 0.7, 20] if lst_sch is None else lst_sch  # from [0] to [1], batches number [2]
        for i in range(lst_sch[2]):
            y_bln = lst_sch[0] + i * (lst_sch[1] - lst_sch[0]) / lst_sch[2]
            y_pre = [1 if i[1] >= y_bln else 0 for i in self.mdl.predict_proba(x_tst)]  # y_pre = mdl_svm.predict(x_tst)
            cfs = metrics.confusion_matrix(y_tst, y_pre)
            cfs = DataFrame(cfs, columns=['p', 'n'], index=['T', 'F'])
            print('confusionMatrix and precision in %.2f:' % y_bln)
            print(cfs, '; ', round((cfs.iloc[1, 1] + cfs.iloc[0, 0]) / (cfs.sum().sum()), 2), '; ')

    def svm_transform(self, rnm=None, prb=0.5, *, rtn=False):
        """
        SVM transform step
        :param rnm: rename [prob, result], need a list in length 2
        :param prb: 概率分割线, 当prob大于这个值时判断class为1, 否则为0
        :param rtn: return the result in DataFrame or not
        :return: if rtn is True, return the target DataFrame
        """
        if self.mdl is None:
            self.mdl_lod()
        # 构造命名
        dct_rnm = {0: self.lcn['mdl']['clm_y'][0] + '_prb', 1: self.lcn['mdl']['clm_y'][0] + '_rst'}
        if rnm is not None:
            dct_tmp = {i: rnm[i] for i in range(len(rnm))} if type(rnm) in [list] else rnm.copy()
            dct_rnm.update(dct_tmp)
        # 效果判断
        y_prb = []
        if type(self.mdl) in typ_svc:
            y_prb = [[i[1], 1 if i[1] >= prb else 0] for i in self.mdl.predict_proba(self._x)]
        elif type(self.mdl) in typ_svr:
            y_prb = [[i, 1 if i >= prb else 0] for i in self.mdl.predict(self._x)]
        y_prb = DataFrame(y_prb, index=self._x.index).rename(columns=dct_rnm)
        self.mrg_dtf(y_prb, prm='outer_index')
        if rtn:
            return y_prb


class mdz(mdz_fct, mdz_kmn, mdz_tsn, mdz_svm):
    """
    model class mixin
    """
    def __init__(self, dts=None, lcn=None, *, spr=False, dfz_mpt=None, dct_mdl=None):
        super(mdz, self).__init__(dts, lcn, spr=spr, dfz_mpt=dfz_mpt, dct_mdl=dct_mdl)

    def rnd_frs_clf(self, x=None, y=None, *, estimator=100):
        """random Forest clarify for importance of columns"""
        x = self._x if x is None else x
        y = self._y if y is None else y
        frs = RandomForestClassifier(n_estimators=estimator, random_state=0, n_jobs=-1)
        frs.fit(x, np_ravel(y))
        x_importance = frs.feature_importances_
        x_name = x.columns[0:]
        indices = np_argsort(x_importance)[::-1]
        for f in range(x.shape[1]):
            print("%2d) %-*s %.4f" % (f + 1, 30, x_name[indices[f]], x_importance[indices[f]]))
