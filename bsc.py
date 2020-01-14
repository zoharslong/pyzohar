#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 22 16:30:38 2019

@author: sl
@alters: 19-03-26 sl
"""
from os import path
from re import search
from time import mktime as tm_mktime, strftime as tm_strftime, struct_time as typ_tm_structtime
from datetime import datetime as dt_datetime
from datetime import date as typ_dt_date, time as typ_dt_time, timedelta as typ_dt_timedelta
from pandas import DataFrame, read_csv, read_excel
from pandas.core.series import Series as typ_pd_Series                  # 定义series类型
from pandas.core.frame import DataFrame as typ_pd_DataFrame             # 定义dataframe类型
from pandas.core.indexes.base import Index as typ_pd_Index              # 定义dataframe.columns类型
from pandas.core.indexes.range import RangeIndex as typ_pd_RangeIndex   # 定义dataframe.index类型
from pandas._libs.tslib import Timestamp as typ_pd_Timestamp            # 定义pandas.Timestamp类型
from numpy import array as np_array, ndarray as typ_np_ndarray
from pymysql import connect, IntegrityError                             # 用于连接mysql数据库, 默认连接腾讯云
from warnings import warn

# 用于匹配日期时间format与其相应的正则表达式
dtt_fmt_rgx = {
    '%Y-%m-%d %H:%M:%S ': '^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2} [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2} $',
    '%Y-%m-%d %H:%M:%S': '^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2} [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}$',
    '%Y-%m-%d': '^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}$',
    '%Y/%m/%d %H:%M:%S': '^[0-9]{4}/[0-9]{1,2}/[0-9]{1,2} [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}$',
    '%Y/%m/%d': '^[0-9]{4}/[0-9]{1,2}/[0-9]{1,2}$',
    '%Y.%m.%d %H:%M:%S': '^[0-9]{4}[.][0-9]{1,2}[.][0-9]{1,2} [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}$',
    '%Y.%m.%d': '^[0-9]{4}[.][0-9]{1,2}[.][0-9]{1,2}$',
    '%Y年%m月%d日 %H:%M:%S': '^[0-9]{2,4}年[0-9]{1,2}月[0-9]{1,2}日 [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}$',
    '%Y年%m月%d日': '^[0-9]{2,4}年[0-9]{1,2}月[0-9]{1,2}日$',
    'int': '^[0-9]+[.]{0,1}[0-9]*$',
}

# # cell alteration
# def fnc_rgx_frm_cll(cll, lst_rgx=None):
#     """
#     从一组自定义的正则列表中筛选出最后一个符合cll的正则表达式
#     :param cll: 目标单位, 类型为字符串
#     :param lst_rgx: 待匹配的正则表达式列表
#     :return: 一个匹配的正则表达式，类型为字符串
#     """
#     lst_rgx = list(dtt_fmt_rgx.values()) if lst_rgx is None else lst_rgx
#     rgx_xpt = None
#     for i_rgx in lst_rgx:
#         rgx_xpt = i_rgx if search(i_rgx, cll) else rgx_xpt
#     return rgx_xpt

# # datetime type alteration and index
def fnc_tms_frm_all(tms_mpt):
    """turn other types into timestamp
    常见内容表示时间的cell从其他格式切换为datetime.datetime格式
    :param tms_mpt: a single unit which is essentially time data but may not in time type
    :return: the original input in type timestamp
    """
    if type(tms_mpt) in [typ_pd_Timestamp, dt_datetime]:
        return tms_mpt
    elif type(tms_mpt) in [typ_dt_date]:
        return dt_datetime.combine(tms_mpt, typ_dt_time())
    elif type(tms_mpt) in [typ_tm_structtime]:
        return dt_datetime.strptime(tm_strftime('%Y-%m-%d %H:%M:%S', tms_mpt), '%Y-%m-%d %H:%M:%S')
    elif type(tms_mpt) in [str]:
        try:
            rgx_tmp = fnc_rgx_frm_cll(tms_mpt, list(dtt_fmt_rgx.values()))
            fmt_tmp = fnc_dct_frm_lst(list(dtt_fmt_rgx.values()), list(dtt_fmt_rgx.keys()))[rgx_tmp]
            if fmt_tmp == 'int':    # 识别形似'1560239414'以str保存的秒计时
                return dt_datetime.fromtimestamp(int(float(tms_mpt)))
            else:                   # 识别其余不同format的str日期
                return dt_datetime.strptime(tms_mpt, fmt_tmp)
        except KeyError:
            print('KeyError: the format of targe is not found')
    elif type(tms_mpt) in [float, int] and tms_mpt >= 84000:
        return dt_datetime.fromtimestamp(tms_mpt)

def fnc_tms_to_thr(tms_dtt, str_typ='str', str_format='%Y-%m-%d'):
    """
    alter a single cell in type datetime.datetime to other types
    :param tms_dtt: the target cell in type datetime.datetime
    :param str_typ: the type of return, default 'str', in ['str','timetuple','float','timestamp']
    :param str_format: if str_typ is 'str', this is needed for its format, default '%Y-%m-%d'
    :return: a single datetime cell in different types
    """
    if str_typ.lower() in ['str']:
        if str_format in ['%Y年%m月%d日']:
            return dt_datetime.strftime(tms_dtt, '%Y{y}%m{m}%d{d}').format(y='年', m='月', d='日')
        else:
            return dt_datetime.strftime(tms_dtt, str_format)
    elif str_typ.lower() in ['timetuple']:
        return tms_dtt.timetuple()
    elif str_typ.lower() in ['flt', 'float', 'int']:
        return int(tm_mktime(tms_dtt.timetuple()))
    elif str_typ.lower() in ['timestamp', 'tms']:
        return tms_dtt
    else:
        raise KeyError("str_typ needs ['str','timetuple','float','timestamp']")

def fnc_dtt_frm_tdy(flt_delta=0, str_typ='str', str_format='%Y-%m-%d', tms_tdy=None):
    """
    return datetime value in types and times from today
    :param flt_delta: hou many days later is the return from today
    :param str_typ: the type of return, default 'str', in ['str','timetuple','float','timestamp']
    :param str_format: if str_typ is 'str', this is needed for its format, default '%Y-%m-%d'
    :param tms_tdy: default today, but it can also be set on any day
    :return: a single datetime cell in different types
    """
    tms_tdy = dt_datetime.now() if tms_tdy is None else fnc_tms_frm_all(tms_tdy)
    tms_dtt = tms_tdy + typ_dt_timedelta(days=flt_delta)
    return fnc_tms_to_thr(tms_dtt, str_typ, str_format)

def fnc_dwk_frm_tdy(flt_delta=0, tms_tdy=None):
    """
    返回指定日期偏移若干周的周信息，format '%yw%w'
    :param flt_delta: how many weeks after the target date's week number
    :param tms_tdy: target date
    :return: year and week in format %yw%w
    """
    tms_tdy = dt_datetime.now() if tms_tdy is None else fnc_tms_frm_all(tms_tdy)
    tms_tdy = tms_tdy + typ_dt_timedelta(days=flt_delta*7)
    tms_dwk = tms_tdy.isocalendar()
    return "%sw%s" % (str(tms_dwk[0])[2:], str(tms_dwk[1]).zfill(2))

def fnc_dmh_frm_tdy(flt_delta=0,tms_tdy=None):
    """
    返回制定日期偏移若干月份的月信息,format '%ym%M'
    :param flt_delta: how many months after the target date's month number
    :param tms_tdy: target date
    :return: year and month in format %ym%M
    """
    tms_tdy = dt_datetime.now() if tms_tdy is None else fnc_tms_frm_all(tms_tdy)
    tms_mnth = tms_tdy.month
    tms_year = tms_tdy.isocalendar()[0]
    tms_fnl = tms_mnth + flt_delta
    while tms_fnl not in range(1,13):
        if tms_fnl>12:
            tms_fnl -= 12
            tms_year += 1
        elif tms_fnl<0:
            tms_fnl += 12
            tms_year -=-1
    return "%sm%s" % (str(tms_year)[2:], str(tms_fnl).zfill(2))

def fnc_tdy_frm_dwk(iso_year, iso_week, iso_day):
    """
    Gregorian calendar date for the given ISO year, week and day
    https://stackoverflow.com/questions/304256/whats-the-best-way-to-find-the-inverse-of-datetime-isocalendar
    """
    fourth_jan = typ_dt_date(iso_year, 1, 4)
    delta = typ_dt_timedelta(fourth_jan.isoweekday()-1)
    year_start = fourth_jan - delta
    return year_start + typ_dt_timedelta(days=iso_day-1, weeks=iso_week-1)

def fnc_lst_edg_frm_dwk(tms_dwk=None,str_typ='str',str_format='%Y-%m-%d'):
    """
    返回指定周的周一和周末日期的列表，周输入格式为'%yw%W'
    :param tms_dwk: in format '%yw%W'
    :param str_typ: 返回列表中日期的类型
    :param str_format: 返回列表中日期的格式
    :return: [dtt_bgn,dtt_end]
    """
    tms_dwk = fnc_dwk_frm_tdy() if tms_dwk is None else tms_dwk
    lst_edg = [fnc_tdy_frm_dwk(int('20' + tms_dwk[:2]), int(tms_dwk[3:]), 1),
               fnc_tdy_frm_dwk(int('20' + tms_dwk[:2]), int(tms_dwk[3:]), 7),]
    return [fnc_dtt_frm_tdy(0,str_typ,str_format,lst_edg[0]),
            fnc_dtt_frm_tdy(0,str_typ,str_format,lst_edg[1])]

def fnc_lst_edg_frm_dmh(str_dmh=None,str_typ='str',str_format='%Y-%m-%d'):
    """
    返回指定月的月一日和月末日期的列表，月输入格式为'%ym%M'
    :param str_dmh:
    :param str_typ:
    :param str_format:
    :return: [dtt_bgn,dtt_end]
    """
    str_dmh = fnc_dmh_frm_tdy() if str_dmh is None else str_dmh
    tms_bgn = dt_datetime(int('20' + str_dmh[:2]), int(str_dmh[3:]), 1)
    tms_tmp = tms_bgn + typ_dt_timedelta(days=31)
    tms_end = dt_datetime(tms_tmp.year, tms_tmp.month, 1) - typ_dt_timedelta(days=1)
    return [fnc_dtt_frm_tdy(0, str_typ, str_format, tms_bgn),
            fnc_dtt_frm_tdy(0, str_typ, str_format, tms_end)]

def fnc_lst_edg_frm_prd(dwkOrDmh,str_typ='str',str_format='%Y-%m-%d'):
    """
    根据输入的dwkOrDmh自动判断计算周或月的边界，并附加[2]为周或月的类型in ['x_week','x_mnth']
    :param dwkOrDmh: etc. '19w21','19m02'
    :param str_typ:
    :param str_format:
    :return: [bgn,end,typ]
    """
    if search('[0-9]{2}[m][0-9]{2}',dwkOrDmh):
        lst_prd = fnc_lst_edg_frm_dmh(dwkOrDmh, str_typ, str_format)
        x_prd = 'x_mnth'
    elif search('[0-9]{2}[w][0-9]{2}',dwkOrDmh):
        lst_prd = fnc_lst_edg_frm_dwk(dwkOrDmh, str_typ, str_format)
        x_prd = 'x_week'
    else:
        raise KeyError('stop: needs format ["%ym%M","%yw%W"]')
    return lst_prd + [x_prd]


# # database import and export preprocessing
def fnc_slt_dtf_frm_sql(str_dtb, str_sql=None, str_typ=None, str_mdl=None):
    """
    export datasets from tencent cloud mysql im type DataFrame
    :param str_sql: sql sentences
    :param str_dtb: name of the target database, now in ["db_spd_dly","db_hhs_dly"]
    :param str_typ: connect tencent cloud mysql by local or web, in ['local','web']
    :param str_mdl: 预设的可通用的sql语句样式, 默认None则str_sql生效, in ['lst_tb',]
    :return: the target dataset in type DataFrame
    """
    warn('deprecated: use fnc_sql_bsc instead.')
    # 预定义腾讯云内网、外网调用地址及端口
    dct_mysql_hst = {'local': ["172.16.0.13", 3306], 'web': ["cdb-cwlc1vtt.gz.tencentcdb.com", 10109]}
    str_typ = 'local' if str_typ is None else str_typ
    str_hst = dct_mysql_hst['local'][0] if str_typ == 'local' else dct_mysql_hst['web'][0]
    str_prt = dct_mysql_hst['local'][1] if str_typ == 'local' else dct_mysql_hst['web'][1]
    # 预定义通用的mysql功能语句样式，需要str_mdl不为None时将取代str_sql
    dct_mysql_mdl = {'lst_tb':"SELECT table_name FROM information_schema.tables WHERE table_schema='%s'" % str_dtb,}
    str_sql = dct_mysql_mdl[str_mdl] if str_mdl is not None else str_sql
    # 数据库连接
    cnn = connect(host=str_hst, port=str_prt, user="sl", password="sl19890421",
                  database=str_dtb, charset="utf8")
    crs = cnn.cursor()
    crs.execute(str_sql)
    if str_sql[:6].lower() == "select":
        dts = crs.fetchall()    # values in type tuple
        clm = crs.description   # columns information
        dtf = DataFrame(list(dts), columns=[i_clm[0] for i_clm in list(clm)])
    elif str_sql[:6].lower() in ["insert","update"]:
        cnn.commit()
        dtf = None
    else:
        dtf = None
    crs.close()
    cnn.close()
    return dtf

def fnc_sql_bsc(str_dtb, str_typ='local', str_sql=None, lst_dts=None, str_mdl=None):
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
    :param str_dtb: name of the target database, now in ["db_spd_dly","db_hhs_dly"]
    :param str_typ: connect tencent cloud mysql by local or web, in ['local','web']
    :param str_sql: sql sentences
    :param lst_dts: the lst_dts in crs.execute(str_sql, lst_dts) for %s without '"' in str_sql
    :param str_mdl: 预设的可通用的sql语句样式, 默认None则str_sql生效, in ['lst_tb',]
    :return: the target dataset in type DataFrame or None
    """
    # 预定义腾讯云内网、外网调用地址及端口
    dtf = None
    dct_mysql_hst = {'local': ["172.16.0.13", 3306], 'web': ["cdb-cwlc1vtt.gz.tencentcdb.com", 10109]}
    str_hst = dct_mysql_hst['local'][0] if str_typ == 'local' else dct_mysql_hst['web'][0]
    str_prt = dct_mysql_hst['local'][1] if str_typ == 'local' else dct_mysql_hst['web'][1]
    # 预定义通用的mysql功能语句样式，需要str_mdl不为None时将取代str_sql
    dct_mysql_mdl = {'lst_tb':"SELECT table_name FROM information_schema.tables WHERE table_schema='%s'" % str_dtb,}
    str_sql = dct_mysql_mdl[str_mdl] if str_mdl is not None and str_sql is None else str_sql
    # 数据库连接
    cnn = connect(host=str_hst,port=str_prt,user="sl",password="sl19890421",database=str_dtb,charset="utf8")
    crs = cnn.cursor()
    try:
        crs.execute(str_sql, lst_dts)
        if str_sql[:6].lower() in ['select']:
            dts = crs.fetchall()    # values in type tuple
            clm = crs.description   # columns information
            dtf = DataFrame(list(dts), columns=[i_clm[0] for i_clm in list(clm)]) if dts else dtf
        elif str_sql[:6].lower() in ['insert','update','delete']:
            cnn.commit()
        return dtf
    except IntegrityError:
        return 'pymysql.IntegrityError'
    except:
        raise KeyError('stop: unknown error.')
    finally:
        crs.close()
        cnn.close()

def fnc_slt_prd_bgn(prcmng_mpt, str_clm, str_prd_bgn):
    """beginning of time periods
    根据现有的库中最晚的时点, 定义当前阶段的窗口起点
    :param prcmng_mpt: 目标库
    :param str_clm: 目标库中时间列名
    :param str_prd_bgn: 若目标库不存在,则默认的初始化窗口起点
    :return: 窗口起点, 类型为字符串
    """
    try:
        xpt_prd = prcmng_mpt.stt_clm(str_clm, False)
    except IndexError:
        xpt_prd = str_prd_bgn
    return xpt_prd

def fnc_pdt_mng(prcmng_mpt, dtf_mpt, lst_clm_ndx, str_prd_mpt=None, str_prd_bgn=None):
    """import from dataframe to collections
    根据库是否存在, 进行有选择性的导入策略,全部导入或更新导入
    :param prcmng_mpt: 目标库 - 集合
    :param dtf_mpt: 待导入的数据集
    :param lst_clm_ndx: 为新建的集合创建唯一索引组
    :param str_prd_mpt: 本次操作所选择的时间窗起点，当此起点与初始化窗口起点相同时，采取创建策略
    :param str_prd_bgn: 初始化的窗口起点
    :return: None
    """
    warn("deprecated: use PrcMng.crt_and_dpt instead.")
    print('*****',prcmng_mpt.myDbs.name,'.',prcmng_mpt.myCln.name,'*****')
    if str_prd_mpt == str_prd_bgn and prcmng_mpt.myCln.count()==0:
        prcmng_mpt.mpt_dtf_to_cln(dtf_mpt)
        prcmng_mpt.crt_ndx(lst_clm_ndx, True)
    else:
        prcmng_mpt.pdt_dtf_to_cln(dtf_mpt, lst_clm_ndx)

# # type list alteration and index
def fnc_lst_frm_all(lst_mpt, bln_cll_lvl=True, str_orient='record'):
    """
    turn other types into list
    :param lst_mpt: a data set in type [list, tuple, dict, np.ndarray, pd.Series, pd.DataFrame]
    :param bln_cll_lvl: if true: {1,2} -> [1,2,2,..]; else: {1,2} -> [{1,2},{1,2},..]; but both [1,2] -> [1,2,2,..]
    :param str_orient: when type is pd.DataFrame, turn it into [{'A':1,'B':2,...},{},...]
    :return: the result in type list
    """
    if bln_cll_lvl is False:
        return [lst_mpt] if type(lst_mpt) is not list else lst_mpt
    elif type(lst_mpt) is list:
        return lst_mpt
    elif type(lst_mpt) in [typ_np_ndarray]:
        return lst_mpt.tolist()
    elif type(lst_mpt) in [tuple, typ_np_ndarray, typ_pd_Series, typ_pd_Index, typ_pd_RangeIndex]:
        return list(lst_mpt)
    elif type(lst_mpt) in [dict]:
        return list(lst_mpt.items())
    elif type(lst_mpt) in [typ_pd_DataFrame]:
        return lst_mpt.to_dict(orient=str_orient)
    else:
        return [lst_mpt]

def fnc_lst_to_thr(lst_mpt, str_typ='list', lst_dct_nms=None):
    """
    turn a list to other types
    :param lst_mpt:
    :param str_typ: turn to what, in ['list','listT','[dict]','[tuple]']
    :param lst_dct_nms: the keys if a list is going to become a [dict]
    :return: the original list in another type
    """
    if str_typ.lower() in ['list', 'lst']:
        return lst_mpt
    elif str_typ.lower() in ['listt', 'lstt', 't']:
        return np_array(lst_mpt).T.tolist()
    elif str_typ.lower() in ['dict', 'dct', '[dict]', '[dct]', 'listdict', 'lstdct']:  # [a,b]+[1,1]->[{a:1},{b:1}]
        lst_dct_nms = fnc_lst_frm_all(lst_dct_nms)
        return DataFrame(lst_mpt, columns=lst_dct_nms).to_dict(orient='record')
    elif str_typ.lower() in ['listtuple', 'lsttpl', 'list_tuple', 'lst_tpl']:  # [a,b,..]+[1,1,..] ->[(a,1),(b,1),..]
        lst_dct_nms = fnc_lst_frm_all(lst_dct_nms)
        lst_dct_nms = fnc_lst_cpy_cll(lst_dct_nms, len(lst_mpt))
        return [(i, lst_dct_nms[lst_mpt.index(i)]) for i in lst_mpt]
    elif str_typ.lower() in ['str']:  # 用于将list转化为不带引号的字符串:[1,'a',...] -> "1,a,..."
        return str(lst_mpt).replace("'", '')[1:-1]

def fnc_flt_for_lst_len(lst_mpt, bln_cll_lvl=True):
    """
    从一组嵌套列表中返回最长的子列表的长度
    :param lst_mpt: list in type [[],[],..], [{},{},..],..
    :param bln_cll_lvl: in fnc_lst_frm_all, if true: {1,2} -> [1,2,2,..]; if false: {1,2} -> [{1,2},{1,2},..]
    :return: a float of the biggest length in list
    """
    flt_len = 0
    for i_prt in lst_mpt:
        i_prt = fnc_lst_frm_all(i_prt, bln_cll_lvl)
        flt_len = len(i_prt) if len(i_prt) > flt_len else flt_len
    return flt_len

def fnc_lst_cpy_cll(lst_mpt, flt_len, bln_cll_lvl=True):
    """
    structuring: [1] -> [1,1,...], [1,2] -> [1,2,2,...]
    :param lst_mpt: a list cells to be ready for coping the last cell, a single cell is approval
    :param flt_len: how long the final list will be
    :param bln_cll_lvl: if true: {1,2} -> [1,2,2,...]; if false: {1,2} -> [{1,2},{1,2},...]
    :return: a new list tailed lst_mpt[-1] and length is flt_len
    """
    lst_mpt = fnc_lst_frm_all(lst_mpt, bln_cll_lvl)
    lst_xpt = lst_mpt.copy()
    while len(lst_xpt) < flt_len:
        lst_xpt.append(lst_mpt[-1])
    return lst_xpt

def fnc_lst_cll_mrg(lst_mpt, lst_add, str_typ='tail'):
    """structuring: ['a','b',...] -> ['ax','bx',...] | ['xa','xb',...]
    :param lst_mpt: original list, a single cell is approval
    :param lst_add: a list wanted to be merged to each cell in the list above, a single cell is approval
    :param str_typ: merge added str from ['head', 'tail'], default 'tail'
    :return: a new list with str_mpt tailed
    """
    lst_mpt = fnc_lst_frm_all(lst_mpt)
    lst_add = fnc_lst_cpy_cll(lst_add, len(lst_mpt))
    if str_typ.lower() in ['tail', 'tails']:
        return [str(lst_mpt[i]) + str(lst_add[i]) for i in range(len(lst_mpt))]
    elif str_typ.lower() in ['head', 'header', 'heads', 'headers']:
        return [str(lst_add[i]) + str(lst_mpt[i]) for i in range(len(lst_mpt))]

def fnc_lst_flatten(lst_mpt):
    """structuring: [a,[b,c],d] -> [a, b, c, d]
    (x,2)的嵌套列表、元组展平
    :param lst_mpt: original list
    :return: list in shape(x,1)
    """
    lst_ppc = []
    for typ in lst_mpt:
        if type(typ) is list:
            for i in typ: lst_ppc.append(i)
        else: lst_ppc.append(typ)
    return lst_ppc

def fnc_lst_frm_dtf_dtp(dtf_mpt, lst_dtp, bln_print=False):
    """list out columns' names in certain data types from a dataframe
    陈列出某个数据框中数据类型dtypes等于lst_dtp中所列dtype的列名
    :param dtf_mpt: the target dataframe to be listing
    :param lst_dtp: a list of columns' dtype, such as ['O','int64','float64',...], can be shown by dataframe.dtypes
    :param bln_print: whether if print all the dtypes of dtf_mpt or not
    :return: a list of columns' name in a certain dtype
    """
    if bln_print is True:
        print(dtf_mpt.dtypes)
    lst_clm_dtp = []
    lst_dtp = fnc_lst_frm_all(lst_dtp)
    for i_dtp in lst_dtp:
        lst_clm_dtp.extend(dtf_mpt.dtypes[dtf_mpt.dtypes == i_dtp].index.values)
    return lst_clm_dtp

def fnc_lst_frm_cll_mrg(str_mtd='intersection', *tpl_lst):
    """result a list of intersection/difference/union from many lists
    指定交并方式后，求得其后所列所有list的交、并、差集合
    :param str_mtd: 选择得到不同列表之间的交、并、差集，default 'intersection'
    :param tpl_lst: lists in auto tuple
    :return: a list of intersection
    """
    lst_intersection = tpl_lst[0]
    for i in range(1, len(tpl_lst)):
        if str_mtd.lower() in ['intersection', 'inter']:    # 交
            lst_intersection = list(set(lst_intersection) & set(tpl_lst[i]))
        elif str_mtd.lower() in ['difference', 'differ']:   # 差
            lst_intersection = list(set(lst_intersection) ^ set(tpl_lst[i]))
        elif str_mtd.lower() in ['union']:                  # 并
            lst_intersection = list(set(lst_intersection) | set(tpl_lst[i]))
    return lst_intersection

def fnc_lst_dtt_bth(lst_mpt, flt_len, lst_typ=None, lst_fmt=None, bln_day=True, str_typ='list', lst_dct=None):
    """
    function separating a list of datetime in batches
    eg.param: (['2016-01-01','2018-01-08'], 500, 'str','%Y-%m-%d',True,'[dict]',['RegDate_from','RegDate_to'])
    :param lst_mpt: a list of datetime in format [datetime_beginning, datetime_ending]
    :param flt_len: the width of a batch
    :param lst_typ: the type of list input and output, in format ['str','str']
    :param lst_fmt: the format of list input and output if type is 'str', informat ['%Y-%m-%d','%Y-%m-%d']
    :param bln_day: the unit of batches is day or short than a day, default True
    :param str_typ: 'list' -> [[A,B],[C,D],..]; 'listT' -> [[A,C,...],[B,D,...]]; '[dict]' -> [{A,B},{C,D},...]
    :param lst_dct: define columns' names if str_typ is '[dict]', default [{'beginning':A,'end':B},...]
    :return: a list of datetime batches, default in format [[tms_011,tms_012],[tms_021,tms_022],...]
    """
    lst_typ = [None, 'str'] if lst_typ is None else fnc_lst_cpy_cll(lst_typ, 2)         # 未指定时默认输出str
    if lst_fmt is None:                                                                 # 未指定时默认输出标准格式
        lst_fmt = [None, '%Y-%m-%d'] if bln_day else [None, '%Y-%m-%d %H:%M:%S']
    else:
        lst_fmt = fnc_lst_cpy_cll(lst_fmt, 2)
    lst_mpt[0], lst_mpt[1] = fnc_tms_frm_all(lst_mpt[0]), fnc_tms_frm_all(lst_mpt[1])   # 进入长度2的list，转化为tms
    lst_xpt = []
    dtm_bdt = lst_mpt[0]                                                                # 时间段初次赋值
    dtm_edt = lst_mpt[0] + typ_dt_timedelta(days=(flt_len-(1 if bln_day is True else 0)))
    while (lst_mpt[1] - dtm_bdt).days >= 0:
        stm_bdt = fnc_dtt_frm_tdy(0, lst_typ[1], lst_fmt[1], dtm_bdt)                   # 时间段输出格式转换
        stm_edt = fnc_dtt_frm_tdy(0, lst_typ[1], lst_fmt[1], dtm_edt if dtm_edt < lst_mpt[1] else lst_mpt[1])
        lst_xpt.append([stm_bdt, stm_edt])                                              # 格式转换后时间段追加入输出列表
        dtm_bdt = dtm_bdt + typ_dt_timedelta(days=flt_len) if bln_day else dtm_edt + typ_dt_timedelta(seconds=1)    # 时间段递增
        dtm_edt += typ_dt_timedelta(days=flt_len)                                                               # 时间段递增
    return fnc_lst_to_thr(lst_xpt, str_typ, lst_dct)

# # type dict alteration and index
def fnc_dct_frm_lst(lst_kys, lst_vls):
    """structuring: [a,b,c] + [x, y, z] -> {a:x,b:y,c:z}
    :param lst_kys: keys in a list, a single cell is approval for a one length dict
    :param lst_vls: values in a list, a single cell is approval
    :return: a new dict combined by lst_mpt and lst_xpt
    """
    lst_kys = fnc_lst_frm_all(lst_kys)
    lst_vls = fnc_lst_cpy_cll(lst_vls, len(lst_kys))
    dct_xpt = {}
    for i in range(len(lst_kys)):
        dct_xpt[lst_kys[i]] = lst_vls[i]
    return dct_xpt

def fnc_dct_mrg_hrl(lst_mpt):
    """
    merge a list of dicts into one dict, [{A},{B},..] -> {A,B,..}
    :param lst_mpt: a list of dicts like [{},{},..]
    :return: a dict {A,B,..}
    """
    dct_tmp = {}
    for i_dct in lst_mpt:
        if type(i_dct) is dict:
            dct_tmp.update(i_dct)
    return dct_tmp

def fnc_dct_for_mng_xpt(lst_clm=None, bln_id=False):
    """structuring: [a,b,..] + False/True -> {'_id':0/1,a:1,b:1,...}
    :param lst_clm: the columns' name for exporting
    :param bln_id: export _id or not, default False
    :return: a dict in type {'_id': 0,'columnName': 1,...}
    """
    lst_clm_bln = [1] if bln_id else [0]
    lst_clm_xpt = ['_id']
    if lst_clm is not None:
        lst_clm = fnc_lst_frm_all(lst_clm)
        lst_clm_xpt.extend(lst_clm)
        lst_clm_bln.append(1)
    return fnc_dct_frm_lst(lst_clm_xpt, lst_clm_bln)

def fnc_dtf_frm_csv_mpt(pth_fold, pth_file, str_sep=None):
    """
    import dataframe from csv
    :param pth_fold:
    :param pth_file:
    :param str_sep: which str is the separation between columns, default None
    :return: a dataset in type dataframe
    """
    if str_sep is None:
        dtf_xpt = read_csv(path.join(pth_fold, pth_file))
    else:
        dtf_xpt = read_csv(path.join(pth_fold, pth_file), sep=str_sep)
    return dtf_xpt

def fnc_dtf_frm_xcl_mpt(pth_fold, pth_file, str_hdr=None,str_sht=None):
    """
    import dataframe from excel
    :param pth_fold:
    :param pth_file:
    :param str_hdr: which row is the header for columns' name, default None for 0
    :param str_sht: sheet name
    :return: a dataset in type dataframe
    """
    if str_hdr is None and str_sht is None:
        dtf_xpt = read_excel(path.join(pth_fold, pth_file))
    elif str_hdr is None:
        dtf_xpt = read_excel(path.join(pth_fold, pth_file), sheet_name=str_sht)
    elif str_sht is None:
        dtf_xpt = read_excel(path.join(pth_fold, pth_file), header=str_hdr)
    else:
        dtf_xpt = read_excel(path.join(pth_fold, pth_file), header=str_hdr,sheet_name=str_sht)
    return dtf_xpt
